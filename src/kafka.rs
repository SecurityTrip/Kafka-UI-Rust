use std::{
    collections::HashMap,
    process::Command,
    time::Duration,
};

use rdkafka::{
    admin::{AdminClient, AdminOptions, ResourceSpecifier},
    bindings,
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
    client::DefaultClientContext,
    error::KafkaError as RdkafkaError,
};
use thiserror::Error;

use crate::models::{Broker, ClusterOverview, ConsumerGroup, Topic, TopicPartition};

#[derive(Debug, Clone)]
pub struct KafkaClient {
    bootstrap_servers: String,
    timeout: Duration,
}

impl KafkaClient {
    pub fn new(bootstrap_servers: String, timeout_ms: u64) -> Self {
        Self {
            bootstrap_servers,
            timeout: Duration::from_millis(timeout_ms),
        }
    }

    pub async fn fetch_snapshot(&self) -> Result<KafkaSnapshot, KafkaClientError> {
        let bootstrap_servers = self.bootstrap_servers.clone();
        let timeout = self.timeout;

        let mut snapshot = tokio::task::spawn_blocking(move || fetch_snapshot_blocking(&bootstrap_servers, timeout))
            .await
            .map_err(KafkaClientError::Join)??;

        let mut broker_ids: Vec<i32> = snapshot.brokers.iter().map(|b| b.id).collect();
        if !broker_ids.iter().any(|id| *id == snapshot.cluster.controller_id) {
            broker_ids.insert(0, snapshot.cluster.controller_id);
        }

        let kafka_version = self
            .fetch_inter_broker_protocol_version(&broker_ids)
            .await
            .unwrap_or_else(|| "Unknown".to_string());

        snapshot.cluster.kafka_version = kafka_version;
        Ok(snapshot)
    }

    async fn fetch_inter_broker_protocol_version(&self, broker_ids: &[i32]) -> Option<String> {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()
            .ok()?;

        for broker_id in broker_ids {
            if *broker_id < 0 {
                continue;
            }

            let spec = ResourceSpecifier::Broker(*broker_id);
            let result = match admin.describe_configs([&spec], &AdminOptions::new()).await {
                Ok(v) => v,
                Err(_) => continue,
            };

            let config_resource = match result.into_iter().next() {
                Some(Ok(res)) => res,
                _ => continue,
            };

            let version = config_resource
                .get("metadata.version")
                .and_then(|entry| entry.value.clone())
                .or_else(|| {
                    config_resource
                        .get("inter.broker.protocol.version")
                        .and_then(|entry| entry.value.clone())
                })
                .or_else(|| {
                    config_resource
                        .get("log.message.format.version")
                        .and_then(|entry| entry.value.clone())
                });

            if version.is_some() {
                return version;
            }
        }

        None
    }
}

#[derive(Debug, Clone)]
pub struct KafkaSnapshot {
    pub cluster: ClusterOverview,
    pub brokers: Vec<Broker>,
    pub topics: Vec<Topic>,
    pub groups: Vec<ConsumerGroup>,
}

fn fetch_snapshot_blocking(
    bootstrap_servers: &str,
    timeout: Duration,
) -> Result<KafkaSnapshot, KafkaClientError> {
    let consumer = build_consumer(bootstrap_servers)?;
    let controller_id = fetch_controller_id(&consumer, timeout);

    let metadata = consumer.fetch_metadata(None, timeout)?;
    let group_list = consumer.fetch_group_list(None, timeout)?;

    let brokers: Vec<Broker> = metadata
        .brokers()
        .iter()
        .map(|broker| Broker {
            id: broker.id(),
            host: broker.host().to_string(),
            port: broker.port() as u16,
        })
        .collect();

    let topic_names: Vec<String> = metadata.topics().iter().map(|t| t.name().to_string()).collect();
    let broker_ids: Vec<i32> = brokers.iter().map(|b| b.id).collect();
    let topic_sizes = fetch_topic_sizes_via_log_dirs(bootstrap_servers, &broker_ids, &topic_names)
        .unwrap_or_default();

    let topics: Vec<Topic> = metadata
        .topics()
        .iter()
        .map(|topic| {
            let partition_details: Vec<TopicPartition> = topic
                .partitions()
                .iter()
                .map(|partition| TopicPartition {
                    id: partition.id(),
                    leader: partition.leader(),
                    replicas: partition.replicas().to_vec(),
                    isr: partition.isr().to_vec(),
                })
                .collect();

            let replication_factor = topic
                .partitions()
                .iter()
                .map(|partition| partition.replicas().len())
                .max()
                .unwrap_or(0) as u16;

            let out_of_sync_replicas = partition_details
                .iter()
                .map(|partition| {
                    let replicas = partition.replicas.len() as i32;
                    let isr = partition.isr.len() as i32;
                    (replicas - isr).max(0) as u32
                })
                .sum::<u32>();

            let message_count = partition_details
                .iter()
                .map(|partition| {
                    consumer
                        .fetch_watermarks(topic.name(), partition.id, timeout)
                        .map(|(low, high)| (high - low).max(0) as u64)
                        .unwrap_or(0)
                })
                .sum::<u64>();

            Topic {
                name: topic.name().to_string(),
                partitions: topic.partitions().len() as u32,
                out_of_sync_replicas,
                replication_factor,
                message_count,
                size_bytes: topic_sizes.get(topic.name()).copied(),
                is_internal: topic.name().starts_with("__"),
                partition_details,
            }
        })
        .collect();

    let groups: Vec<ConsumerGroup> = group_list
        .groups()
        .iter()
        .map(|group| ConsumerGroup {
            name: group.name().to_string(),
            state: group.state().to_string(),
            members: group.members().len() as u32,
        })
        .collect();

    let cluster = ClusterOverview {
        cluster_id: "unknown".to_string(),
        controller_id,
        kafka_version: "Unknown".to_string(),
        broker_count: brokers.len(),
        topic_count: topics.len(),
        consumer_group_count: groups.len(),
        bootstrap_servers: bootstrap_servers.to_string(),
    };

    Ok(KafkaSnapshot {
        cluster,
        brokers,
        topics,
        groups,
    })
}

fn build_consumer(bootstrap_servers: &str) -> Result<BaseConsumer, KafkaClientError> {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "apache-kafka-ui-observer")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create::<BaseConsumer>()?;

    Ok(consumer)
}

fn fetch_controller_id(consumer: &BaseConsumer, timeout: Duration) -> i32 {
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    unsafe { bindings::rd_kafka_controllerid(consumer.client().native_ptr(), timeout_ms) }
}

fn fetch_topic_sizes_via_log_dirs(
    bootstrap_servers: &str,
    broker_ids: &[i32],
    topic_names: &[String],
) -> Option<HashMap<String, u64>> {
    if broker_ids.is_empty() || topic_names.is_empty() {
        return Some(HashMap::new());
    }

    let broker_list = broker_ids
        .iter()
        .map(i32::to_string)
        .collect::<Vec<String>>()
        .join(",");
    let topic_list = topic_names.join(",");

    let output = ["kafka-log-dirs", "kafka-log-dirs.sh"]
        .iter()
        .find_map(|bin| {
            Command::new(bin)
                .arg("--bootstrap-server")
                .arg(bootstrap_servers)
                .arg("--describe")
                .arg("--broker-list")
                .arg(&broker_list)
                .arg("--topic-list")
                .arg(&topic_list)
                .output()
                .ok()
        })?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    let json_start = stdout.find('{')?;
    let json_text = &stdout[json_start..];
    let value: serde_json::Value = serde_json::from_str(json_text).ok()?;

    let mut sizes: HashMap<String, u64> = HashMap::new();
    let brokers = value.get("brokers")?.as_array()?;

    for broker in brokers {
        let log_dirs = broker
            .get("logDirs")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for log_dir in log_dirs {
            let partitions = log_dir
                .get("partitions")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            for partition in partitions {
                let partition_name = partition
                    .get("partition")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let Some((topic_name, _)) = partition_name.rsplit_once('-') else {
                    continue;
                };

                let size = partition
                    .get("size")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let current = sizes.entry(topic_name.to_string()).or_insert(0);
                *current = current.saturating_add(size);
            }
        }
    }

    Some(sizes)
}

#[derive(Debug, Error)]
pub enum KafkaClientError {
    #[error("kafka operation failed: {0}")]
    Kafka(#[from] RdkafkaError),
    #[error("internal task join failure: {0}")]
    Join(#[from] tokio::task::JoinError),
}
