use std::time::Duration;

use rdkafka::{
    admin::{AdminClient, AdminOptions, ResourceSpecifier},
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

            Topic {
                name: topic.name().to_string(),
                partitions: topic.partitions().len() as u32,
                replication_factor,
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
        controller_id: metadata.orig_broker_id(),
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

#[derive(Debug, Error)]
pub enum KafkaClientError {
    #[error("kafka operation failed: {0}")]
    Kafka(#[from] RdkafkaError),
    #[error("internal task join failure: {0}")]
    Join(#[from] tokio::task::JoinError),
}
