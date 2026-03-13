use std::{
    collections::HashMap,
    process::Command,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use rdkafka::{
    admin::{AdminClient, AdminOptions, ResourceSpecifier},
    bindings,
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
    client::DefaultClientContext,
    error::KafkaError as RdkafkaError,
    message::{Headers, Message},
    topic_partition_list::{Offset, TopicPartitionList},
};
use thiserror::Error;

use crate::models::{
    Broker, ClusterOverview, ConsumerGroup, Topic, TopicConsumer, TopicMessage,
    TopicMessageHeader,
    TopicOverviewResponse, TopicPartition,
};

#[derive(Debug, Clone)]
pub struct KafkaClient {
    bootstrap_servers: String,
    timeout: Duration,
}

const TOPIC_OVERVIEW_CACHE_TTL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
struct CachedOverview {
    updated_at: Instant,
    overview: TopicOverviewResponse,
}

fn topic_overview_cache() -> &'static Mutex<HashMap<String, CachedOverview>> {
    static CACHE: OnceLock<Mutex<HashMap<String, CachedOverview>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
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

    pub async fn fetch_topic_messages(
        &self,
        topic: String,
        limit: usize,
    ) -> Result<Vec<TopicMessage>, KafkaClientError> {
        let bootstrap_servers = self.bootstrap_servers.clone();
        let timeout = self.timeout;
        let capped_limit = limit.clamp(1, 200);

        tokio::task::spawn_blocking(move || {
            fetch_topic_messages_blocking(&bootstrap_servers, timeout, &topic, capped_limit)
        })
        .await
        .map_err(KafkaClientError::Join)?
    }

    pub async fn fetch_topic_overview(
        &self,
        topic: String,
    ) -> Result<TopicOverviewResponse, KafkaClientError> {
        let bootstrap_servers = self.bootstrap_servers.clone();
        let timeout = self.timeout;
        let cache_key = format!("{}::{}", bootstrap_servers, topic);

        if let Ok(cache) = topic_overview_cache().lock() {
            if let Some(entry) = cache.get(&cache_key) {
                if entry.updated_at.elapsed() < TOPIC_OVERVIEW_CACHE_TTL {
                    return Ok(entry.overview.clone());
                }
            }
        }

        let overview = tokio::task::spawn_blocking(move || {
            fetch_topic_overview_blocking(&bootstrap_servers, timeout, &topic)
        })
        .await
        .map_err(KafkaClientError::Join)??;

        if let Ok(mut cache) = topic_overview_cache().lock() {
            cache.insert(
                cache_key,
                CachedOverview {
                    updated_at: Instant::now(),
                    overview: overview.clone(),
                },
            );
        }

        Ok(overview)
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

fn fetch_topic_messages_blocking(
    bootstrap_servers: &str,
    timeout: Duration,
    topic: &str,
    limit: usize,
) -> Result<Vec<TopicMessage>, KafkaClientError> {
    let consumer = build_consumer(bootstrap_servers)?;
    let metadata = consumer.fetch_metadata(Some(topic), timeout)?;
    let meta_topic = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic);

    let Some(meta_topic) = meta_topic else {
        return Ok(Vec::new());
    };

    if meta_topic.partitions().is_empty() {
        return Ok(Vec::new());
    }

    let mut assignment = TopicPartitionList::new();
    for partition in meta_topic.partitions() {
        let (low, high) = consumer
            .fetch_watermarks(topic, partition.id(), timeout)
            .unwrap_or((0, 0));
        let start = if high <= low {
            low
        } else {
            (high - limit as i64).max(low)
        };
        assignment.add_partition_offset(topic, partition.id(), Offset::Offset(start))?;
    }
    consumer.assign(&assignment)?;

    let mut messages: Vec<TopicMessage> = Vec::new();
    let mut idle_polls = 0usize;
    while messages.len() < limit && idle_polls < 10 {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                idle_polls = 0;
                let key = message.key().map(|v| String::from_utf8_lossy(v).to_string());
                let value = message
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                let headers = message
                    .headers()
                    .map(|items| {
                        items
                            .iter()
                            .map(|header| TopicMessageHeader {
                                key: header.key.to_string(),
                                value: header
                                    .value
                                    .map(|raw| String::from_utf8_lossy(raw).to_string()),
                            })
                            .collect::<Vec<TopicMessageHeader>>()
                    })
                    .unwrap_or_default();
                let topic_message = TopicMessage {
                    partition: message.partition(),
                    offset: message.offset(),
                    timestamp_ms: message.timestamp().to_millis(),
                    headers,
                    key_size: message.key().map(|v| v.len()).unwrap_or(0),
                    value_size: message.payload().map(|v| v.len()).unwrap_or(0),
                    key,
                    value,
                };
                messages.push(topic_message);
            }
            Some(Err(_)) => {
                idle_polls += 1;
            }
            None => {
                idle_polls += 1;
            }
        }
    }

    messages.sort_by(|a, b| {
        b.timestamp_ms
            .unwrap_or(i64::MIN)
            .cmp(&a.timestamp_ms.unwrap_or(i64::MIN))
            .then(b.offset.cmp(&a.offset))
    });
    if messages.len() > limit {
        messages.truncate(limit);
    }
    Ok(messages)
}

fn fetch_topic_overview_blocking(
    bootstrap_servers: &str,
    timeout: Duration,
    topic: &str,
) -> Result<TopicOverviewResponse, KafkaClientError> {
    let consumer = build_consumer(bootstrap_servers)?;
    let metadata = consumer.fetch_metadata(Some(topic), timeout)?;
    let group_list = consumer.fetch_group_list(None, timeout)?;

    let brokers: Vec<i32> = metadata.brokers().iter().map(|b| b.id()).collect();
    let topic_sizes = fetch_topic_sizes_via_log_dirs(
        bootstrap_servers,
        &brokers,
        &[topic.to_string()],
    )
    .unwrap_or_default();

    let Some(meta_topic) = metadata.topics().iter().find(|t| t.name() == topic) else {
        return Ok(TopicOverviewResponse {
            topic_name: topic.to_string(),
            partitions: 0,
            replication_factor: 0,
            urp: 0,
            in_sync_replicas: 0,
            total_replicas: 0,
            topic_type: "External".to_string(),
            segment_size_bytes: None,
            segment_count: None,
            cleanup_policy: "-".to_string(),
            message_count: 0,
            consumers: Vec::new(),
        });
    };

    let partition_details: Vec<TopicPartition> = meta_topic
        .partitions()
        .iter()
        .map(|partition| TopicPartition {
            id: partition.id(),
            leader: partition.leader(),
            replicas: partition.replicas().to_vec(),
            isr: partition.isr().to_vec(),
        })
        .collect();

    let replication_factor = partition_details
        .iter()
        .map(|partition| partition.replicas.len())
        .max()
        .unwrap_or(0) as u16;

    let urp = partition_details
        .iter()
        .map(|partition| {
            let replicas = partition.replicas.len() as i32;
            let isr = partition.isr.len() as i32;
            (replicas - isr).max(0) as u32
        })
        .sum::<u32>();

    let in_sync_replicas = partition_details
        .iter()
        .map(|partition| partition.isr.len() as u32)
        .sum::<u32>();
    let total_replicas = partition_details
        .iter()
        .map(|partition| partition.replicas.len() as u32)
        .sum::<u32>();

    let message_count = partition_details
        .iter()
        .map(|partition| {
            consumer
                .fetch_watermarks(topic, partition.id, timeout)
                .map(|(low, high)| (high - low).max(0) as u64)
                .unwrap_or(0)
        })
        .sum::<u64>();

    let group_states: HashMap<String, String> = group_list
        .groups()
        .iter()
        .map(|g| (g.name().to_string(), g.state().to_string()))
        .collect();

    let consumers = fetch_topic_consumers_via_cli(bootstrap_servers, topic, &group_states);

    let (cleanup_policy, segment_size_bytes) =
        fetch_topic_configs(bootstrap_servers, topic).unwrap_or_else(|| ("delete".to_string(), None));

    let size_bytes = topic_sizes.get(topic).copied();
    let segment_count = match (size_bytes, segment_size_bytes) {
        (Some(size), Some(segment_size)) if segment_size > 0 => {
            Some(((size + segment_size - 1) / segment_size).max(1))
        }
        _ => None,
    };

    Ok(TopicOverviewResponse {
        topic_name: topic.to_string(),
        partitions: partition_details.len() as u32,
        replication_factor,
        urp,
        in_sync_replicas,
        total_replicas,
        topic_type: if topic.starts_with("__") {
            "Internal".to_string()
        } else {
            "External".to_string()
        },
        segment_size_bytes,
        segment_count,
        cleanup_policy,
        message_count,
        consumers,
    })
}

fn fetch_topic_configs(bootstrap_servers: &str, topic: &str) -> Option<(String, Option<u64>)> {
    let output = ["kafka-configs", "kafka-configs.sh"]
        .iter()
        .find_map(|bin| {
            Command::new(bin)
                .arg("--bootstrap-server")
                .arg(bootstrap_servers)
                .arg("--entity-type")
                .arg("topics")
                .arg("--entity-name")
                .arg(topic)
                .arg("--describe")
                .output()
                .ok()
        })?;

    if !output.status.success() {
        return None;
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let mut cleanup_policy = "delete".to_string();
    let mut segment_size_bytes = None;

    for line in text.lines() {
        if let Some(pos) = line.find("cleanup.policy=") {
            let tail = &line[(pos + "cleanup.policy=".len())..];
            cleanup_policy = tail
                .split_whitespace()
                .next()
                .unwrap_or("delete")
                .to_string();
        }
        if let Some(pos) = line.find("segment.bytes=") {
            let tail = &line[(pos + "segment.bytes=".len())..];
            segment_size_bytes = tail
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok());
        }
    }

    Some((cleanup_policy, segment_size_bytes))
}

fn fetch_topic_consumers_via_cli(
    bootstrap_servers: &str,
    topic: &str,
    group_states: &HashMap<String, String>,
) -> Vec<TopicConsumer> {
    let output = ["kafka-consumer-groups", "kafka-consumer-groups.sh"]
        .iter()
        .find_map(|bin| {
            Command::new(bin)
                .arg("--bootstrap-server")
                .arg(bootstrap_servers)
                .arg("--describe")
                .arg("--all-groups")
                .output()
                .ok()
        });

    let Some(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lag_by_group: HashMap<String, i64> = HashMap::new();
    let mut members_by_group: HashMap<String, HashMap<String, bool>> = HashMap::new();

    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("GROUP") {
            continue;
        }

        let cols: Vec<&str> = trimmed.split_whitespace().collect();
        if cols.len() < 6 {
            continue;
        }

        if cols[1] != topic {
            continue;
        }

        let group_id = cols[0].to_string();
        let lag = cols[5].parse::<i64>().unwrap_or(0);
        let consumer_id = if cols.len() > 6 { cols[6] } else { "-" };

        let current_lag = lag_by_group.entry(group_id.clone()).or_insert(0);
        *current_lag += lag.max(0);

        if consumer_id != "-" {
            members_by_group
                .entry(group_id)
                .or_default()
                .insert(consumer_id.to_string(), true);
        }
    }

    let mut consumers = lag_by_group
        .into_iter()
        .map(|(group_id, consumer_lag)| {
            let active_consumers = members_by_group
                .get(&group_id)
                .map(|m| m.len() as u32)
                .unwrap_or(0);
            TopicConsumer {
                group_id: group_id.clone(),
                state: group_states
                    .get(&group_id)
                    .cloned()
                    .unwrap_or_else(|| "Unknown".to_string()),
                active_consumers,
                consumer_lag,
            }
        })
        .collect::<Vec<TopicConsumer>>();

    consumers.sort_by(|a, b| a.group_id.cmp(&b.group_id));
    consumers
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
