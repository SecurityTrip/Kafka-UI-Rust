use std::time::Duration;

use rdkafka::{
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError as RdkafkaError,
};
use thiserror::Error;

use crate::models::{Broker, ClusterOverview, ConsumerGroup, Topic};

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

        tokio::task::spawn_blocking(move || fetch_snapshot_blocking(&bootstrap_servers, timeout))
            .await
            .map_err(KafkaClientError::Join)?
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
