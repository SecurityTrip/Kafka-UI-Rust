use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
pub struct Topic {
    pub name: String,
    pub partitions: u32,
    pub out_of_sync_replicas: u32,
    pub replication_factor: u16,
    pub message_count: u64,
    pub size_bytes: Option<u64>,
    pub is_internal: bool,
    pub partition_details: Vec<TopicPartition>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicPartition {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMessageHeader {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProduceMessageRequest {
    pub key: Option<String>,
    pub value: String,
    #[serde(default)]
    pub headers: Vec<TopicMessageHeader>,
}

#[derive(Debug, Serialize)]
pub struct ProduceMessageResponse {
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicMessage {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<String>,
    pub value: Option<String>,
    pub headers: Vec<TopicMessageHeader>,
    pub key_size: usize,
    pub value_size: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicConsumer {
    pub group_id: String,
    pub state: String,
    pub active_consumers: u32,
    pub consumer_lag: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicOverviewResponse {
    pub topic_name: String,
    pub partitions: u32,
    pub replication_factor: u16,
    pub urp: u32,
    pub in_sync_replicas: u32,
    pub total_replicas: u32,
    pub topic_type: String,
    pub segment_size_bytes: Option<u64>,
    pub segment_count: Option<u64>,
    pub cleanup_policy: String,
    pub message_count: u64,
    pub consumers: Vec<TopicConsumer>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Broker {
    pub id: i32,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsumerGroup {
    pub name: String,
    pub state: String,
    pub members: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterOverview {
    pub cluster_id: String,
    pub controller_id: i32,
    pub kafka_version: String,
    pub broker_count: usize,
    pub topic_count: usize,
    pub consumer_group_count: usize,
    pub bootstrap_servers: String,
}

#[derive(Debug, Serialize)]
pub struct SnapshotResponse {
    pub cluster: ClusterOverview,
    pub brokers: Vec<Broker>,
    pub topics: Vec<Topic>,
    pub groups: Vec<ConsumerGroup>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
}

#[derive(Debug, Serialize)]
pub struct ListResponse<T> {
    pub total: usize,
    pub items: Vec<T>,
}
