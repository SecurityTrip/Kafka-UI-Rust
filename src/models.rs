use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Topic {
    pub name: String,
    pub partitions: u32,
    pub replication_factor: u16,
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
