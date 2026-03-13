use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Topic {
    pub name: String,
    pub partitions: u32,
    pub replication_factor: u16,
}

#[derive(Debug, Clone, Serialize)]
pub struct Broker {
    pub id: i32,
    pub host: String,
    pub port: u16,
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
