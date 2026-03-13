use crate::models::{Broker, Topic};

#[derive(Debug)]
pub struct AppState {
    topics: Vec<Topic>,
    brokers: Vec<Broker>,
}

impl AppState {
    pub fn seeded() -> Self {
        Self {
            topics: vec![
                Topic {
                    name: "payments.events".to_string(),
                    partitions: 12,
                    replication_factor: 3,
                },
                Topic {
                    name: "orders.v1".to_string(),
                    partitions: 8,
                    replication_factor: 3,
                },
            ],
            brokers: vec![
                Broker {
                    id: 1,
                    host: "kafka-1.local".to_string(),
                    port: 9092,
                },
                Broker {
                    id: 2,
                    host: "kafka-2.local".to_string(),
                    port: 9092,
                },
            ],
        }
    }

    pub fn topics(&self) -> &[Topic] {
        &self.topics
    }

    pub fn brokers(&self) -> &[Broker] {
        &self.brokers
    }
}
