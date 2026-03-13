use crate::kafka::KafkaClient;

#[derive(Debug)]
pub struct AppState {
    kafka_client: KafkaClient,
}

impl AppState {
    pub fn new(kafka_client: KafkaClient) -> Self {
        Self { kafka_client }
    }

    pub fn kafka_client(&self) -> &KafkaClient {
        &self.kafka_client
    }
}
