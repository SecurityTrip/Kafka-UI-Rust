use std::sync::Mutex;

use crate::{
    kafka::KafkaClient,
    models::Broker,
};

#[derive(Debug)]
pub struct AppState {
    kafka_client: KafkaClient,
    last_controller_id: Mutex<Option<i32>>,
}

impl AppState {
    pub fn new(kafka_client: KafkaClient) -> Self {
        Self {
            kafka_client,
            last_controller_id: Mutex::new(None),
        }
    }

    pub fn kafka_client(&self) -> &KafkaClient {
        &self.kafka_client
    }

    pub fn stabilize_controller_id(&self, current_id: i32, brokers: &[Broker]) -> i32 {
        if current_id >= 0 {
            if let Ok(mut guard) = self.last_controller_id.lock() {
                *guard = Some(current_id);
            }
            return current_id;
        }

        if let Ok(guard) = self.last_controller_id.lock() {
            if let Some(last_known) = *guard {
                return last_known;
            }
        }

        if let Some(first_broker) = brokers.first() {
            if let Ok(mut guard) = self.last_controller_id.lock() {
                *guard = Some(first_broker.id);
            }
            return first_broker.id;
        }

        -1
    }
}
