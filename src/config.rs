use std::{env, net::IpAddr};

use thiserror::Error;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub host: IpAddr,
    pub port: u16,
    pub kafka_bootstrap_servers: String,
    pub kafka_timeout_ms: u64,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let host = match env::var("APP_HOST") {
            Ok(value) => value
                .parse::<IpAddr>()
                .map_err(|_| ConfigError::InvalidHost(value))?,
            Err(_) => IpAddr::from([127, 0, 0, 1]),
        };

        let port = match env::var("APP_PORT") {
            Ok(value) => value
                .parse::<u16>()
                .map_err(|_| ConfigError::InvalidPort(value))?,
            Err(_) => 3000,
        };

        let kafka_bootstrap_servers =
            env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string());

        let kafka_timeout_ms = match env::var("KAFKA_TIMEOUT_MS") {
            Ok(value) => value
                .parse::<u64>()
                .map_err(|_| ConfigError::InvalidKafkaTimeout(value))?,
            Err(_) => 5_000,
        };

        Ok(Self {
            host,
            port,
            kafka_bootstrap_servers,
            kafka_timeout_ms,
        })
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid APP_HOST value: {0}")]
    InvalidHost(String),
    #[error("invalid APP_PORT value: {0}")]
    InvalidPort(String),
    #[error("invalid KAFKA_TIMEOUT_MS value: {0}")]
    InvalidKafkaTimeout(String),
}
