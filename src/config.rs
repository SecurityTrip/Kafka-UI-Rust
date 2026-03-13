use std::{env, net::IpAddr};

use thiserror::Error;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub host: IpAddr,
    pub port: u16,
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

        Ok(Self { host, port })
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid APP_HOST value: {0}")]
    InvalidHost(String),
    #[error("invalid APP_PORT value: {0}")]
    InvalidPort(String),
}
