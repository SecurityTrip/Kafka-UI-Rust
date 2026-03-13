mod config;
mod error;
mod kafka;
mod models;
mod state;
mod web;

use std::{error::Error, net::SocketAddr, sync::Arc};

use config::AppConfig;
use kafka::KafkaClient;
use state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_tracing();

    let config = AppConfig::from_env()?;
    let kafka_client = KafkaClient::new(
        config.kafka_bootstrap_servers.clone(),
        config.kafka_timeout_ms,
    );
    let state = Arc::new(AppState::new(kafka_client));
    let app = web::router(state);

    let addr = SocketAddr::new(config.host, config.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!(%addr, kafka = %config.kafka_bootstrap_servers, "server started");
    axum::serve(listener, app).await?;
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "apache_kafka_ui=info,tower_http=info".into()),
        )
        .with_target(false)
        .compact()
        .init();
}
