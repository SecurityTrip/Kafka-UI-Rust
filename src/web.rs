use std::sync::Arc;

use askama::Template;
use axum::{
    Json, Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
};

use crate::{
    error::AppError,
    models::{Broker, HealthResponse, ListResponse, Topic},
    state::AppState,
};

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate<'a> {
    topics: &'a [Topic],
    brokers: &'a [Broker],
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/api/topics", get(api_topics))
        .route("/api/brokers", get(api_brokers))
        .with_state(state)
}

async fn index(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, AppError> {
    let template = IndexTemplate {
        topics: state.topics(),
        brokers: state.brokers(),
    };

    Ok(Html(template.render()?))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "apache-kafka-ui",
    })
}

async fn api_topics(State(state): State<Arc<AppState>>) -> Json<ListResponse<Topic>> {
    let items = state.topics().to_vec();
    Json(ListResponse {
        total: items.len(),
        items,
    })
}

async fn api_brokers(State(state): State<Arc<AppState>>) -> Json<ListResponse<Broker>> {
    let items = state.brokers().to_vec();
    Json(ListResponse {
        total: items.len(),
        items,
    })
}
