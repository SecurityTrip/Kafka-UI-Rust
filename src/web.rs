use std::{convert::Infallible, sync::Arc, time::Duration};

use async_stream::stream;
use askama::Template;
use axum::{
    Json, Router,
    extract::State,
    response::{
        Html, IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use tower_http::services::ServeDir;

use crate::{
    error::AppError,
    models::{
        Broker, ClusterOverview, ConsumerGroup, HealthResponse, ListResponse, SnapshotResponse,
        Topic,
    },
    state::AppState,
};

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate<'a> {
    cluster: &'a ClusterOverview,
    topics: &'a [Topic],
    brokers: &'a [Broker],
    groups: &'a [ConsumerGroup],
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/api/stream", get(api_stream))
        .route("/api/snapshot", get(api_snapshot))
        .route("/api/cluster", get(api_cluster))
        .route("/api/topics", get(api_topics))
        .route("/api/brokers", get(api_brokers))
        .route("/api/groups", get(api_groups))
        .nest_service("/static", ServeDir::new("templates/static"))
        .with_state(state)
}

async fn index(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, AppError> {
    let snapshot = state.kafka_client().fetch_snapshot().await?;
    let template = IndexTemplate {
        cluster: &snapshot.cluster,
        topics: &snapshot.topics,
        brokers: &snapshot.brokers,
        groups: &snapshot.groups,
    };

    Ok(Html(template.render()?))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "apache-kafka-ui",
    })
}

async fn api_stream(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stream = stream! {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;

            match state.kafka_client().fetch_snapshot().await {
                Ok(snapshot) => {
                    let payload = SnapshotResponse {
                        cluster: snapshot.cluster,
                        brokers: snapshot.brokers,
                        topics: snapshot.topics,
                        groups: snapshot.groups,
                    };

                    match serde_json::to_string(&payload) {
                        Ok(json) => {
                            yield Ok::<Event, Infallible>(Event::default().event("snapshot").data(json));
                        }
                        Err(error) => {
                            yield Ok::<Event, Infallible>(Event::default().event("error").data(format!("serialization error: {error}")));
                        }
                    }
                }
                Err(error) => {
                    yield Ok::<Event, Infallible>(Event::default().event("error").data(format!("kafka stream error: {error}")));
                }
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("keep-alive"))
}

async fn api_snapshot(
    State(state): State<Arc<AppState>>,
) -> Result<Json<SnapshotResponse>, AppError> {
    let snapshot = state.kafka_client().fetch_snapshot().await?;
    Ok(Json(SnapshotResponse {
        cluster: snapshot.cluster,
        brokers: snapshot.brokers,
        topics: snapshot.topics,
        groups: snapshot.groups,
    }))
}

async fn api_cluster(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ClusterOverview>, AppError> {
    let snapshot = state.kafka_client().fetch_snapshot().await?;
    Ok(Json(snapshot.cluster))
}

async fn api_topics(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListResponse<Topic>>, AppError> {
    let snapshot = state.kafka_client().fetch_snapshot().await?;
    Ok(Json(ListResponse {
        total: snapshot.topics.len(),
        items: snapshot.topics,
    }))
}

async fn api_brokers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListResponse<Broker>>, AppError> {
    let snapshot = state.kafka_client().fetch_snapshot().await?;
    Ok(Json(ListResponse {
        total: snapshot.brokers.len(),
        items: snapshot.brokers,
    }))
}

async fn api_groups(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListResponse<ConsumerGroup>>, AppError> {
    let snapshot = state.kafka_client().fetch_snapshot().await?;
    Ok(Json(ListResponse {
        total: snapshot.groups.len(),
        items: snapshot.groups,
    }))
}
