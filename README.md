# apache-kafka-ui

Lightweight Apache Kafka web UI written in Rust.

## Current status

This project is in early development. It already includes:

- Axum-based HTTP server
- Server-side rendered HTML with Askama
- JSON API endpoints for topics and brokers
- Docker and Docker Compose setup for local development

## Features

- Fast Rust backend
- Simple SSR dashboard
- REST API endpoints:
  - `GET /health`
  - `GET /api/topics`
  - `GET /api/brokers`

## Tech stack

- Rust 1.86+
- Axum
- Askama
- Tokio
- Tracing

## Getting started

### Run locally

```bash
cargo run
```

Open http://127.0.0.1:3000

### Environment variables

- `APP_HOST` (default: `127.0.0.1`)
- `APP_PORT` (default: `3000`)
- `RUST_LOG` (default: `info`)

### Docker

Build and run the app image:

```bash
docker build -t apache-kafka-ui .
docker run --rm -p 3000:3000 apache-kafka-ui
```

### Docker Compose (Kafka + UI)

A tracked example compose file is provided as `docker-compose.example.yml`.

```bash
cp docker-compose.example.yml docker-compose.yml
docker compose up --build
```

Open:

- UI: http://localhost:3000
- Kafka broker (host): localhost:9092

## Development

### Quality checks

```bash
cargo fmt
cargo check
```

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening an issue or pull request.

## Security

To report vulnerabilities, follow [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE).
