FROM rust:1.94-bookworm AS builder

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake pkg-config g++ \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY templates ./templates

RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

ARG KAFKA_VERSION=3.8.1
ARG SCALA_VERSION=2.13

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl bash openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -o /tmp/kafka.tgz \
    && mkdir -p /opt \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka \
    && rm -f /tmp/kafka.tgz

WORKDIR /app

RUN useradd --system --create-home --uid 10001 appuser

# Runtime allowlist: copy only the app binary and templates with explicit ownership.
COPY --from=builder --chown=10001:10001 /app/target/release/apache-kafka-ui /usr/local/bin/apache-kafka-ui
COPY --from=builder --chown=10001:10001 /app/templates ./templates

RUN chmod 0555 /usr/local/bin/apache-kafka-ui \
    && chmod -R a-w /app/templates

ENV APP_HOST=0.0.0.0
ENV APP_PORT=3000
ENV RUST_LOG=info
ENV HOME=/home/appuser
ENV PATH="/opt/kafka/bin:${PATH}"

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 CMD curl -fsS http://127.0.0.1:3000/health || exit 1

USER 10001:10001

CMD ["apache-kafka-ui"]