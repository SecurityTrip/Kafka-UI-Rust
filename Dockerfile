FROM rust:1.94-bookworm AS builder

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake pkg-config g++ \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY templates ./templates

RUN cargo build --release

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

COPY --from=builder /app/target/release/apache-kafka-ui /usr/local/bin/apache-kafka-ui
COPY --from=builder /app/templates ./templates

ENV APP_HOST=0.0.0.0
ENV APP_PORT=3000
ENV RUST_LOG=info
ENV PATH="/opt/kafka/bin:${PATH}"

EXPOSE 3000

CMD ["apache-kafka-ui"]