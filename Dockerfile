# syntax=docker/dockerfile:1
FROM rust:1.95-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release && \
    cp target/release/translate-bot /translate-bot

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /translate-bot /usr/local/bin/translate-bot

VOLUME /app/store
VOLUME /app/config

WORKDIR /app

CMD ["translate-bot", "/app/config/config.toml"]
