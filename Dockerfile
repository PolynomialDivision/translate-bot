# syntax=docker/dockerfile:1
# ── Base: chef + build deps ───────────────────────────────────────────────────
FROM rust:1.98.0-slim-bookworm AS chef
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev libsqlite3-dev git \
    && rm -rf /var/lib/apt/lists/*
# sccache (via openssl-sys) needs pkg-config/libssl-dev at its own build
# time, so this must come after the apt-get above.
RUN cargo install cargo-chef sccache --locked

# sccache wraps rustc so identical compilation work (e.g. the matrix-sdk /
# ruma / mxbot-common dependency graph shared by these bots) is reused from
# one BuildKit cache mount instead of recompiled per project. Incremental
# compilation writes its own per-crate cache that fights sccache's
# object-level cache, so it's disabled here per sccache's own guidance.
ENV RUSTC_WRAPPER=sccache \
    SCCACHE_DIR=/sccache \
    SCCACHE_CACHE_SIZE=20G \
    CARGO_INCREMENTAL=0
WORKDIR /build

# ── Planner: capture the full dependency graph ────────────────────────────────
FROM chef AS planner
COPY . .
RUN --mount=type=cache,id=shared-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=shared-cargo-registry,target=/usr/local/cargo/registry \
    cargo chef prepare --recipe-path recipe.json

# ── Builder ───────────────────────────────────────────────────────────────────
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json

RUN --mount=type=cache,id=shared-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=shared-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=shared-sccache,target=/sccache \
    --mount=type=cache,id=translate-bot-target,target=/build/target \
    cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN --mount=type=cache,id=shared-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=shared-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=shared-sccache,target=/sccache \
    --mount=type=cache,id=translate-bot-target,target=/build/target \
    cargo build --release && \
    cp target/release/translate-bot /translate-bot

# ── Runtime ───────────────────────────────────────────────────────────────────
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
