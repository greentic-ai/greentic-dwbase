# Multi-stage build for dwbase-node: minimal runtime, non-root.

# Builder
FROM rust:1.81-slim-bookworm AS builder
WORKDIR /app
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates build-essential && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY deploy ./deploy
COPY docs ./docs
COPY config.example.toml ./
RUN cargo build -p dwbase-node --release

# Runtime
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/dwbase-node /usr/local/bin/dwbase-node
# Non-root user
RUN useradd -u 10001 -m dwbase && mkdir -p /data && chown -R dwbase:dwbase /data
USER dwbase
ENV RUST_LOG=info
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/dwbase-node", "--config", "/config/config.toml"]
