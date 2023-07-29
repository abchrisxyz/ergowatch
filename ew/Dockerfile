# Based on https://github.com/LukeMathWalker/cargo-chef
FROM rust:1 AS chef 
# We only pay the installation cost once, 
# it will be cached from the second build onwards
RUN cargo install cargo-chef 
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --no-default-features --bin ew

# We do not need the Rust toolchain to run the binary!
FROM debian:11-slim AS runtime
RUN apt-get update && apt install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/ew /app/ew
ENTRYPOINT ["/app/ew"]
