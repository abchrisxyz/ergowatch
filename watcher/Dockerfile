FROM rust as builder
WORKDIR /usr/src/watcher
COPY ./src ./src
COPY ./Cargo.toml ./Cargo.toml
RUN cargo install --path .

FROM debian:bullseye-slim
WORKDIR /ew
COPY --from=builder /usr/local/cargo/bin/watcher /ew/watcher
CMD ["./watcher", "-c", "/ew/config.toml"]