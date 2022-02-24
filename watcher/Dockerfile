FROM rust as builder
WORKDIR /usr/src/watcher
COPY ./src ./src
COPY ./Cargo.toml ./Cargo.toml
RUN cargo install --path .

FROM debian:bullseye-slim
WORKDIR /ew
COPY --from=builder /usr/local/cargo/bin/watcher /ew/watcher
COPY ./db/constraints.sql /ew/constraints.sql
ENTRYPOINT ["./watcher", "-c", "/ew/config.toml"]
CMD ["-k", "/ew/constraints.sql"]