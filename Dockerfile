# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY sql sql
COPY benches benches
COPY tests tests
RUN cargo build --release --locked --bin pgshadow

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 pgshadow
COPY --from=build /src/target/release/pgshadow /usr/local/bin/pgshadow
USER pgshadow
EXPOSE 6432 9187
# Configure with a file (-c /etc/pgshadow.toml) and/or PGSHADOW_<SECTION>_<KEY> variables.
ENTRYPOINT ["pgshadow"]
CMD ["serve"]
