# Build context MUST be the repo root so that librespot/ and inferno/ (the path
# dependencies, with their git submodules checked out) are included. Initialise
# the inferno submodules before building:
#   git submodule update --init --recursive
# (searchfire, usrvclock-rs and alsa-sys-all under inferno/).

FROM rust:1.88-bookworm AS build
WORKDIR /build

# Manifests first for better layer caching of the dependency build.
COPY Cargo.toml Cargo.lock ./
COPY librespot ./librespot
COPY inferno ./inferno
COPY src ./src

RUN cargo build --release --locked --bin lucyfer

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/target/release/lucyfer /usr/local/bin/lucyfer

# inferno persists channel renames / TX subscriptions under $XDG_STATE_HOME.
ENV XDG_STATE_HOME=/var/lib/lucyfer/state
VOLUME /var/lib/lucyfer

ENTRYPOINT ["lucyfer", "--config", "/etc/lucyfer/config.yaml"]
