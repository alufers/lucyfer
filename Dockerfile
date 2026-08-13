# Build context MUST be the repo root so that inferno/ (the path dependency) and
# statime/ (the bundled PTP daemon) are included, both with their git submodules
# checked out. Initialise them before building:
#   git submodule update --init --recursive
# (searchfire, usrvclock-rs and alsa-sys-all under inferno/; timestamped-socket and
# clock-steering under statime/).

FROM rust:1.88-bookworm AS build
WORKDIR /build

# Manifests first for better layer caching of the dependency build.
COPY Cargo.toml Cargo.lock ./
COPY inferno ./inferno
COPY src ./src

RUN cargo build --release --locked --bin lucyfer

# The PTP daemon: teodly's Statime fork, the only one that speaks Dante's PTPv1 and
# can export a usrvclock media clock. Built as a separate stage (and as a plain
# binary, not a linked dependency) so lucyfer source changes do not rebuild it.
# Licence note: lucyfer is GPL-3.0-or-later, Statime is Apache-2.0 OR MIT — the two
# only ship side by side, they are not combined into one work.
FROM rust:1.88-bookworm AS statime-build
WORKDIR /build
COPY statime ./statime
RUN cargo build --release --locked --manifest-path statime/Cargo.toml \
    -p statime-linux --bin statime

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/target/release/lucyfer /usr/local/bin/lucyfer
COPY --from=statime-build /build/statime/target/release/statime /usr/local/bin/statime
COPY docker/statime.toml /etc/lucyfer/statime.toml
COPY docker/entrypoint.sh /usr/local/bin/lucyfer-entrypoint.sh

# inferno persists channel renames / TX subscriptions under $XDG_STATE_HOME.
ENV XDG_STATE_HOME=/var/lib/lucyfer/state
VOLUME /var/lib/lucyfer

# Statime only runs when LUCYFER_PTP is enabled; unset means lucyfer alone, with the
# media clock coming from somewhere else. See docker/entrypoint.sh.
ENTRYPOINT ["/usr/local/bin/lucyfer-entrypoint.sh"]
