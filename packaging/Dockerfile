# Build from config_analyzer/:
# docker build --platform linux/amd64 -f packaging/Dockerfile -t config-analyzer:hackathon .
# docker save config-analyzer:hackathon | gzip > config-analyzer.tar.gz
# scp config-analyzer.tar.gz packaging/config-analyzer.service admin@<switch>:
# On the switch: docker load -i config-analyzer.tar.gz
#   sudo cp config-analyzer.service /etc/systemd/system/
#   sudo systemctl daemon-reload && sudo systemctl enable --now config-analyzer
# From a laptop: ssh -L 8099:127.0.0.1:8099 admin@<switch>

FROM rust:1.98-bookworm AS builder
RUN apt-get update && apt-get install --no-install-recommends -y cmake libpcre2-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN cargo build --locked --release -p config_analyzer_service

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/config-analyzer-service /usr/local/bin/config-analyzer-service
COPY pipelines.yaml /etc/config-analyzer/pipelines.yaml
EXPOSE 8099
ENTRYPOINT ["/usr/local/bin/config-analyzer-service"]