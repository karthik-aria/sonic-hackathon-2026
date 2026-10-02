# SONiC Config Analyzer

Captures and serves Perfetto traces for SONiC configuration changes. The service
watches Redis MONITOR streams, correlates CONFIG_DB / APPL_DB / ASIC_DB writes for
the pipelines listed in `pipelines.yaml`, and exposes the resulting traces over HTTP.

## Build the Docker image

Build from this directory (`config_analyzer/`) — the Dockerfile expects the
workspace root as its build context:

```bash
docker build --platform linux/amd64 -f packaging/Dockerfile -t config-analyzer:hackathon .
```

The multi-stage build compiles `config_analyzer_service` in release mode inside a
`rust:1.98-bookworm` builder (which installs `cmake` and `libpcre2-dev` for the
`libyang3-sys` build script) and copies the resulting binary plus `pipelines.yaml`
into a `debian:bookworm-slim` runtime image.

Optionally export the image for transfer to a switch:

```bash
docker save config-analyzer:hackathon | gzip > config-analyzer.tar.gz
```

## Deploy to a switch

```bash
scp config-analyzer.tar.gz packaging/config-analyzer.service admin@<switch>:
```

On the switch:

```bash
docker load -i config-analyzer.tar.gz
sudo cp config-analyzer.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now config-analyzer
```

The unit file runs the container with `--network host`, mounts `/var/run/redis`,
`/run/redis/auth`, and the host's YANG models, and listens on `0.0.0.0:8099`.

## View traces

From a laptop:

```bash
ssh -L 8099:127.0.0.1:8099 admin@<switch>
```
Then open the HTTP endpoint at `http://127.0.0.1:8099`.

Sample Screenshots

<img width="1237" height="834" alt="Screenshot 2026-10-01 at 9 22 31 PM" src="https://github.com/user-attachments/assets/d8bc5170-3e63-4f90-a223-b1f194a305fd" />

<img width="1707" height="817" alt="Screenshot 2026-10-01 at 9 22 42 PM" src="https://github.com/user-attachments/assets/ee00c783-2f22-4cbe-a8e6-3ef2f13d09a4" />



