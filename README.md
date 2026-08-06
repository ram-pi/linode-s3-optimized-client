# S3 Optimized Client

A fast parallel downloader for S3-compatible object storage, built for Linode Object Storage. It maximizes download throughput by resolving the endpoint to all its public IPs and opening a dedicated TCP/TLS connection to each. For bulk downloads, it spawns multiple persistent worker processes per IP — each with its own Python GIL and keep-alive connection — so all IPs stay saturated without contention.

## Prerequisites

- Python 3.12+ and [uv](https://docs.astral.sh/uv/) (for local runs), or Docker.

## Running with Python (uv)

Install dependencies and run:

```sh
uv sync
uv run python -m s3_optimized_client \
  --bucket mybucket \
  --endpoint gb-lon-1.linodeobjects.com \
  --access-key AKID \
  --secret-key SECRET \
  --key path/to/object.tar \
  --output ./object.tar
```

Download a folder (prefix):

```sh
uv run python -m s3_optimized_client \
  --bucket mybucket \
  --endpoint gb-lon-1.linodeobjects.com \
  --access-key AKID \
  --secret-key SECRET \
  --prefix data/ \
  --output ./data/
```

Download an entire bucket:

```sh
uv run python -m s3_optimized_client \
  --bucket mybucket \
  --endpoint gb-lon-1.linodeobjects.com \
  --access-key AKID \
  --secret-key SECRET \
  --all \
  --output ./mybucket/
```

Resolve endpoint IPs only (no download):

```sh
uv run python -m s3_optimized_client \
  --endpoint gb-lon-1.linodeobjects.com \
  --dns-only
```

## Running with Docker

Pull from GHCR (or build locally with `docker build -t s3-optimized-client .`):

```sh
docker run --rm \
  -e AWS_ACCESS_KEY_ID=AKID \
  -e AWS_SECRET_ACCESS_KEY=SECRET \
  -v "$(pwd)/downloads:/home/app/downloads" \
  ghcr.io/<owner>/s3_optimized_client:latest \
  --bucket mybucket \
  --endpoint gb-lon-1.linodeobjects.com \
  --key path/to/object.tar \
  --output /home/app/downloads/object.tar
```

For a folder or whole bucket, mount a volume and point `--output` to it:

```sh
docker run --rm \
  -e AWS_ACCESS_KEY_ID=AKID \
  -e AWS_SECRET_ACCESS_KEY=SECRET \
  -v "$(pwd)/data:/home/app/data" \
  ghcr.io/<owner>/s3_optimized_client:latest \
  --bucket mybucket \
  --endpoint gb-lon-1.linodeobjects.com \
  --prefix data/ \
  --output /home/app/data/
```

## CLI reference

```
--endpoint HOST          Object storage endpoint (e.g. gb-lon-1.linodeobjects.com)
--bucket NAME            Bucket name
--access-key KEY         Access key (or env AWS_ACCESS_KEY_ID)
--secret-key KEY         Secret key (or env AWS_SECRET_ACCESS_KEY)
--region REGION          SigV4 region (default: us-east-1 or env AWS_DEFAULT_REGION)
--key KEY                Download a single object
--prefix PREFIX          Download all objects under a prefix
--all                    Download the entire bucket
--output PATH            Output file (--key) or directory (--prefix/--all)
--connections-per-ip N   Concurrent processes (TCP connections) per IP (default: 4)
--dns-only               Resolve endpoint to IPs and exit
--ipv6                   Include IPv6 addresses (default: IPv4 only)
--no-verify-tls          Disable TLS certificate verification (insecure)
-v / -vv                 Increase logging verbosity
```

## How it works

1. Resolves the endpoint hostname to all public IPs via `socket.getaddrinfo`.
2. For prefix/bucket downloads: spawns `connections_per_ip` persistent worker processes per IP (e.g. 12 IPs x 4 = 48 processes). Each process has its own GIL, SSL context, and keep-alive TCP connection.
3. Objects are distributed round-robin across IPs via per-IP task queues.
4. Each worker downloads full objects sequentially (no Range header, no chunk splitting) and writes them to disk.
5. A polling thread reads shared-memory counters to update the live progress bar.
6. Prints live progress (MB/s, ETA) and a per-IP summary table at completion.

## Notes

- Linode Object Storage uses **virtual-host style** addressing: `https://<bucket>.<endpoint>/<key>`.
- Credentials default to `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars; override with `--access-key` / `--secret-key`.
- SigV4 region defaults to `us-east-1`; override with `--region` or `AWS_DEFAULT_REGION`.
- Folder/bucket downloads recreate the local directory structure under `--output`.
- `--connections-per-ip` controls how many processes (and TCP connections) run per IP. Increase for more parallelism (e.g. `--connections-per-ip 8`), decrease to reduce resource usage.