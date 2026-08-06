# S3 Optimized Client

A fast parallel downloader for S3-compatible object storage, built for Linode Object Storage. It maximizes download throughput by resolving the endpoint to all its public IPs and opening a dedicated TCP/TLS connection to each. For bulk downloads, it spawns multiple persistent worker processes per IP — each with its own Python GIL and keep-alive connection — so all IPs stay saturated without contention.

## Prerequisites

- Python 3.12+ and [uv](https://docs.astral.sh/uv/) (for local runs), or Docker.

## Dependencies

| Library | Purpose |
|---|---|
| [boto3](https://github.com/boto/boto3) / [botocore](https://github.com/boto/botocore) | S3 ListObjectsV2 (prefix/bucket listing) + SigV4 request signing |
| [rich](https://github.com/Textualize/rich) | Live progress bar + summary table rendering |

Everything else (DNS resolution, HTTP/TLS, file I/O, multiprocessing) uses the Python standard library.

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
--size-only              Calculate and print total size without downloading
--connections-per-ip N   Concurrent processes (TCP connections) per IP (default: 4)
--dns-only               Resolve endpoint to IPs and exit
--ipv6                   Include IPv6 addresses (default: IPv4 only)
--no-verify-tls          Disable TLS certificate verification (insecure)
-v / -vv                 Increase logging verbosity
```

## How it works

See [`docs/diagram.mermaid`](docs/diagram.mermaid) for visual flow diagrams.

1. **DNS lookup** — resolves the endpoint hostname to all public IPs via `socket.getaddrinfo`.
2. **List objects** — uses boto3 to list all objects under the prefix.
3. **Distribute work** — objects are distributed round-robin across per-IP task queues.
4. **Worker processes** — `connections_per_ip` processes per IP, each with its own GIL, SSL context, and keep-alive TCP connection. Each process downloads full objects sequentially (no Range header, no chunk splitting) and writes them to disk.
5. **Progress tracking** — a polling thread in the main process reads shared-memory counters every 0.5s and updates the live progress bar.
6. **Summary** — prints a per-IP summary table (bytes, chunks, avg speed) at completion.

## Understanding `--connections-per-ip`

This flag controls how many **worker processes** (and TCP connections) run per resolved IP. Each process has its own Python GIL and its own keep-alive connection.

### Why not just 1?

A single TCP connection to an S3 endpoint may not saturate the available bandwidth per IP. Some providers cap per-connection throughput below the per-IP cap. In that case, multiple concurrent connections multiply throughput:

| `--connections-per-ip` | TCP connections per IP | When to use |
|---|---|---|
| 1 | 1 | Single connection already saturates per-IP bandwidth (e.g. Linode Object Storage, where 1 connection hits ~125 MiB/s per IP) |
| 4 (default) | 4 | Provider caps per-connection throughput below per-IP cap (e.g. AWS S3, Backblaze B2) |
| 8+ | 8+ | High-bandwidth links where 4 connections aren't enough; more processes = more memory (~50 MB each) |

### How to know which value to use

Run with `--connections-per-ip 1` and check the per-IP avg speed in the summary. Then try `--connections-per-ip 4`:

- **No improvement** → 1 connection already saturates per-IP bandwidth. Use `1` (saves memory and processes).
- **Improvement** → per-connection throughput was the bottleneck. Increase until throughput stops improving.

On Linode Object Storage, 1 connection per IP typically hits the per-IP bandwidth cap (~1 Gbps), so higher values don't help. On other providers (AWS S3, MinIO, etc.), 4-8 connections per IP may significantly improve throughput.

### Resource cost

Each process uses ~50 MB of memory (Python interpreter + SSL context + buffers). With 12 IPs:

| `--connections-per-ip` | Total processes | Memory |
|---|---|---|
| 1 | 12 | ~600 MB |
| 4 | 48 | ~2.4 GB |
| 8 | 96 | ~4.8 GB |

## Notes

- Linode Object Storage uses **virtual-host style** addressing: `https://<bucket>.<endpoint>/<key>`.
- Credentials default to `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars; override with `--access-key` / `--secret-key`.
- SigV4 signing requires a region parameter. Linode Object Storage ignores it for routing (the endpoint URL already encodes the region, e.g. `gb-lon-1`), but the signature must include a valid region string. `us-east-1` works as a placeholder; override with `--region` or `AWS_DEFAULT_REGION` if your provider requires a specific value.
- Folder/bucket downloads recreate the local directory structure under `--output`.
- The GIL (Global Interpreter Lock) is a built-in part of CPython that allows only one thread to execute Python bytecode at a time. This tool works around it by using `multiprocessing` — each worker process has its own GIL, so they run independently without contention. Standard CPython 3.12 is used (not the experimental free-threaded 3.13 build).
