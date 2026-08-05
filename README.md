# S3 Optimized Client

A fast parallel downloader for S3-compatible object storage, built for Linode Object Storage. It maximizes download throughput by using every IP the endpoint resolves to — each IP gets its own TCP/TLS connection, and each object is split into byte ranges fetched concurrently across all of them.

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
--chunks N               Override byte-range chunk count (auto by default)
--concurrency N          Parallel objects for --prefix/--all (default: number of IPs)
--connections-per-ip N   Concurrent TCP connections per IP (default: 4)
--min-chunk-mb N         Minimum chunk size in MiB for auto chunking (default: 2)
--dns-only               Resolve endpoint to IPs and exit
--ipv6                   Include IPv6 addresses (default: IPv4 only)
--no-verify-tls          Disable TLS certificate verification (insecure)
-v / -vv                 Increase logging verbosity
```

## How it works

1. Resolves the endpoint hostname to all public IPs via `socket.getaddrinfo`.
2. Opens a dedicated TCP/TLS connection per IP (SNI pinned to the virtual host).
3. Splits each object into byte ranges distributed round-robin across IPs.
4. Downloads ranges concurrently with a per-IP keep-alive connection pool.
5. Writes chunks to the correct file offset with `os.pwrite` (lock-free, parallel).
6. Prints live progress (MB/s, ETA) and a per-IP summary table at completion.

## Notes

- Linode Object Storage uses **virtual-host style** addressing: `https://<bucket>.<endpoint>/<key>`.
- Credentials default to `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars; override with `--access-key` / `--secret-key`.
- SigV4 region defaults to `us-east-1`; override with `--region` or `AWS_DEFAULT_REGION`.
- Folder/bucket downloads recreate the local directory structure under `--output`.