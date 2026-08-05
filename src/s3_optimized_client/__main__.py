"""CLI entrypoint for the S3 optimized client.

Usage examples
--------------
Single object::

    s3-optimized-client --bucket mybucket \\
        --endpoint gb-lon-1.linodeobjects.com \\
        --key bigfile.tar --output ./bigfile.tar

Prefix (folder)::

    s3-optimized-client --bucket mybucket \\
        --endpoint gb-lon-1.linodeobjects.com \\
        --prefix data/ --output ./data/

Whole bucket::

    s3-optimized-client --bucket mybucket \\
        --endpoint gb-lon-1.linodeobjects.com --all --output ./mybucket/

DNS-only (debug)::

    s3-optimized-client --endpoint gb-lon-1.linodeobjects.com --dns-only

Credentials default to ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``;
override with ``--access-key`` / ``--secret-key``. Region defaults to
``AWS_DEFAULT_REGION`` or ``us-east-1``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from . import __version__
from .dns import resolve_endpoint
from .downloader import ParallelDownloader
from .s3 import S3Client, S3Credentials

log = logging.getLogger("s3_optimized_client")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="s3-optimized-client",
        description=(
            "Fast parallel downloader for S3-compatible object storage "
            "(Linode Object Storage). Resolves the endpoint to multiple IPs, "
            "opens one TCP connection per IP, and downloads byte ranges in parallel."
        ),
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    # Connection
    p.add_argument(
        "--endpoint",
        required=True,
        help="Object storage endpoint hostname, e.g. gb-lon-1.linodeobjects.com",
    )
    p.add_argument("--bucket", help="Bucket name (required unless --dns-only)")
    p.add_argument(
        "--region",
        default=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        help="SigV4 region (default: us-east-1 or AWS_DEFAULT_REGION)",
    )
    p.add_argument(
        "--no-verify-tls",
        action="store_true",
        help="Disable TLS certificate verification (insecure)",
    )

    # Credentials
    p.add_argument(
        "--access-key",
        default=os.environ.get("AWS_ACCESS_KEY_ID"),
        help="Access key (default: AWS_ACCESS_KEY_ID)",
    )
    p.add_argument(
        "--secret-key",
        default=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        help="Secret key (default: AWS_SECRET_ACCESS_KEY)",
    )

    # What to download (mutually exclusive group)
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--key", help="Download a single object key")
    mode.add_argument("--prefix", help="Download all objects under a prefix (folder)")
    mode.add_argument("--all", action="store_true", help="Download the entire bucket")

    # Output
    p.add_argument(
        "--output", "-o", help="Output file (--key) or directory (--prefix/--all); default: cwd"
    )

    # Tuning
    p.add_argument(
        "--chunks", type=int, default=None, help="Override byte-range chunk count (auto by default)"
    )
    p.add_argument(
        "--min-chunk-mb",
        type=int,
        default=2,
        help="Minimum chunk size in MiB for auto chunking (default: 2)",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Parallel objects for --prefix/--all (default: number of resolved IPs)",
    )

    # Debug
    p.add_argument("--dns-only", action="store_true", help="Resolve endpoint to IPs and exit")
    p.add_argument("-v", "--verbose", action="count", default=0, help="Increase logging (-v, -vv)")
    return p


def configure_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity >= 2:
        level = logging.DEBUG
    elif verbosity == 1:
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.verbose)

    # --dns-only short-circuit -------------------------------------------------
    if args.dns_only:
        ips = resolve_endpoint(args.endpoint)
        print(f"Resolved {args.endpoint} -> {len(ips)} IP(s):")
        for ip in ips:
            tag = "IPv6" if ip.is_ipv6 else "IPv4"
            print(f"  {tag} {ip.ip}")
        return 0

    # Validate required args for a real download.
    if not args.bucket:
        log.error("--bucket is required unless --dns-only is used")
        return 2
    if not (args.key or args.prefix or args.all):
        log.error("one of --key, --prefix, or --all is required")
        return 2
    if not args.access_key or not args.secret_key:
        log.error(
            "credentials required: provide --access-key/--secret-key or "
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"
        )
        return 2

    # Resolve endpoint to IPs.
    try:
        resolved = resolve_endpoint(args.endpoint)
    except Exception as exc:  # noqa: BLE001
        log.error("failed to resolve %s: %s", args.endpoint, exc)
        return 1
    ips = [r.ip for r in resolved]
    log.info("resolved %s -> %d IPs: %s", args.endpoint, len(ips), ", ".join(ips))

    creds = S3Credentials(
        access_key=args.access_key,
        secret_key=args.secret_key,
        region=args.region,
    )

    with S3Client(
        args.endpoint,
        creds,
        ips,
        verify_tls=not args.no_verify_tls,
    ) as client:
        downloader = ParallelDownloader(
            client,
            ips,
            chunks_override=args.chunks,
            min_chunk_mb=args.min_chunk_mb,
            object_concurrency=args.concurrency,
            verify_tls=not args.no_verify_tls,
        )

        try:
            if args.key:
                out = Path(args.output) if args.output else Path(args.key).name
                res = downloader.download_object(args.bucket, args.key, out)
                if not res.success:
                    log.error("download failed: %s", res.error)
                    return 1
                log.info("done: %s (%d bytes, %.2fs)", res.output_path, res.size, res.elapsed)
            elif args.prefix is not None:
                out = Path(args.output) if args.output else Path(args.prefix)
                results = downloader.download_prefix(args.bucket, args.prefix, out)
                failed = [r for r in results if not r.success]
                if failed:
                    log.error("%d/%d objects failed", len(failed), len(results))
                    for r in failed:
                        log.error("  %s: %s", r.key, r.error)
                    return 1
            elif args.all:
                out = Path(args.output) if args.output else Path(args.bucket)
                results = downloader.download_prefix(args.bucket, "", out)
                failed = [r for r in results if not r.success]
                if failed:
                    log.error("%d/%d objects failed", len(failed), len(results))
                    for r in failed:
                        log.error("  %s: %s", r.key, r.error)
                    return 1
        except Exception as exc:  # noqa: BLE001 - top-level error surface
            log.error("download error: %s", exc)
            if args.verbose >= 2:
                raise
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
