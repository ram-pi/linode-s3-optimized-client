"""S3 client: virtual-host style URLs, SigV4 signing, forced-IP TLS connections.

Design
------
* Metadata (``HEAD`` to get object size) and bulk transfer (``Range`` GETs) go
  through a custom :class:`ForcedIPHTTPSConnection` that dials a *specific*
  resolved IP while keeping TLS SNI / certificate verification pinned to the
  virtual-host hostname (``<bucket>.<endpoint>``). This is what lets us open one
  TCP connection per public IP returned by the DNS lookup.
* Connections are pooled per (IP, bucket) and reused across range requests via
  keep-alive — TLS handshakes are expensive (~100-200 ms each) and re-handshaking
  per chunk dominated transfer time in early benchmarks.
* ``LIST`` (used for prefix / whole-bucket downloads) is low-volume and goes
  through ``boto3`` against the virtual-host endpoint; the heavy lifting (range
  GETs) still uses the forced-IP connections.
* Signing uses ``botocore.auth.S3SigV4Auth`` so we stay wire-compatible with
  AWS S3 / Linode Object Storage without re-implementing SigV4.

Notes
-----
* Linode Object Storage uses **virtual-host style** addressing:
  ``https://<bucket>.<endpoint>/<key>``.
* SigV4 region defaults to ``us-east-1`` (overridable via ``--region``).
"""

from __future__ import annotations

import http.client
import logging
import socket
import ssl
import threading
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Forced-IP TLS connection
# ---------------------------------------------------------------------------


class ForcedIPHTTPSConnection(http.client.HTTPSConnection):
    """An HTTPS connection that dials ``forced_ip`` but presents SNI and
    validates the TLS certificate against ``host`` (the virtual-host name).

    This is the core primitive enabling one TCP connection per resolved IP.
    """

    def __init__(  # noqa: PLR0913 - matches http.client signature
        self,
        host: str,
        forced_ip: str,
        *,
        port: int = 443,
        timeout: float | None = 30.0,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        super().__init__(host, port=port, timeout=timeout, context=ssl_context)
        self._forced_ip = forced_ip

    def connect(self) -> None:
        # Dial the forced IP directly (no DNS here — we already resolved it).
        # Use a short connect timeout so a dead/unreachable IP fails fast
        # instead of stalling the whole download for 60s.
        connect_timeout = min(self.timeout or 60.0, 10.0)
        raw_sock = socket.create_connection((self._forced_ip, self.port), timeout=connect_timeout)
        if self._tunnel_host:  # pragma: no cover - we don't use HTTP CONNECT
            self.sock = raw_sock
            self._tunnel()  # type: ignore[attr-defined]
            return
        # SNI + cert verification use the virtual-host hostname.
        self.sock = self._context.wrap_socket(raw_sock, server_hostname=self.host)


# ---------------------------------------------------------------------------
# SigV4 signer (botocore-backed)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class S3Credentials:
    access_key: str
    secret_key: str
    region: str = "us-east-1"


class S3Signer:
    """Signs an HTTP request with AWS SigV4 using botocore.

    Uses :class:`botocore.auth.S3SigV4Auth` (not the base ``SigV4Auth``) so the
    required ``X-Amz-Content-SHA256`` header is added — S3/Linode reject
    requests without it with HTTP 403.
    """

    def __init__(self, creds: S3Credentials) -> None:
        self._creds = creds
        # Import botocore lazily so ``--dns-only`` works without boto3 installed
        # at import time (still required by the project, but keeps this module
        # usable in isolation for unit tests of the connection class).
        from botocore.auth import S3SigV4Auth
        from botocore.credentials import Credentials

        self._auth = S3SigV4Auth(
            Credentials(access_key=creds.access_key, secret_key=creds.secret_key),
            "s3",
            creds.region,
        )

    def sign(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        *,
        body: bytes = b"",
    ) -> dict[str, str]:
        """Return a copy of *headers* with SigV4 authorization headers added."""
        from botocore.awsrequest import AWSRequest

        aws_req = AWSRequest(method=method.upper(), url=url, headers=dict(headers), data=body)
        self._auth.add_auth(aws_req)
        # AWSRequest headers are HTTPHeaderMapping; normalize to plain dict.
        return {k: v for k, v in aws_req.headers.items()}


# ---------------------------------------------------------------------------
# S3 client
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ObjectMeta:
    """Metadata for a single object."""

    key: str
    size: int


class S3Client:
    """S3-compatible client with per-IP forced connections.

    Args:
        endpoint: hostname without scheme/bucket, e.g. ``gb-lon-1.linodeobjects.com``.
        creds: credentials + region for SigV4 signing.
        ips: list of IPs to distribute connections across.
        verify_tls: whether to verify the server certificate (default True).
    """

    def __init__(
        self,
        endpoint: str,
        creds: S3Credentials,
        ips: list[str],
        *,
        verify_tls: bool = True,
        timeout: float = 60.0,
        pool_size_per_ip: int = 4,
    ) -> None:
        self.endpoint = endpoint
        self._creds = creds
        self._ips = list(ips)
        self._timeout = timeout
        self._pool_size_per_ip = pool_size_per_ip
        self._ssl_context = ssl.create_default_context()
        if not verify_tls:
            self._ssl_context.check_hostname = False
            self._ssl_context.verify_mode = ssl.CERT_NONE
        self._signer = S3Signer(creds)

        # Per-(bucket, IP) connection pool. Each IP gets up to
        # ``pool_size_per_ip`` reusable keep-alive connections so concurrent
        # range requests on the same IP don't pay a TLS handshake per chunk.
        # Keyed by (bucket, ip) since the vhost (SNI + Host) differs per bucket.
        self._pools: dict[tuple[str, str], list[ForcedIPHTTPSConnection]] = {}
        self._pool_locks: dict[tuple[str, str], threading.Lock] = {}
        self._pool_lock = threading.Lock()

    # -- URL helpers -------------------------------------------------------

    def _virtual_host(self, bucket: str) -> str:
        return f"{bucket}.{self.endpoint}"

    def _url(self, bucket: str, key: str) -> str:
        clean = key.lstrip("/")
        return f"https://{self._virtual_host(bucket)}/{clean}"

    def _new_conn(self, ip: str, bucket: str) -> ForcedIPHTTPSConnection:
        """Open a fresh connection to *ip* with SNI set to the bucket vhost."""
        return ForcedIPHTTPSConnection(
            self._virtual_host(bucket),
            ip,
            port=443,
            timeout=self._timeout,
            ssl_context=self._ssl_context,
        )

    def _checkout(self, bucket: str, ip: str) -> ForcedIPHTTPSConnection:
        """Get a connection from the pool (or create a new one)."""
        key = (bucket, ip)
        with self._pool_lock:
            if key not in self._pool_locks:
                self._pool_locks[key] = threading.Lock()
        with self._pool_locks[key]:
            pool = self._pools.setdefault(key, [])
            if pool:
                return pool.pop()
            return self._new_conn(ip, bucket)

    def _checkin(self, bucket: str, ip: str, conn: ForcedIPHTTPSConnection) -> None:
        """Return a healthy connection to the pool, or close it if full/broken."""
        key = (bucket, ip)
        with self._pool_locks.setdefault(key, threading.Lock()):
            pool = self._pools.setdefault(key, [])
            if len(pool) < self._pool_size_per_ip:
                pool.append(conn)
                return
        with suppress(OSError):
            conn.close()

    # -- HTTP request primitive --------------------------------------------

    def _request(
        self,
        method: str,
        bucket: str,
        key: str,
        ip: str,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], http.client.HTTPResponse, ForcedIPHTTPSConnection]:
        """Execute a single HTTP request to *ip* and return (status, headers, response, conn).

        Checks out a pooled keep-alive connection (or opens a new one if the pool
        is empty). The **caller must call** :meth:`_checkin` or ``conn.close()``
        once the response body is fully consumed. Retries once on reset; a stale
        pooled connection is discarded on failure and a fresh one is opened.
        """
        url = self._url(bucket, key)
        base_headers = {"Host": self._virtual_host(bucket)}
        if extra_headers:
            base_headers.update(extra_headers)
        signed = self._signer.sign(method, url, base_headers)

        last_exc: Exception | None = None
        for attempt in range(2):
            conn = self._checkout(bucket, ip) if attempt == 0 else self._new_conn(ip, bucket)
            try:
                conn.request(method, f"/{key.lstrip('/')}", headers=signed)
                resp = conn.getresponse()
                return resp.status, dict(resp.getheaders()), resp, conn
            except (http.client.HTTPException, OSError) as exc:
                last_exc = exc
                with suppress(OSError):
                    conn.close()
                if attempt == 0:
                    log.debug("request to %s failed (%s), retrying with fresh conn", ip, exc)
                    continue
                break
        assert last_exc is not None
        raise last_exc

    # -- Public API ---------------------------------------------------------

    @staticmethod
    def _header(headers: dict[str, str], name: str) -> str | None:
        """Case-insensitive header lookup (HTTP headers are case-insensitive).

        Linode returns lowercase ``content-length``; AWS returns ``Content-Length``.
        """
        name_lower = name.lower()
        for k, v in headers.items():
            if k.lower() == name_lower:
                return v
        return None

    def head_object(self, bucket: str, key: str, ip: str | None = None) -> ObjectMeta:
        """Return object metadata (size). Issues a ``HEAD`` to one IP."""
        ip = ip or self._ips[0]
        status, headers, resp, conn = self._request("HEAD", bucket, key, ip)
        try:
            if status != 200:
                body = resp.read(2048)
                msg = f"HEAD {bucket}/{key} failed: HTTP {status}: {body[:200]!r}"
                raise RuntimeError(msg)
            size = int(self._header(headers, "Content-Length") or "0")
            # Drain any body so the connection can be reused (keep-alive).
            resp.read()
            return ObjectMeta(key=key, size=size)
        finally:
            with suppress(OSError):
                resp.close()
            self._checkin(bucket, ip, conn)

    def range_get(
        self,
        bucket: str,
        key: str,
        start: int,
        end: int,
        ip: str,
    ) -> Iterator[bytes]:
        """Stream the byte range [start, end] (inclusive) from *ip*.

        Yields chunks of the response body. The caller is responsible for
        writing them at the correct offset. The connection is returned to the
        pool after the body is fully consumed.
        """
        range_header = f"bytes={start}-{end}"
        status, _headers, resp, conn = self._request(
            "GET", bucket, key, ip, extra_headers={"Range": range_header}
        )
        if status == 206:
            pass
        elif status == 200:
            log.warning(
                "server returned 200 (ignored Range) for %s/%s on %s; slicing", bucket, key, ip
            )
        else:
            body = resp.read(2048)
            with suppress(OSError):
                resp.close()
            with suppress(OSError):
                conn.close()
            msg = (
                f"Range GET {bucket}/{key} bytes={start}-{end} failed: "
                f"HTTP {status}: {body[:200]!r}"
            )
            raise RuntimeError(msg)

        # Stream the body, then return the connection to the pool for reuse.
        try:
            while True:
                chunk = resp.read(256 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            with suppress(OSError):
                resp.close()
            self._checkin(bucket, ip, conn)

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        *,
        endpoint_url: str | None = None,
    ) -> Iterator[ObjectMeta]:
        """List objects under *prefix* using boto3 (low-volume metadata path).

        This deliberately uses boto3's well-tested pagination + XML parsing
        rather than re-implementing ListObjectsV2. The bulk transfer still
        uses forced-IP connections.
        """
        import boto3
        from botocore.client import Config

        endpoint = endpoint_url or f"https://{self.endpoint}"
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=self._creds.access_key,
            aws_secret_access_key=self._creds.secret_key,
            region_name=self._creds.region,
            config=Config(s3={"addressing_style": "virtual"}, signature_version="s3v4"),
        )
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield ObjectMeta(key=obj["Key"], size=int(obj["Size"]))

    # -- Lifecycle ---------------------------------------------------------

    def close(self) -> None:
        with self._pool_lock:
            for conns in self._pools.values():
                for conn in conns:
                    with suppress(OSError):
                        conn.close()
            self._pools.clear()

    def __enter__(self) -> S3Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
