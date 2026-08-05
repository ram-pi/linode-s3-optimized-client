"""Parallel byte-range downloader.

Downloads a single object by splitting it into byte ranges, fetching each
range concurrently on a TCP connection pinned to a specific resolved IP, and
writing the bytes to the correct offset in the output file. Also supports
downloading a prefix (folder) or an entire bucket by listing objects and
running the single-object downloader concurrently.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .chunk_pool import Range, plan_chunks
from .stats import ProgressTracker

if TYPE_CHECKING:
    from .s3 import ObjectMeta, S3Client

log = logging.getLogger(__name__)

# Write buffer size for chunk workers.
WRITE_CHUNK = 64 * 1024


@dataclass(slots=True)
class DownloadResult:
    """Outcome of a single-object download."""

    key: str
    output_path: Path
    size: int
    elapsed: float
    chunks: int
    success: bool
    error: str | None = None


class ParallelDownloader:
    """Orchestrates parallel byte-range downloads across resolved IPs."""

    def __init__(
        self,
        client: S3Client,
        ips: list[str],
        *,
        chunks_override: int | None = None,
        min_chunk_mb: int = 8,
        object_concurrency: int | None = None,
        verify_tls: bool = True,
    ) -> None:
        self._client = client
        self._ips = ips
        self._chunks_override = chunks_override
        self._min_chunk_mb = min_chunk_mb
        # Default concurrency to the number of resolved IPs so every TCP
        # connection stays busy without the user having to tune anything.
        self._object_concurrency = max(1, object_concurrency or len(ips))
        self._verify_tls = verify_tls

    # -- Single object -----------------------------------------------------

    def download_object(
        self,
        bucket: str,
        key: str,
        output_path: Path,
        *,
        tracker: ProgressTracker | None = None,
    ) -> DownloadResult:
        """Download *bucket*/*key* to *output_path* using parallel ranges.

        Args:
            tracker: optional shared tracker (used in multi-object mode to show
                aggregate progress). When ``None``, a fresh tracker is created
                for this object and a summary is printed at the end.
        """
        start = time.monotonic()
        # Resolve object size via HEAD (use the first IP).
        meta = self._client.head_object(bucket, key, ip=self._ips[0])
        total = meta.size
        if total == 0:
            # Empty object: just create the file.
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.touch()
            elapsed = time.monotonic() - start
            return DownloadResult(key, output_path, 0, elapsed, 0, True)

        ranges = plan_chunks(
            total,
            self._ips,
            chunks_override=self._chunks_override,
            min_chunk_mb=self._min_chunk_mb,
        )

        # Preallocate the output file. We use os.pwrite for concurrent writes
        # at offsets — pwrite is atomic and needs no lock, so workers write
        # directly to disk in parallel without serializing through a mutex.
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(output_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            os.ftruncate(fd, total)

            own_tracker = tracker is None
            if own_tracker:
                tracker = ProgressTracker(total, ip_label=f"{bucket}/{key}")
                tracker.start()

            errors: list[str] = []
            err_lock = threading.Lock()

            def _worker(rng: Range) -> None:
                try:
                    offset = rng.start
                    for buf in self._client.range_get(bucket, key, rng.start, rng.end, rng.ip):
                        # os.pwrite writes at an absolute offset without moving
                        # the file offset — safe for concurrent workers.
                        view = memoryview(buf)
                        written = 0
                        while written < len(buf):
                            n = os.pwrite(fd, view[written:], offset + written)
                            written += n
                        offset += len(buf)
                        if tracker is not None:
                            tracker.add_bytes(rng.ip, len(buf))
                    if tracker is not None:
                        tracker.add_chunk(rng.ip)
                except Exception as exc:  # noqa: BLE001 - surface any failure
                    with err_lock:
                        errors.append(f"chunk {rng.index} ({rng.ip}): {exc}")
                    log.exception("chunk %d failed", rng.index)

            with ThreadPoolExecutor(max_workers=len(ranges)) as pool:
                futures = [pool.submit(_worker, r) for r in ranges]
                for fut in as_completed(futures):
                    fut.result()

            os.fsync(fd)

            if own_tracker and tracker is not None:
                elapsed = tracker.stop()
                tracker.print_summary(
                    title=f"Summary: {bucket}/{key}", object_label=None, elapsed=elapsed
                )
            elif tracker is not None:
                elapsed = time.monotonic() - start
        finally:
            os.close(fd)

        success = not errors
        return DownloadResult(
            key=key,
            output_path=output_path,
            size=total,
            elapsed=elapsed,
            chunks=len(ranges),
            success=success,
            error="; ".join(errors) if errors else None,
        )

    # -- Prefix / bucket ---------------------------------------------------

    def download_prefix(
        self,
        bucket: str,
        prefix: str,
        output_dir: Path,
        *,
        recursive: bool = True,
    ) -> list[DownloadResult]:
        """Download all objects under *prefix* into *output_dir*.

        Each object's key is mapped to a relative path under *output_dir* by
        stripping *prefix* (or the leading bucket name component). Concurrent
        objects are downloaded via a thread pool capped at
        ``object_concurrency``.
        """
        results: list[DownloadResult] = []
        metas = list(self._client.list_objects(bucket, prefix=prefix))
        if not metas:
            log.warning("no objects found under prefix %r in bucket %r", prefix, bucket)
            return results

        # Shared tracker spanning all objects.
        total_bytes = sum(m.size for m in metas)
        tracker = ProgressTracker(
            total_bytes, ip_label=f"{bucket}: {len(metas)} objects", show_live=True
        )
        tracker.start()

        def _one(meta: ObjectMeta) -> DownloadResult:
            rel = _relative_key(meta.key, prefix)
            out = output_dir / rel
            out.parent.mkdir(parents=True, exist_ok=True)
            try:
                return self.download_object(bucket, meta.key, out, tracker=tracker)
            except Exception as exc:  # noqa: BLE001 - isolate per-object failure
                log.error("object %s failed: %s", meta.key, exc)
                # Account for the skipped bytes so the progress bar total is
                # still consistent (mark them as "downloaded" so the bar fills).
                tracker.add_bytes(self._ips[0], 0)
                return DownloadResult(
                    key=meta.key,
                    output_path=out,
                    size=meta.size,
                    elapsed=0.0,
                    chunks=0,
                    success=False,
                    error=str(exc),
                )

        with ThreadPoolExecutor(max_workers=self._object_concurrency) as pool:
            futures = {pool.submit(_one, m): m for m in metas}
            for fut in as_completed(futures):
                res = fut.result()
                results.append(res)

        elapsed = tracker.stop()
        tracker.print_summary(
            title=f"Summary: {bucket}/{prefix or '(root)'}",
            object_label=f"{len(results)} objects, {sum(r.size for r in results)} bytes",
            elapsed=elapsed,
        )
        return results


def _relative_key(key: str, prefix: str) -> str:
    """Map an object key to a filesystem path relative to *prefix*.

    Strips the leading *prefix* (and any bucket name that may have been
    included). Ensures no absolute path escapes the output directory.
    """
    rel = key
    if prefix and rel.startswith(prefix):
        rel = rel[len(prefix) :]
    rel = rel.lstrip("/")
    # Guard against path traversal: drop any leading ``..`` components.
    parts = [p for p in rel.split("/") if p not in ("", ".", "..")]
    return "/".join(parts)
