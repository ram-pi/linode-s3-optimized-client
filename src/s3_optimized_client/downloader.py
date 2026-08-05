"""Parallel byte-range downloader using multiprocessing.

Downloads a single object by splitting it into byte ranges, fetching each
range on a TCP connection pinned to a specific resolved IP, and writing the
bytes to the correct offset in the output file. Each IP gets its own **process**
(not thread) so the Python GIL doesn't serialize I/O across IPs — this is the
key difference that enables per-IP throughput beyond ~35 MiB/s.

Also supports downloading a prefix (folder) or an entire bucket by listing
objects and running the single-object downloader concurrently.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .chunk_pool import Range, group_ranges_by_ip, plan_chunks
from .s3 import S3Client, S3Credentials
from .stats import ProgressTracker

if TYPE_CHECKING:
    from .s3 import ObjectMeta

log = logging.getLogger(__name__)


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


# ---------------------------------------------------------------------------
# Module-level worker function (must be picklable for ProcessPoolExecutor).
# ---------------------------------------------------------------------------

# Module-level globals set by the pool initializer via fork inheritance.
# These are shared-memory objects that can't be passed as submit() args.
_shared_bytes: multiprocessing.Array | None = None
_shared_chunks: multiprocessing.Array | None = None
_error_queue: multiprocessing.Queue | None = None


def _pool_initializer(shared_bytes, shared_chunks, error_queue):
    """Set module-level shared objects in each worker process.

    Called once per worker process at startup. With fork(), the shared arrays
    and queue are inherited by the child, so we just stash references here.
    """
    global _shared_bytes, _shared_chunks, _error_queue
    _shared_bytes = shared_bytes
    _shared_chunks = shared_chunks
    _error_queue = error_queue


def _download_ranges_worker(
    ip: str,
    ranges: list[Range],
    bucket: str,
    key: str,
    output_path: str,
    endpoint: str,
    creds: S3Credentials,
    verify_tls: bool,
    ip_index: int,
) -> int:
    """Download all *ranges* assigned to *ip* into *output_path*.

    Runs in a **separate process** with its own GIL, SSL context, and
    connection pool. Writes to the output file via ``os.pwrite`` at absolute
    offsets — multiple processes can write to the same file concurrently
    because each opens its own fd and pwrite is atomic at the kernel level.

    Uses module-level ``_shared_bytes`` / ``_shared_chunks`` / ``_error_queue``
    set by :func:`_pool_initializer`.

    Returns the total bytes downloaded (also accumulated in shared memory).
    """
    client = S3Client(endpoint, creds, [ip], verify_tls=verify_tls)
    fd = os.open(output_path, os.O_WRONLY)
    total_downloaded = 0
    try:
        for rng in ranges:
            offset = rng.start
            for buf in client.range_get(bucket, key, rng.start, rng.end, ip):
                view = memoryview(buf)
                written = 0
                while written < len(buf):
                    n = os.pwrite(fd, view[written:], offset + written)
                    written += n
                offset += len(buf)
                total_downloaded += len(buf)
                with _shared_bytes.get_lock():
                    _shared_bytes[ip_index] += len(buf)
        with _shared_chunks.get_lock():
            _shared_chunks[ip_index] += len(ranges)
    except Exception as exc:  # noqa: BLE001
        _error_queue.put(f"{ip}: {exc}")
        log.exception("worker for %s failed", ip)
    finally:
        os.close(fd)
        client.close()
    return total_downloaded


# ---------------------------------------------------------------------------
# Downloader orchestrator
# ---------------------------------------------------------------------------


class ParallelDownloader:
    """Orchestrates parallel byte-range downloads across resolved IPs.

    Uses one process per IP (via :class:`ProcessPoolExecutor` with fork) so
    each IP gets its own Python GIL. Object-level concurrency for prefix/bucket
    downloads uses threads (lightweight metadata + process spawning per object).
    """

    def __init__(
        self,
        client: S3Client,
        ips: list[str],
        *,
        chunks_override: int | None = None,
        min_chunk_mb: int = 2,
        object_concurrency: int | None = None,
        verify_tls: bool = True,
    ) -> None:
        self._client = client
        self._ips = ips
        self._chunks_override = chunks_override
        self._min_chunk_mb = min_chunk_mb
        self._object_concurrency = max(1, object_concurrency or len(ips))
        self._verify_tls = verify_tls
        # fork is fast and works from non-main threads (needed for prefix mode).
        self._mp_ctx = multiprocessing.get_context("fork")

    # -- Single object -----------------------------------------------------

    def download_object(
        self,
        bucket: str,
        key: str,
        output_path: Path,
        *,
        tracker: ProgressTracker | None = None,
    ) -> DownloadResult:
        """Download *bucket*/*key* to *output_path* using parallel processes.

        Args:
            tracker: optional shared tracker (used in multi-object mode to show
                aggregate progress). When ``None``, a fresh tracker is created
                for this object and a summary is printed at the end.
        """
        start = time.monotonic()
        # Resolve object size via HEAD (use the first IP, in the main process).
        meta = self._client.head_object(bucket, key, ip=self._ips[0])
        total = meta.size
        if total == 0:
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
        ranges_by_ip = group_ranges_by_ip(ranges)
        ip_list = list(ranges_by_ip.keys())
        ip_index_map = {ip: i for i, ip in enumerate(ip_list)}

        # Preallocate the output file.
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(output_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        os.ftruncate(fd, total)
        os.close(fd)  # workers will open their own fd

        own_tracker = tracker is None
        if own_tracker:
            tracker = ProgressTracker(total, ip_label=f"{bucket}/{key}")
            tracker.start()

        # Shared memory counters for per-IP stats (read by the polling thread).
        shared_bytes = self._mp_ctx.Array("q", len(ip_list))
        shared_chunks = self._mp_ctx.Array("i", len(ip_list))
        error_queue = self._mp_ctx.Queue()

        # Polling thread: reads shared counters and advances the tracker.
        # Uses a mutable container so the main thread can read the last value
        # after the thread exits (for a final delta flush).
        stop_event = threading.Event()
        _poll_last_total = [0]

        def _poll() -> None:
            while not stop_event.is_set():
                time.sleep(0.5)
                current = sum(shared_bytes[:])
                delta = current - _poll_last_total[0]
                if delta > 0 and tracker is not None:
                    tracker.advance(delta)
                _poll_last_total[0] = current

        poll_thread = threading.Thread(target=_poll, daemon=True)
        poll_thread.start()

        # Spawn one process per IP. The shared arrays and error queue are
        # passed via the pool initializer (fork inheritance), not as submit()
        # args — multiprocessing.Array can't be pickled as a function argument.
        creds = self._client._creds  # noqa: SLF001 - needed for pickling
        futures = []
        with ProcessPoolExecutor(
            max_workers=len(ip_list),
            mp_context=self._mp_ctx,
            initializer=_pool_initializer,
            initargs=(shared_bytes, shared_chunks, error_queue),
        ) as pool:
            for ip, ip_ranges in ranges_by_ip.items():
                fut = pool.submit(
                    _download_ranges_worker,
                    ip,
                    ip_ranges,
                    bucket,
                    key,
                    str(output_path),
                    self._client.endpoint,
                    creds,
                    self._verify_tls,
                    ip_index_map[ip],
                )
                futures.append(fut)

            for fut in as_completed(futures):
                fut.result()

        stop_event.set()
        poll_thread.join(timeout=2.0)

        # Final flush: advance the tracker by any remaining delta (the polling
        # thread reads every 0.5s and may have missed the last few bytes).
        if tracker is not None:
            final_obj_bytes = sum(shared_bytes[:])
            # _poll_last_total is set by the polling thread; compute the delta
            # between what the poller last saw and the final count.
            delta = final_obj_bytes - _poll_last_total[0]
            if delta > 0:
                tracker.advance(delta)
            _poll_last_total[0] = final_obj_bytes

        # Collect errors.
        errors: list[str] = []
        while not error_queue.empty():
            errors.append(error_queue.get())

        # Sync the file to disk.
        sync_fd = os.open(str(output_path), os.O_WRONLY)
        os.fsync(sync_fd)
        os.close(sync_fd)

        if own_tracker and tracker is not None:
            elapsed = tracker.stop()
            ip_bytes = {ip_list[i]: int(shared_bytes[i]) for i in range(len(ip_list))}
            ip_chunks = {ip_list[i]: int(shared_chunks[i]) for i in range(len(ip_list))}
            tracker.print_summary_from_shared(
                title=f"Summary: {bucket}/{key}",
                elapsed=elapsed,
                ip_bytes=ip_bytes,
                ip_chunks=ip_chunks,
            )
        elif tracker is not None:
            elapsed = time.monotonic() - start

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
    ) -> list[DownloadResult]:
        """Download all objects under *prefix* into *output_dir*.

        Each object's key is mapped to a relative path under *output_dir* by
        stripping *prefix*. Concurrent objects are downloaded via a thread pool
        capped at ``object_concurrency``; each object internally uses processes
        for per-IP range downloads.
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
                tracker.advance(0)
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
