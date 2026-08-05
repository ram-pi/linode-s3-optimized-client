"""Parallel byte-range downloader using persistent multiprocessing.

For single-object downloads: uses threads (process overhead isn't worth it
for one object).

For prefix/bucket downloads: spawns **persistent worker processes** — one per
resolved IP — at startup. Each worker creates a single ``S3Client`` pinned to
its IP and reuses keep-alive connections across all objects. Objects are
distributed round-robin so all IPs stay saturated without process churn.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from .chunk_pool import Range, plan_chunks
from .s3 import S3Client, S3Credentials
from .stats import ProgressTracker

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
# Persistent worker process (one per IP, lives for the entire prefix download).
# ---------------------------------------------------------------------------


def _persistent_worker(
    ip: str,
    task_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
    shared_bytes: multiprocessing.Array,
    shared_chunks: multiprocessing.Array,
    ip_index: int,
    endpoint: str,
    creds: S3Credentials,
    verify_tls: bool,
    chunks_override: int | None,
    min_chunk_mb: int,
) -> None:
    """Worker process: downloads objects from *task_queue* using *ip* only.

    Creates one ``S3Client`` at startup and reuses it (with keep-alive
    connections) for all objects. Each object is split into byte ranges and
    downloaded sequentially via the keep-alive pool. Writes to output files
    via ``os.pwrite``.

    Runs until it receives ``None`` on the task queue (sentinel).
    """
    client = S3Client(endpoint, creds, [ip], verify_tls=verify_tls)
    all_ips = [ip]  # this worker only uses its own IP
    try:
        while True:
            task = task_queue.get()
            if task is None:
                break
            bucket, key, output_path, obj_size = task
            try:
                output_path = Path(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)

                if obj_size == 0:
                    output_path.touch()
                    result_queue.put((key, True, None))
                    continue

                # Plan ranges across this single IP (sequential download).
                ranges = plan_chunks(
                    obj_size,
                    all_ips,
                    chunks_override=chunks_override,
                    min_chunk_mb=min_chunk_mb,
                )

                fd = os.open(str(output_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                try:
                    os.ftruncate(fd, obj_size)
                    for rng in ranges:
                        offset = rng.start
                        for buf in client.range_get(bucket, key, rng.start, rng.end, ip):
                            view = memoryview(buf)
                            written = 0
                            while written < len(buf):
                                n = os.pwrite(fd, view[written:], offset + written)
                                written += n
                            offset += len(buf)
                            with shared_bytes.get_lock():
                                shared_bytes[ip_index] += len(buf)
                    with shared_chunks.get_lock():
                        shared_chunks[ip_index] += len(ranges)
                    os.fsync(fd)
                finally:
                    os.close(fd)
                result_queue.put((key, True, None))
            except Exception as exc:  # noqa: BLE001
                log.exception("worker %s: object %s failed", ip, key)
                # Account for the bytes we didn't download so progress is consistent.
                with shared_bytes.get_lock():
                    shared_bytes[ip_index] += obj_size
                result_queue.put((key, False, str(exc)))
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Downloader orchestrator
# ---------------------------------------------------------------------------


class ParallelDownloader:
    """Orchestrates parallel downloads across resolved IPs.

    Single-object: threads (low overhead, GIL impact is small for one object).
    Prefix/bucket: persistent processes (one per IP, no process churn).
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
        self._mp_ctx = multiprocessing.get_context("fork")

    # -- Single object (threads) ------------------------------------------

    def download_object(
        self,
        bucket: str,
        key: str,
        output_path: Path,
        *,
        tracker: ProgressTracker | None = None,
    ) -> DownloadResult:
        """Download *bucket*/*key* to *output_path* using parallel threads.

        Uses threads (not processes) because for a single object the GIL
        impact is small and process spawning overhead isn't worth it.
        """
        start = time.monotonic()
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
                except Exception as exc:  # noqa: BLE001
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
                tracker.print_summary(title=f"Summary: {bucket}/{key}", elapsed=elapsed)
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

    # -- Prefix / bucket (persistent processes) ----------------------------

    def download_prefix(
        self,
        bucket: str,
        prefix: str,
        output_dir: Path,
    ) -> list[DownloadResult]:
        """Download all objects under *prefix* into *output_dir*.

        Spawns one persistent process per IP. Objects are distributed
        round-robin to the workers via queues. Each worker reuses its
        ``S3Client`` (keep-alive connections) across all objects — no process
        churn, no repeated TLS handshakes.
        """
        results: list[DownloadResult] = []
        metas = list(self._client.list_objects(bucket, prefix=prefix))
        if not metas:
            log.warning("no objects found under prefix %r in bucket %r", prefix, bucket)
            return results

        total_bytes = sum(m.size for m in metas)
        n_ips = len(self._ips)
        tracker = ProgressTracker(
            total_bytes, ip_label=f"{bucket}: {len(metas)} objects", show_live=True
        )
        tracker.start()

        # Shared memory counters.
        shared_bytes = self._mp_ctx.Array("q", n_ips)
        shared_chunks = self._mp_ctx.Array("i", n_ips)

        # One task queue per IP (round-robin distribution), one shared result queue.
        task_queues = [self._mp_ctx.Queue() for _ in range(n_ips)]
        result_queue = self._mp_ctx.Queue()
        creds = self._client._creds  # noqa: SLF001

        # Distribute objects round-robin across IPs.
        for i, meta in enumerate(metas):
            ip_idx = i % n_ips
            rel = _relative_key(meta.key, prefix)
            out = output_dir / rel
            task_queues[ip_idx].put((bucket, meta.key, str(out), meta.size))

        # Send a sentinel (None) to each worker to signal completion.
        for q in task_queues:
            q.put(None)

        # Spawn persistent worker processes.
        workers = []
        for i, ip in enumerate(self._ips):
            p = self._mp_ctx.Process(
                target=_persistent_worker,
                args=(
                    ip,
                    task_queues[i],
                    result_queue,
                    shared_bytes,
                    shared_chunks,
                    i,
                    self._client.endpoint,
                    creds,
                    self._verify_tls,
                    self._chunks_override,
                    self._min_chunk_mb,
                ),
            )
            workers.append(p)
            p.start()

        # Polling thread: read shared counters and update the progress tracker.
        stop_event = threading.Event()
        _poll_last = [0]

        def _poll() -> None:
            while not stop_event.is_set():
                time.sleep(0.5)
                current = sum(shared_bytes[:])
                delta = current - _poll_last[0]
                if delta > 0:
                    tracker.advance(delta)
                _poll_last[0] = current

        poll_thread = threading.Thread(target=_poll, daemon=True)
        poll_thread.start()

        # Collect results as they complete.
        completed = 0
        total_objects = len(metas)
        while completed < total_objects:
            try:
                key, success, error = result_queue.get(timeout=120)
            except Exception:  # noqa: BLE001
                log.error("timeout waiting for worker results")
                break
            completed += 1
            if not success:
                log.error("object %s failed: %s", key, error)

        # Wait for all workers to finish.
        for p in workers:
            p.join(timeout=30)

        stop_event.set()
        poll_thread.join(timeout=2.0)

        # Final delta flush.
        final_bytes = sum(shared_bytes[:])
        delta = final_bytes - _poll_last[0]
        if delta > 0:
            tracker.advance(delta)

        elapsed = tracker.stop()
        ip_bytes = {self._ips[i]: int(shared_bytes[i]) for i in range(n_ips)}
        ip_chunks = {self._ips[i]: int(shared_chunks[i]) for i in range(n_ips)}
        tracker.print_summary_from_shared(
            title=f"Summary: {bucket}/{prefix or '(root)'}",
            object_label=f"{completed}/{total_objects} objects, {_human_bytes(final_bytes)}",
            elapsed=elapsed,
            ip_bytes=ip_bytes,
            ip_chunks=ip_chunks,
        )

        return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _relative_key(key: str, prefix: str) -> str:
    """Map an object key to a filesystem path relative to *prefix*."""
    rel = key
    if prefix and rel.startswith(prefix):
        rel = rel[len(prefix) :]
    rel = rel.lstrip("/")
    parts = [p for p in rel.split("/") if p not in ("", ".", "..")]
    return "/".join(parts)


def _human_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n = int(n) // 1024  # type: ignore[assignment]
    return f"{n} PiB"
