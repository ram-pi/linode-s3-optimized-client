"""Parallel downloader using persistent multiprocessing.

Single-object: downloads via the main process using ``get_object`` (simple,
no range splitting — parallelism benefit is in prefix mode across many objects).

Prefix/bucket: spawns ``connections_per_ip`` persistent worker processes per
resolved IP. Each process is single-threaded with its own GIL and TCP
connection, downloading full objects sequentially. No range splitting, no
reassembly — just a simple GET → write → next object loop.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path

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
    success: bool
    error: str | None = None


# ---------------------------------------------------------------------------
# Persistent worker process (single-threaded, downloads full objects).
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
) -> None:
    """Worker process: downloads full objects from *task_queue* using *ip*.

    Single-threaded — no GIL contention with other connections on the same IP.
    Creates one ``S3Client`` at startup and reuses keep-alive connections.
    Downloads full objects (no Range header) and writes them to disk.

    Multiple processes for the same IP read from the same task queue — each
    gets a different object, so all connections are busy simultaneously.

    Runs until it receives ``None`` on the task queue (sentinel).
    """
    client = S3Client(endpoint, creds, [ip], verify_tls=verify_tls, pool_size_per_ip=1)
    try:
        while True:
            task = task_queue.get()
            if task is None:
                break
            bucket, key, output_path, obj_size = task
            try:
                output_path = Path(output_path)

                if obj_size == 0:
                    output_path.touch()
                    result_queue.put((key, True, None))
                    continue

                fd = os.open(str(output_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                try:
                    for buf in client.get_object(bucket, key, ip):
                        view = memoryview(buf)
                        written = 0
                        while written < len(buf):
                            n = os.write(fd, view[written:])
                            written += n
                        with shared_bytes.get_lock():
                            shared_bytes[ip_index] += len(buf)
                    with shared_chunks.get_lock():
                        shared_chunks[ip_index] += 1
                finally:
                    os.close(fd)
                result_queue.put((key, True, None))
            except Exception as exc:  # noqa: BLE001
                log.exception("worker %s: object %s failed", ip, key)
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

    Single-object: simple download via ``get_object`` (no range splitting).
    Prefix/bucket: ``connections_per_ip`` persistent processes per IP, each
    single-threaded with its own GIL and TCP connection.
    """

    def __init__(
        self,
        client: S3Client,
        ips: list[str],
        *,
        connections_per_ip: int = 4,
        verify_tls: bool = True,
    ) -> None:
        self._client = client
        self._ips = ips
        self._verify_tls = verify_tls
        self._connections_per_ip = max(1, connections_per_ip)
        self._mp_ctx = multiprocessing.get_context("fork")

    # -- Single object ----------------------------------------------------

    def download_object(
        self,
        bucket: str,
        key: str,
        output_path: Path,
        *,
        tracker: ProgressTracker | None = None,
    ) -> DownloadResult:
        """Download *bucket*/*key* to *output_path*.

        Simple full-object download using the first IP. For single objects the
        overhead of spawning processes isn't worth it — the parallelism benefit
        is in prefix mode across many objects.
        """
        start = time.monotonic()
        meta = self._client.head_object(bucket, key, ip=self._ips[0])
        total = meta.size

        output_path.parent.mkdir(parents=True, exist_ok=True)

        own_tracker = tracker is None
        if own_tracker:
            tracker = ProgressTracker(total, ip_label=f"{bucket}/{key}")
            tracker.start()

        if total == 0:
            output_path.touch()
            elapsed = time.monotonic() - start
            if own_tracker and tracker is not None:
                tracker.stop()
            return DownloadResult(key, output_path, 0, elapsed, True)

        ip = self._ips[0]
        fd = os.open(str(output_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        error: str | None = None
        try:
            for buf in self._client.get_object(bucket, key, ip):
                view = memoryview(buf)
                written = 0
                while written < len(buf):
                    n = os.write(fd, view[written:])
                    written += n
                if tracker is not None:
                    tracker.add_bytes(ip, len(buf))
            if tracker is not None:
                tracker.add_chunk(ip)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            log.exception("download of %s/%s failed", bucket, key)
        finally:
            os.close(fd)

        if own_tracker and tracker is not None:
            elapsed = tracker.stop()
            tracker.print_summary(title=f"Summary: {bucket}/{key}", elapsed=elapsed)
        else:
            elapsed = time.monotonic() - start

        return DownloadResult(
            key=key,
            output_path=output_path,
            size=total,
            elapsed=elapsed,
            success=error is None,
            error=error,
        )

    # -- Prefix / bucket --------------------------------------------------

    def download_prefix(
        self,
        bucket: str,
        prefix: str,
        output_dir: Path,
    ) -> list[DownloadResult]:
        """Download all objects under *prefix* into *output_dir*.

        Spawns ``connections_per_ip`` persistent processes per IP. Each process
        is single-threaded with its own GIL and TCP connection, downloading
        full objects sequentially. Multiple processes for the same IP read
        from the same task queue — each gets a different object.
        """
        results: list[DownloadResult] = []
        metas = list(self._client.list_objects(bucket, prefix=prefix))
        if not metas:
            log.warning("no objects found under prefix %r in bucket %r", prefix, bucket)
            return results

        # Pre-create all output directories in the main process.
        for meta in metas:
            rel = _relative_key(meta.key, prefix)
            out = output_dir / rel
            out.parent.mkdir(parents=True, exist_ok=True)

        total_bytes = sum(m.size for m in metas)
        n_ips = len(self._ips)
        cpi = self._connections_per_ip
        tracker = ProgressTracker(
            total_bytes, ip_label=f"{bucket}: {len(metas)} objects", show_live=True
        )
        tracker.start()

        # Shared memory counters — one slot per IP (shared by all processes for that IP).
        shared_bytes = self._mp_ctx.Array("q", n_ips)
        shared_chunks = self._mp_ctx.Array("i", n_ips)

        # One task queue per IP. Multiple consumer processes per IP read from
        # the same queue — multiprocessing.Queue is safe for multiple consumers.
        task_queues = [self._mp_ctx.Queue() for _ in range(n_ips)]
        result_queue = self._mp_ctx.Queue()
        creds = self._client._creds  # noqa: SLF001

        # Distribute objects round-robin across IPs.
        for i, meta in enumerate(metas):
            ip_idx = i % n_ips
            rel = _relative_key(meta.key, prefix)
            out = output_dir / rel
            task_queues[ip_idx].put((bucket, meta.key, str(out), meta.size))

        # Send connections_per_ip sentinels per IP (one per consumer process).
        for q in task_queues:
            for _ in range(cpi):
                q.put(None)

        # Spawn connections_per_ip processes per IP.
        workers = []
        for i, ip in enumerate(self._ips):
            for _ in range(cpi):
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
                _key, success, error = result_queue.get(timeout=120)
            except Exception:  # noqa: BLE001
                log.error("timeout waiting for worker results")
                break
            completed += 1
            if not success:
                log.error("object %s failed: %s", _key, error)

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
        tracker.print_summary(
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
