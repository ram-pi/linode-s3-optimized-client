"""Byte-range chunk planning.

Computes how to split an object into byte ranges and distributes them across
the resolved IPs (round-robin).
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MIN_CHUNK_MB = 4
MAX_CHUNKS_PER_IP = 64  # cap to avoid excessive overhead on huge files


@dataclass(frozen=True, slots=True)
class Range:
    """Inclusive byte range [start, end] assigned to a specific IP."""

    index: int
    start: int
    end: int
    ip: str

    @property
    def size(self) -> int:
        return self.end - self.start + 1


def plan_chunks(
    total_size: int,
    ips: list[str],
    *,
    chunks_override: int | None = None,
    min_chunk_mb: int = DEFAULT_MIN_CHUNK_MB,
    target_chunks: int | None = None,
) -> list[Range]:
    """Plan byte ranges to download *total_size* bytes across *ips*.

    Args:
        total_size: object size in bytes.
        ips: resolved IP addresses (>=1).
        chunks_override: if set, use exactly this many chunks (clamped to
            ``[1, total_size]``); otherwise auto-compute.
        min_chunk_mb: minimum chunk size (MiB) for auto-computing chunk count.
        target_chunks: if set, aim for exactly this many chunks so all
            download threads are used in a single round (no sequential rounds).
            The actual count is clamped by ``min_chunk_mb`` (won't make chunks
            smaller than the minimum) and by ``total_size``. When the object is
            large enough, this produces exactly ``target_chunks`` ranges.

    Returns:
        Ordered list of :class:`Range` with ``ip`` assigned round-robin.

    Raises:
        :class:`ValueError`: if ``total_size`` is <= 0 or no IPs are provided.
    """
    if total_size <= 0:
        msg = f"total_size must be positive, got {total_size}"
        raise ValueError(msg)
    if not ips:
        msg = "at least one IP is required"
        raise ValueError(msg)

    min_chunk_bytes = max(1, min_chunk_mb) * 1024 * 1024

    if chunks_override is not None:
        n = max(1, min(chunks_override, total_size))
    elif target_chunks is not None:
        # Aim for exactly target_chunks ranges so all threads download in a
        # single round. Shrink if chunks would be smaller than min_chunk.
        n = max(1, target_chunks)
        while n > 1 and _ceil_div(total_size, n) < min_chunk_bytes:
            n -= 1
        n = max(1, min(n, total_size))
    else:
        # Auto: at least one chunk per IP, but cap each chunk at >= min_chunk.
        # Start with len(ips) chunks; if any chunk < min_chunk, reduce count.
        n = len(ips)
        # Shrink until each chunk >= min_chunk (but never below 1).
        while n > 1 and _ceil_div(total_size, n) < min_chunk_bytes:
            n -= 1
        # Grow up to MAX_CHUNKS_PER_IP * len(ips) if the object is large
        # enough that each chunk still exceeds min_chunk (more parallelism).
        cap = MAX_CHUNKS_PER_IP * len(ips)
        while n < cap and _ceil_div(total_size, n) >= min_chunk_bytes * 2:
            n *= 2
        n = max(1, min(n, total_size))

    base = total_size // n
    remainder = total_size % n
    ranges: list[Range] = []
    offset = 0
    for i in range(n):
        size = base + (1 if i < remainder else 0)
        start = offset
        end = offset + size - 1
        ranges.append(Range(index=i, start=start, end=end, ip=ips[i % len(ips)]))
        offset = end + 1
    assert offset == total_size  # noqa: S101 - invariant check
    return ranges


def _ceil_div(a: int, b: int) -> int:
    return -(-a // b)


def group_ranges_by_ip(ranges: list[Range]) -> dict[str, list[Range]]:
    """Group a list of :class:`Range` objects into a dict keyed by IP.

    Each entry contains the ranges assigned to that IP, in order. Used to
    distribute work to one process per IP.
    """
    groups: dict[str, list[Range]] = {}
    for rng in ranges:
        groups.setdefault(rng.ip, []).append(rng)
    return groups
