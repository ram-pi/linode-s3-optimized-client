"""Stats: live progress bar + final per-IP summary.

Uses ``rich`` for both the in-place progress display and the post-download
summary table.
"""

from __future__ import annotations

import sys
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table

if TYPE_CHECKING:
    from rich.console import Console as RichConsole


@dataclass(slots=True)
class IPStats:
    """Per-IP accumulators for the final summary."""

    bytes_downloaded: int = 0
    elapsed: float = 0.0
    chunks: int = 0
    start: float = field(default_factory=time.monotonic)

    @property
    def avg_speed(self) -> float:
        return self.bytes_downloaded / self.elapsed if self.elapsed > 0 else 0.0


class ProgressTracker:
    """Thread-safe accumulator for live progress + per-IP stats."""

    def __init__(
        self,
        total: int,
        *,
        ip_label: str = "Downloading",
        console: RichConsole | None = None,
        show_live: bool = True,
    ) -> None:
        self.total = total
        self._downloaded = 0
        self._lock = threading.Lock()
        self._per_ip: dict[str, IPStats] = {}
        self._start = time.monotonic()
        # Disable live bar when stdout is not a TTY (piped/redirected).
        if show_live and not sys.stdout.isatty():
            show_live = False
        self._console = console or Console()
        self._show_live = show_live
        self._label = ip_label

        if show_live:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TextColumn("[cyan]ETA"),
                TimeRemainingColumn(),
                TextColumn("[dim]elapsed"),
                TimeElapsedColumn(),
                console=self._console,
                transient=False,
            )
            self._task = self._progress.add_task(ip_label, total=total)
        else:
            self._progress = None  # type: ignore[assignment]
            self._task = 0
        self._last_print = 0.0
        self._last_printed_bytes = 0

    def add_bytes(self, ip: str, n: int) -> None:
        """Record *n* bytes downloaded from *ip* (single-object mode)."""
        with self._lock:
            self._downloaded += n
            stats = self._per_ip.setdefault(ip, IPStats(start=time.monotonic()))
            stats.bytes_downloaded += n
        if self._show_live and self._progress is not None:
            self._progress.update(self._task, advance=n)
        else:
            self._maybe_print_progress()

    def add_chunk(self, ip: str) -> None:
        """Mark that a chunk completed on *ip*."""
        with self._lock:
            stats = self._per_ip.setdefault(ip, IPStats(start=time.monotonic()))
            stats.chunks += 1

    def advance(self, n: int) -> None:
        """Advance the total counter by *n* bytes (multiprocessing polling)."""
        with self._lock:
            self._downloaded += n
        if self._show_live and self._progress is not None:
            self._progress.update(self._task, advance=n)
        else:
            self._maybe_print_progress()

    def _maybe_print_progress(self) -> None:
        """Print a progress line every ~2s when no live bar is active."""
        now = time.monotonic()
        if now - self._last_print < 2.0:
            return
        with self._lock:
            downloaded = self._downloaded
            delta = downloaded - self._last_printed_bytes
        elapsed = now - self._start
        speed = delta / (now - self._last_print) if now > self._last_print else 0
        self._last_print = now
        self._last_printed_bytes = downloaded
        pct = (downloaded / self.total * 100) if self.total > 0 else 0
        self._console.print(
            f"{self._label}: {pct:5.1f}% | {_human_bytes(downloaded)}/{_human_bytes(self.total)} "
            f"| {_human_speed(speed)} | {elapsed:.0f}s elapsed",
            markup=False,
        )

    def start(self) -> None:
        if self._show_live and self._progress is not None:
            self._progress.start()

    def stop(self) -> float:
        """Stop the live display and return elapsed seconds."""
        elapsed = time.monotonic() - self._start
        if self._show_live and self._progress is not None:
            self._progress.stop()
        with self._lock:
            for stats in self._per_ip.values():
                stats.elapsed = time.monotonic() - stats.start
        return elapsed

    def print_summary(
        self,
        *,
        title: str = "Download summary",
        elapsed: float | None = None,
        object_label: str | None = None,
        ip_bytes: dict[str, int] | None = None,
        ip_chunks: dict[str, int] | None = None,
    ) -> None:
        """Print the final summary table.

        Args:
            ip_bytes: per-IP byte counts (from shared memory in multiprocessing
                mode). When ``None``, uses the tracker's internal per-IP stats.
            ip_chunks: per-IP chunk counts (same as above).
        """
        elapsed = elapsed if elapsed is not None else (time.monotonic() - self._start)
        with self._lock:
            total_bytes = self._downloaded

        # Build per-IP stats from either shared memory or internal tracking.
        per_ip: dict[str, IPStats] = {}
        if ip_bytes:
            for ip, n in ip_bytes.items():
                per_ip[ip] = IPStats(
                    bytes_downloaded=n,
                    chunks=(ip_chunks or {}).get(ip, 0),
                    elapsed=elapsed,
                )
        else:
            with self._lock:
                per_ip = dict(self._per_ip)

        table = Table(title=title, show_header=True, header_style="bold green")
        table.add_column("IP", style="cyan", no_wrap=True)
        table.add_column("Chunks", justify="right")
        table.add_column("Bytes", justify="right")
        table.add_column("Avg speed", justify="right")

        for ip, stats in sorted(per_ip.items()):
            table.add_row(
                ip,
                str(stats.chunks),
                _human_bytes(stats.bytes_downloaded),
                _human_speed(stats.avg_speed),
            )

        self._console.print()
        if object_label:
            self._console.print(f"[bold]{object_label}[/bold]")
        self._console.print(table)

        agg = Table(show_header=False, box=None, padding=(0, 0))
        agg.add_column("k", style="bold")
        agg.add_column("v")
        agg.add_row("Total bytes", _human_bytes(total_bytes))
        agg.add_row("Elapsed", f"{elapsed:.2f}s")
        agg.add_row("Avg speed", _human_speed(total_bytes / elapsed if elapsed > 0 else 0))
        agg.add_row("IPs used", str(len(per_ip)))
        self._console.print(agg)
        self._console.print()


def _human_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n = int(n) // 1024  # type: ignore[assignment]
    return f"{n} PiB"


def _human_speed(bps: float) -> str:
    return f"{_human_bytes(int(bps))}/s"
