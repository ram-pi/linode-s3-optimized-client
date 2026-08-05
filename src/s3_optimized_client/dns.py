"""DNS resolution: endpoint -> list of public IPs via socket.getaddrinfo.

Resolves a hostname to all unique A/AAAA records. Deduplicates and preserves
the order returned by the resolver (which typically reflects the OS's view).
"""

from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ResolvedIP:
    """A single resolved IP address with its family."""

    ip: str
    family: int  # socket.AF_INET | socket.AF_INET6

    @property
    def is_ipv6(self) -> bool:
        return self.family == socket.AF_INET6

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.ip


def resolve_endpoint(endpoint: str, port: int = 443) -> list[ResolvedIP]:
    """Resolve *endpoint* to a list of unique IPs.

    Uses :func:`socket.getaddrinfo` to obtain all A/AAAA records. Filters to
    IPv4/IPv6 and deduplicates while preserving order.

    Args:
        endpoint: hostname (without scheme, without bucket prefix), e.g.
            ``gb-lon-1.linodeobjects.com``.
        port: TCP port to resolve for (default 443).

    Returns:
        List of :class:`ResolvedIP` (unique, ordered). IPv6 addresses are
        returned after IPv4 when both families are available.

    Raises:
        :class:`socket.gaierror`: if resolution fails.
        :class:`RuntimeError`: if no usable A/AAAA records are returned.
    """
    # Normalize: strip any accidental scheme / trailing slash.
    host = endpoint
    if "://" in host:
        host = host.split("://", 1)[1]
    host = host.split("/", 1)[0].split(":", 1)[0]

    try:
        results = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        # Retry without AI_ADDRCONFIG quirks by asking for both families.
        results = socket.getaddrinfo(host, port, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM)

    seen: set[str] = set()
    ipv4: list[ResolvedIP] = []
    ipv6: list[ResolvedIP] = []
    for family, _stype, _proto, _canon, sockaddr in results:
        if family not in (socket.AF_INET, socket.AF_INET6):
            continue
        ip = sockaddr[0]
        try:
            ipaddress.ip_address(ip)
        except ValueError:
            continue
        if ip in seen:
            continue
        seen.add(ip)
        resolved = ResolvedIP(ip=ip, family=family)
        (ipv6 if family == socket.AF_INET6 else ipv4).append(resolved)

    all_ips = ipv4 + ipv6
    if not all_ips:
        msg = f"No A/AAAA records returned for {host!r}"
        raise RuntimeError(msg)

    return all_ips


def resolve_endpoint_ipv4_only(endpoint: str, port: int = 443) -> list[str]:
    """Convenience: resolve to IPv4 string addresses only.

    Returns an empty list if no IPv4 records exist (caller decides whether to
    fall back to IPv6).
    """
    return [r.ip for r in resolve_endpoint(endpoint, port) if not r.is_ipv6]
