"""
ipv4_only.py - Force AF_INET-only DNS resolution for the importing process.

Import this FIRST in every process entrypoint (main.py, download_worker.py,
tiktok/worker.py). Subprocess workers do not import main.py, so a patch
living only there never reaches the process that actually runs yt-dlp.

Everything stays IPv4 by default. Since 2026-09-25 the API container also
sits on the IPv6 network audioforges-net6; only the IPv6 download attempt
turns AAAA resolution on, via allow_ipv6().
"""
import socket

_allow_ipv6 = False


def allow_ipv6(on: bool) -> None:
    """Lets the one IPv6 download attempt (youtube._extract_direct) resolve
    AAAA records. Workers run one download per process, so a module flag
    is enough."""
    global _allow_ipv6
    _allow_ipv6 = bool(on)


if not getattr(socket.getaddrinfo, "_ipv4_only", False):
    _orig_getaddrinfo = socket.getaddrinfo

    def _getaddrinfo_ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
        if not _allow_ipv6 and family in (0, socket.AF_UNSPEC, socket.AF_INET6):
            family = socket.AF_INET
        return _orig_getaddrinfo(host, port, family, type, proto, flags)

    _getaddrinfo_ipv4_only._ipv4_only = True
    socket.getaddrinfo = _getaddrinfo_ipv4_only