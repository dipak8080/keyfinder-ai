"""
ipv4_only.py - Force AF_INET-only DNS resolution for the importing process.

Import this FIRST in every process entrypoint (main.py, download_worker.py,
tiktok/worker.py). Subprocess workers do not import main.py, so a patch
living only there never reaches the process that actually runs yt-dlp.

The container has no IPv6 route; unrestricted getaddrinfo on dual-stack
googlevideo edges raises "Address family for hostname not supported".
Remove once Docker IPv6 networking is properly configured.
"""
import socket

if not getattr(socket.getaddrinfo, "_ipv4_only", False):
    _orig_getaddrinfo = socket.getaddrinfo

    def _getaddrinfo_ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
        if family in (0, socket.AF_UNSPEC, socket.AF_INET6):
            family = socket.AF_INET
        return _orig_getaddrinfo(host, port, family, type, proto, flags)

    _getaddrinfo_ipv4_only._ipv4_only = True
    socket.getaddrinfo = _getaddrinfo_ipv4_only