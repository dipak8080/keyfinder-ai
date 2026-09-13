"""
client_ip.py - One place that decides who the caller is.

WHY THIS EXISTS (2026-09-13): admin_auth.py, rate_limit.py and
log_stream.py each read X-Forwarded-For and took its FIRST entry, with no
validation. Cloudflare and nginx's $proxy_add_x_forwarded_for both APPEND
to that header rather than replacing it, so a value the client sends
arrives first and wins. Demonstrated against production: a request
carrying `X-Forwarded-For: 203.0.113.99` (TEST-NET-3, an address that can
never be a real client) was recorded in request_logs as exactly that.

That is not a logging cosmetic. Both admin_auth layers - the request cap
and the wrong-key lockout that exists because a pentest brute-forced
/admin/upload-cookies - key on this value, so rotating the header per
request gives an attacker a fresh bucket every time and unlimited key
guesses, with the lockout alert never firing. rate_limit.py keys free-tier
buckets on it too, which makes the persistent limiter's allowance
sidesteppable no matter how correct its transaction is.

CF-Connecting-IP is the fix because Cloudflare OVERWRITES it rather than
appending, so a client-supplied value never survives the edge, and the
origin is ufw-locked to Cloudflare IPs so the edge cannot be skipped.
credits/identity.py already did exactly this behind TRUST_CF_CONNECTING_IP
(default true); this module is that logic, shared, for the three that
never got it.

The X-Forwarded-For fallback is DELIBERATELY KEPT and is still forgeable.
It only applies when CF-Connecting-IP is absent, which in this deployment
means a request that did not come through Cloudflare at all: the canary,
the local health probes, a curl on 127.0.0.1:8000. Dropping it would
collapse every such caller into one bucket. The guarantee this module
makes is about public traffic, which is where the attack lives.

No dependency but the Request object, so any module can import it -
including the three this one protects.
"""
import os


def _trust_cf() -> bool:
    raw = os.environ.get("TRUST_CF_CONNECTING_IP", "true").strip().lower()
    return raw not in ("0", "false", "no", "off")


_TRUST_CF = _trust_cf()


def get_client_ip(request, default: str = "unknown") -> str:
    if _TRUST_CF:
        cf = request.headers.get("cf-connecting-ip")
        if cf:
            return cf.strip()

    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        first = forwarded.split(",")[0].strip()
        if first:
            return first

    return request.client.host if request.client else default