"""
cookie_upload.py - Upload cookies.txt files directly (no more base64 env vars).

Replaces the YT_COOKIES_B64 / YT_COOKIES_B64_2 / YT_COOKIES_B64_3 approach.
Cookies are now stored as plain files on a persistent Docker volume at /app/data/,
uploaded via a simple web form - no SSH, no nano, no base64 encoding needed.

Mount points (added to main.py):
  GET  /admin/upload-cookies              -> simple HTML upload form
  POST /admin/upload-cookies              -> handles the actual file upload
  GET  /admin/cookies/status              -> JSON status of all 3 cookie slots

Requires ?key=<ADMIN_STATUS_KEY> same as your other admin endpoints.

--------------------------------------------------------------------------
UPDATED 2026-08-04: rate limit + brute-force lockout added.

This is the exact endpoint a Strix pentest run targeted with an automated
key-bruteforce agent - thousands of guesses against the `key` query param
fired in quick succession, with nothing previously slowing it down. See
admin_auth.py for the full reasoning; short version: admin routes need a
much stricter, separate guard from the public tool rate limits, since
only the site owner should ever be calling this endpoint at all.

_check_admin() now takes `request` in addition to `key` so it can resolve
the caller's real IP (via X-Forwarded-For, same as log_stream.py) and
check it against the shared rate-limit/lockout state in admin_auth.py
before ever comparing the key itself. Every route below passes its
`request` parameter through - nothing else about them changed.
--------------------------------------------------------------------------

UPDATED 2026-08-15: EXPIRY PARSING - "exists" was never the question.

/admin/cookies/status reported exists / size_bytes / last_modified. All
three can look perfectly healthy on a file whose session died months
ago, and that is exactly what happened: cookies_3.txt sat on disk at
full size, /admin/status counted accounts_available=3, and its SID had
already expired ~9 hours earlier. Two of three slots were live; the
dashboard said three.

WHY THE DISCORD ALERT DID NOT CATCH IT - this is the important part, and
it is structural, not a bug:

  _maybe_alert_cookie_expiry() in youtube.py is REACTIVE. It fires only
  when yt-dlp itself emits a "cookies are no longer valid" warning while
  running a download. That requires the account to actually be USED.

  Slots 2 and 3 are standby failover, not rotation - download_with_fallback
  only advances to the next account on account-identity errors
  (age-gate, members-only, format-unavailable). On a bot-check or
  IP-block it stops rotating and escalates to proxy, because a different
  cookie cannot fix a bad IP. Since bot-check/403 is the dominant
  failure shape here, slots 2 and 3 are almost never reached.

  Never reached -> never used -> yt-dlp never warns -> no alert. A dead
  standby account is INVISIBLE to reactive alerting by construction. It
  can only ever tell you an account died after you needed it.

Reading the expiry timestamp off the file is the only way to know
BEFORE the emergency. That is what _parse_cookie_expiry() below does.
It is proactive and needs no download to have happened.

WHY ONLY *CRITICAL* COOKIES ARE CONSIDERED: a cookies.txt export
contains dozens of entries, most of them short-lived consent/preference
junk (CONSENT, VISITOR_INFO1_LIVE, YSC, PREF) that expire constantly on
a perfectly healthy session. Taking the minimum expiry across ALL of
them would report a live account as expired within days of every export,
train you to ignore the warning, and be worse than no signal at all.
Only the authentication cookies below actually determine whether the
session still works.
--------------------------------------------------------------------------
"""

import os
import time
from fastapi import APIRouter, HTTPException, Query, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse

from config import (
    YT_COOKIES_PATH_DEFAULT,
    COOKIE_ACCOUNT_2_PATH,
    COOKIE_ACCOUNT_3_PATH,
)
from admin_auth import guard_admin_request, verify_admin_key

ADMIN_KEY = os.environ.get("ADMIN_STATUS_KEY", "")

# Use the EXACT same paths youtube.py's rotation logic reads from -
# imported from config.py rather than reconstructed here, so there's no
# risk of a path mismatch between where this uploads to and where
# get_cookie_accounts() looks.
_SLOT_PATHS = {
    1: os.environ.get("YT_COOKIES_PATH", YT_COOKIES_PATH_DEFAULT),
    2: COOKIE_ACCOUNT_2_PATH,
    3: COOKIE_ACCOUNT_3_PATH,
}

for _path in _SLOT_PATHS.values():
    os.makedirs(os.path.dirname(_path), exist_ok=True)

router = APIRouter()


# The cookies that actually carry the Google session. If these are alive
# the account authenticates; if any is dead it does not, regardless of
# how many other entries the file still has. __Secure-1PSID/3PSID are
# included because YouTube increasingly relies on them over bare SID.
CRITICAL_COOKIE_NAMES = {
    "SID",
    "HSID",
    "SSID",
    "APISID",
    "SAPISID",
    "__Secure-1PSID",
    "__Secure-3PSID",
    "__Secure-1PAPISID",
    "__Secure-3PAPISID",
    "LOGIN_INFO",
}

# Lead time before expiry at which a slot stops being "ok". Two weeks is
# enough notice to re-export without rushing; three days means do it now.
EXPIRY_WARNING_DAYS = 14
EXPIRY_CRITICAL_DAYS = 3


def _parse_cookie_expiry(path: str) -> dict:
    """
    Reads a Netscape-format cookies.txt and returns the earliest expiry
    among the CRITICAL auth cookies - the moment the session stops
    working, not the moment the first trivial preference cookie lapses.

    Netscape format is tab-separated:
        domain  includeSubdomains  path  secure  expiry  name  value

    Handles the two real-world quirks that break naive parsers:
      - yt-dlp and several browser extensions prefix HttpOnly entries
        with "#HttpOnly_", which looks like a comment line and gets
        skipped by anything that filters on a leading "#". Those are
        often the SID/HSID entries, i.e. exactly the ones that matter.
      - expiry 0 means a session cookie (dies with the browser). Those
        carry no useful deadline and are excluded rather than treated as
        "expired in 1970", which would report every file as long dead.

    Never raises. A malformed or unreadable cookie file must degrade to
    "unknown" in a status panel, never take down the status endpoint.
    """
    result = {
        "expires_at": None,
        "expires_in_days": None,
        "expiry_status": "unknown",
        "critical_cookies_found": 0,
    }

    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        return result

    now = time.time()
    earliest = None
    found = 0

    for line in lines:
        line = line.rstrip("\n")
        if not line.strip():
            continue

        # "#HttpOnly_.youtube.com" is DATA, not a comment. Strip the
        # marker and parse it; skip everything else starting with "#".
        if line.startswith("#HttpOnly_"):
            line = line[len("#HttpOnly_"):]
        elif line.startswith("#"):
            continue

        parts = line.split("\t")
        if len(parts) < 7:
            continue

        name = parts[5].strip()
        if name not in CRITICAL_COOKIE_NAMES:
            continue

        try:
            expiry = int(float(parts[4]))
        except (ValueError, IndexError):
            continue

        found += 1
        if expiry <= 0:
            continue  # session cookie - no deadline to report
        if earliest is None or expiry < earliest:
            earliest = expiry

    result["critical_cookies_found"] = found

    if found == 0:
        # File exists and parsed, but carries no auth cookies at all -
        # usually a logged-out export. Distinct from "unknown" (couldn't
        # read it) and far more actionable.
        result["expiry_status"] = "no_auth_cookies"
        return result

    if earliest is None:
        # Auth cookies present but all session-scoped. Can't date it.
        result["expiry_status"] = "session_only"
        return result

    seconds_left = earliest - now
    days_left = seconds_left / 86400

    result["expires_at"] = earliest
    result["expires_in_days"] = round(days_left, 1)
    if seconds_left <= 0:
        result["expiry_status"] = "expired"
    elif days_left <= EXPIRY_CRITICAL_DAYS:
        result["expiry_status"] = "critical"
    elif days_left <= EXPIRY_WARNING_DAYS:
        result["expiry_status"] = "warning"
    else:
        result["expiry_status"] = "ok"

    return result


def _check_admin(request: Request, key: str):
    """
    Rate-limited + lockout-protected admin check - replaces the previous
    bare equality check (`if not ADMIN_KEY or key != ADMIN_KEY: raise
    401`). See admin_auth.py for why a plain comparison alone isn't
    enough: this exact endpoint is what a Strix pentest run targeted with
    an automated brute-force agent.

    Kept as 401 on failure (not 403 like admin_auth's own default) to
    preserve this file's existing status-code contract - only the
    PROTECTION got stricter, the response shape for "wrong key" is
    unchanged for anything already calling this endpoint.
    """
    if not ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    client_ip = guard_admin_request(request)
    try:
        verify_admin_key(key, client_ip)
    except HTTPException as e:
        if e.status_code == 403:
            raise HTTPException(status_code=401, detail="Unauthorized")
        raise


def _cookie_path(slot: int) -> str:
    return _SLOT_PATHS[slot]


@router.post("/admin/upload-cookies")
async def upload_cookies(
    request: Request,
    key: str = Query(...),
    slot: int = Query(..., ge=1, le=3, description="Which cookie slot: 1, 2, or 3"),
    file: UploadFile = File(...),
):
    _check_admin(request, key)
    path = _cookie_path(slot)
    content = await file.read()

    if not content or b"youtube.com" not in content:
        raise HTTPException(
            status_code=400,
            detail="This doesn't look like a valid YouTube cookies.txt file. Upload rejected.",
        )

    with open(path, "wb") as f:
        f.write(content)

    # Parsed immediately and returned with the upload response, so a
    # logged-out or already-expired export is caught at upload time
    # rather than discovered weeks later during an outage. The upload
    # still SUCCEEDS either way - this is information, not a gate. A
    # hard reject would be wrong: expiry parsing is a heuristic over a
    # loosely-specified file format, and refusing a working file because
    # the parser didn't recognise it would be worse than accepting a bad
    # one with a visible warning.
    expiry = _parse_cookie_expiry(path)

    return JSONResponse({
        "status": "ok",
        "slot": slot,
        "path": path,
        "bytes_written": len(content),
        **expiry,
    })


@router.get("/admin/cookies/status")
def cookies_status(request: Request, key: str = Query(...)):
    _check_admin(request, key)
    result = {}
    for slot in (1, 2, 3):
        path = _cookie_path(slot)
        if os.path.exists(path):
            stat = os.stat(path)
            result[f"slot_{slot}"] = {
                "exists": True,
                "path": path,
                "size_bytes": stat.st_size,
                "last_modified": stat.st_mtime,
                # See this module's docstring: exists/size/mtime all look
                # healthy on a file whose session died months ago. This
                # is the field that actually answers "will this work?".
                **_parse_cookie_expiry(path),
            }
        else:
            result[f"slot_{slot}"] = {
                "exists": False,
                "path": path,
                "expiry_status": "missing",
                "expires_at": None,
                "expires_in_days": None,
                "critical_cookies_found": 0,
            }
    return JSONResponse(result)


@router.get("/admin/upload-cookies", response_class=HTMLResponse)
def upload_form(request: Request, key: str = Query(...)):
    _check_admin(request, key)
    html = """
<!DOCTYPE html>
<html>
<head>
<title>AudioForges - Upload Cookies</title>
<meta charset="utf-8">
<style>
  body { font-family: -apple-system, system-ui, sans-serif; background: #0f1117; color: #e2e2e2; margin: 0; padding: 30px; max-width: 600px; }
  h1 { font-size: 20px; }
  .card { background: #1a1d29; padding: 20px; border-radius: 10px; margin-bottom: 16px; }
  label { display: block; margin-bottom: 8px; font-size: 13px; color: #888; }
  select, input[type=file] { width: 100%; background: #262a38; color: #e2e2e2; border: 1px solid #333748; border-radius: 6px; padding: 8px; margin-bottom: 14px; box-sizing: border-box; }
  button { background: #4ade80; color: #0f1117; border: none; padding: 10px 20px; border-radius: 6px; cursor: pointer; font-weight: 600; }
  button:hover { background: #3fd873; }
  .status { font-size: 13px; margin-top: 10px; }
  .status-table { width: 100%; font-size: 13px; margin-top: 10px; border-collapse: collapse; }
  .status-table td, .status-table th { padding: 6px 8px; border-bottom: 1px solid #262a38; text-align: left; }
  .ok { color: #4ade80; }
  .warn { color: #fbbf24; }
  .missing { color: #f87171; }
</style>
</head>
<body>
  <h1>Upload YouTube Cookies</h1>

  <div class="card">
    <form id="uploadForm">
      <label>Cookie slot</label>
      <select id="slot" name="slot">
        <option value="1">Slot 1 (cookies.txt)</option>
        <option value="2">Slot 2 (cookies_2.txt)</option>
        <option value="3">Slot 3 (cookies_3.txt)</option>
      </select>

      <label>cookies.txt file (exported from browser extension)</label>
      <input type="file" id="cookieFile" name="file" accept=".txt" required>

      <button type="submit">Upload</button>
    </form>
    <div class="status" id="result"></div>
  </div>

  <div class="card">
    <strong>Current cookie status:</strong>
    <table class="status-table" id="statusTable"><tbody></tbody></table>
  </div>

<script>
const KEY = encodeURIComponent("PLACEHOLDER_ADMIN_KEY");

function expiryCell(info) {
  if (!info.exists) return '<span class="missing">-</span>';
  var s = info.expiry_status;
  if (s === "expired") return '<span class="missing">EXPIRED</span>';
  if (s === "critical") return '<span class="missing">' + info.expires_in_days + 'd left</span>';
  if (s === "warning") return '<span class="warn">' + info.expires_in_days + 'd left</span>';
  if (s === "ok") return '<span class="ok">' + info.expires_in_days + 'd left</span>';
  if (s === "no_auth_cookies") return '<span class="missing">no auth cookies</span>';
  if (s === "session_only") return '<span class="warn">session only</span>';
  return '<span class="warn">unknown</span>';
}

async function loadStatus() {
  const res = await fetch("/admin/cookies/status?key=" + KEY);
  const data = await res.json();
  const rows = Object.entries(data).map(function(entry) {
    const slotName = entry[0];
    const info = entry[1];
    const cls = info.exists ? "ok" : "missing";
    const sizeInfo = info.exists ? (info.size_bytes / 1024).toFixed(1) + " KB" : "not uploaded";
    const modified = info.exists ? new Date(info.last_modified * 1000).toLocaleString() : "-";
    return "<tr><td>" + slotName + "</td><td class=\\"" + cls + "\\">" + (info.exists ? "present" : "missing") + "</td><td>" + expiryCell(info) + "</td><td>" + sizeInfo + "</td><td>" + modified + "</td></tr>";
  }).join("");
  document.getElementById("statusTable").innerHTML =
    "<tr><th>Slot</th><th>File</th><th>Session</th><th>Size</th><th>Last updated</th></tr>" + rows;
}

document.getElementById("uploadForm").addEventListener("submit", async function(e) {
  e.preventDefault();
  const slot = document.getElementById("slot").value;
  const fileInput = document.getElementById("cookieFile");
  const formData = new FormData();
  formData.append("file", fileInput.files[0]);

  const res = await fetch("/admin/upload-cookies?key=" + KEY + "&slot=" + slot, {
    method: "POST",
    body: formData,
  });
  const data = await res.json();
  const resultDiv = document.getElementById("result");
  if (res.ok) {
    var warn = "";
    if (data.expiry_status === "expired") {
      warn = ' <span class="missing">- WARNING: this export is already expired.</span>';
    } else if (data.expiry_status === "no_auth_cookies") {
      warn = ' <span class="missing">- WARNING: no auth cookies found, likely a logged-out export.</span>';
    } else if (data.expires_in_days != null) {
      warn = " - session valid for " + data.expires_in_days + " days.";
    }
    resultDiv.innerHTML = "<span class=\\"ok\\">Uploaded successfully: " + data.bytes_written + " bytes to slot " + data.slot + "</span>" + warn;
  } else {
    resultDiv.innerHTML = "<span class=\\"missing\\">Error: " + data.detail + "</span>";
  }
  loadStatus();
});

loadStatus();
</script>
</body>
</html>
    """
    html = html.replace("PLACEHOLDER_ADMIN_KEY", ADMIN_KEY)
    return HTMLResponse(content=html)