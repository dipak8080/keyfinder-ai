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
"""

import os
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

    return JSONResponse({"status": "ok", "slot": slot, "path": path, "bytes_written": len(content)})


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
            }
        else:
            result[f"slot_{slot}"] = {"exists": False, "path": path}
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

async function loadStatus() {
  const res = await fetch("/admin/cookies/status?key=" + KEY);
  const data = await res.json();
  const rows = Object.entries(data).map(function(entry) {
    const slotName = entry[0];
    const info = entry[1];
    const cls = info.exists ? "ok" : "missing";
    const sizeInfo = info.exists ? (info.size_bytes / 1024).toFixed(1) + " KB" : "not uploaded";
    const modified = info.exists ? new Date(info.last_modified * 1000).toLocaleString() : "-";
    return "<tr><td>" + slotName + "</td><td class=\\"" + cls + "\\">" + (info.exists ? "present" : "missing") + "</td><td>" + sizeInfo + "</td><td>" + modified + "</td></tr>";
  }).join("");
  document.getElementById("statusTable").innerHTML =
    "<tr><th>Slot</th><th>Status</th><th>Size</th><th>Last updated</th></tr>" + rows;
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
    resultDiv.innerHTML = "<span class=\\"ok\\">Uploaded successfully: " + data.bytes_written + " bytes to slot " + data.slot + "</span>";
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