"""Account library endpoints.

    GET    /library                              the signed-in account's saved Studio results
    GET    /library/{job_id}/stem/{stem}         short-lived R2 link (?download=1 to save, ?json=1 for the URL)
    DELETE /library/{job_id}                     remove an item now
"""

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from fastapi.responses import RedirectResponse

import library
from credits import paywall
from credits.identity import Identity

router = APIRouter()


def _account(identity: Identity) -> str:
    if not identity.account_id:
        raise HTTPException(401, {"error": "sign_in_required"})
    if not library.configured():
        raise HTTPException(503, {"error": "library_unavailable"})
    return identity.account_id


@router.get("/library")
def list_library(response: Response, identity: Identity = Depends(paywall.get_identity)) -> dict:
    response.headers["Cache-Control"] = "no-store"
    account_id = _account(identity)
    return {"enabled": library.enabled(), "items": library.items_for(account_id)}


@router.get("/library/{job_id}/stem/{stem}")
def library_stem(
    job_id: str = Path(..., max_length=64),
    stem: str = Path(..., max_length=32, pattern=r"^[a-z][a-z0-9_]*$"),
    download: bool = Query(False),
    json: bool = Query(False),
    identity: Identity = Depends(paywall.get_identity),
):
    row = library.item(_account(identity), job_id)
    if row is None:
        raise HTTPException(404, "Not in your library (it may have expired).")
    url = library.signed_url(row, stem, download)
    if url is None:
        raise HTTPException(404, "That stem isn't in this item.")
    if json:
        return {"url": url, "expires_in": 3600}
    return RedirectResponse(url, status_code=302, headers={"Cache-Control": "no-store"})


@router.delete("/library/{job_id}")
def delete_library_item(job_id: str = Path(..., max_length=64),
                        identity: Identity = Depends(paywall.get_identity)) -> dict:
    if not library.remove(job_id, _account(identity)):
        raise HTTPException(404, "Not in your library.")
    return {"ok": True}