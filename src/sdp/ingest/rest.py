# src/sdp/ingest/rest.py
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Iterator

import httpx

from sdp.config import settings

log = logging.getLogger(__name__)

_RETRY_STATUS = {429, 500, 502, 503, 504}


def _client() -> httpx.Client:
    return httpx.Client(
        base_url=settings.massive_api_base,
        headers={"Authorization": f"Bearer {settings.massive_api_key}"},
        timeout=httpx.Timeout(30.0, connect=10.0),
        follow_redirects=True,
    )


def _get(client: httpx.Client, url: str, params: dict | None = None, *, attempts: int = 5):
    for i in range(attempts):
        resp = client.get(url, params=params)
        if resp.status_code in _RETRY_STATUS:
            wait = min(2**i, 30)
            log.warning("HTTP %s on %s, retrying in %ss", resp.status_code, url, wait)
            time.sleep(wait)
            continue
        if resp.is_error:
            raise RuntimeError(
                f"HTTP {resp.status_code} on {resp.request.url}\n{resp.text[:800]}"
            )
        return resp.json()
    raise RuntimeError(f"exhausted {attempts} attempts on {url}")


def paginate(path: str, params: dict[str, Any]) -> Iterator[dict]:
    """Yield every result across all pages of a cursor-paginated endpoint."""
    with _client() as client:
        payload = _get(client, path, params)
        page = 0
        seen_cursors: set[str] = set()
        while True:
            page += 1
            results = payload.get("results") or []
            log.info("%s page %s: %s records", path, page, len(results))
            yield from results

            next_url = payload.get("next_url")
            if not next_url:
                return
            if next_url in seen_cursors:
                raise RuntimeError(f"{path}: cursor repeated at page {page}, aborting")
            seen_cursors.add(next_url)
            if page > 5000:
                raise RuntimeError(f"{path}: exceeded 5000 pages, aborting")
            payload = _get(client, next_url)


def dump_ndjson(dataset: str, path: str, params: dict[str, Any],
                pull_date, *, force: bool = False) -> Path:
    dest = settings.vendor_dir / dataset / f"{pull_date:%Y-%m-%d}.ndjson"
    if dest.exists() and not force:
        log.info("vendor file already present: %s", dest)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")

    n = 0
    with tmp.open("w", encoding="utf-8") as fh:
        for record in paginate(path, params):
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")
            n += 1

    if n == 0:
        tmp.unlink()
        raise RuntimeError(f"{dataset}: zero records returned")

    os.replace(tmp, dest)
    log.info("wrote %s records to %s", n, dest)
    return dest