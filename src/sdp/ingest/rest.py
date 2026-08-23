# src/sdp/ingest/rest.py
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal, overload

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
            log.warning("HTTP %s from %s. Retry in %s s.", resp.status_code, url, wait)
            time.sleep(wait)
            continue
        if resp.is_error:
            raise RuntimeError(
                f"HTTP {resp.status_code} on {resp.request.url}\n{resp.text[:800]}"
            )
        return resp.json()
    raise RuntimeError(f"All {attempts} attempts on {url} failed.")


def paginate(path: str, params: dict[str, Any]) -> Iterator[dict]:
    """Yield every result from every page of an endpoint that uses a cursor."""
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
                raise RuntimeError(f"{path}: the cursor repeated at page {page}. The run stopped.")
            seen_cursors.add(next_url)
            if page > 5000:
                raise RuntimeError(f"{path}: more than 5000 pages. The run stopped.")
            payload = _get(client, next_url)


def _temp_beside(dest: Path) -> Path:
    """Return an unused temporary path in the directory of `dest`.

    The name is unique for each call. Two fetches of the same date therefore
    write to two different files, and each rename is atomic and independent of
    the other.

    A shared name is what breaks. The loser of the race finds that the winner
    has already renamed the file away, and its own rename fails with
    FileNotFoundError. That happened on 2026-08-23, when a second copy of the
    backfill script ran beside the first: 247 dates failed that way and 41 more
    failed reading a file mid-rename.

    The directory is the same as the destination on purpose. `os.replace` is
    atomic only inside one filesystem.
    """
    fd, name = tempfile.mkstemp(dir=dest.parent, prefix=f"{dest.name}.", suffix=".part")
    os.close(fd)
    return Path(name)


# The two signatures below say that this function returns None only when the
# caller asked for it. Without them the return type is `Path | None` for every
# call, and each of the four call sites has to test for a None that three of
# them can never receive. The rule belongs in the type and not in a branch.
@overload
def dump_ndjson(dataset: str, path: str, params: dict[str, Any],
                pull_date: dt.date, *, force: bool = ...,
                allow_empty: Literal[False] = ...) -> Path: ...


@overload
def dump_ndjson(dataset: str, path: str, params: dict[str, Any],
                pull_date: dt.date, *, force: bool = ...,
                allow_empty: Literal[True]) -> Path | None: ...


def dump_ndjson(dataset: str, path: str, params: dict[str, Any],
                pull_date: dt.date, *, force: bool = False,
                allow_empty: bool = False) -> Path | None:
    """Write every record of an endpoint to one NDJSON file in vendor/.

    Set allow_empty for a dataset that has no record on some dates. The short
    interest endpoint reports on a two-week cadence, so most sessions have no
    settlement. The short volume endpoint starts on 2024-02-06 and has nothing
    before that date. An empty answer on those dates is the correct answer and
    it is not a failure. The function then writes no file and returns None.
    """
    dest = settings.vendor_dir / dataset / f"{pull_date:%Y-%m-%d}.ndjson"
    if dest.exists() and not force:
        log.info("The vendor file is already present: %s", dest)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = _temp_beside(dest)

    n = 0
    try:
        with tmp.open("w", encoding="utf-8") as fh:
            for record in paginate(path, params):
                fh.write(json.dumps(record, separators=(",", ":")) + "\n")
                n += 1

        if n == 0:
            if allow_empty:
                log.info("%s: the endpoint returned no records for %s.", dataset, pull_date)
                return None
            raise RuntimeError(f"{dataset}: the endpoint returned no records.")

        os.replace(tmp, dest)
    finally:
        # A successful rename already moved the file, so this is then a no-op.
        # Anything still here is from an empty answer or from a failure part way
        # through, and it must not be left for the next run to find.
        tmp.unlink(missing_ok=True)

    log.info("Wrote %s records to %s", n, dest)
    return dest