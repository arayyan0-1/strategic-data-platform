# src/sdp/ingest/rest.py
from __future__ import annotations

import datetime as dt
import gzip
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

_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_RETRY_AFTER_STATUS = frozenset({429, 503})
_MAX_BACKOFF_S = 30
_MAX_RETRY_AFTER_S = 120


def _client() -> httpx.Client:
    return httpx.Client(
        base_url=settings.massive_api_base,
        headers={"Authorization": f"Bearer {settings.massive_api_key}"},
        timeout=httpx.Timeout(30.0, connect=10.0),
        follow_redirects=True,
    )


def _retry_after(resp: httpx.Response) -> int | None:
    """Return the Retry-After delay in seconds, capped at 120 s.
    Return None when the header is absent or is not an integer."""
    if resp.status_code not in _RETRY_AFTER_STATUS:
        return None
    value = resp.headers.get("Retry-After", "").strip()
    if not (value.isascii() and value.isdigit()):
        return None
    return min(int(value), _MAX_RETRY_AFTER_S)


def _get(client: httpx.Client, url: str, params: dict | None = None, *, attempts: int = 5):
    """Return the JSON body of one GET request.

    A transport error, HTTP 429 and HTTP 5xx cause a retry with exponential backoff.
    Any other error status causes an error immediately.
    """
    if attempts < 1:
        raise ValueError(f"attempts must be 1 or more. The value was {attempts}.")
    target = client.build_request("GET", url, params=params).url
    last = ""
    for i in range(attempts):
        wait = min(2**i, _MAX_BACKOFF_S)
        try:
            resp = client.get(url, params=params)
        except httpx.TransportError as exc:
            last = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
        else:
            if resp.status_code in _RETRY_STATUS:
                last = f"HTTP {resp.status_code}"
                retry_after = _retry_after(resp)
                if retry_after is not None:
                    wait = retry_after
            elif resp.is_error:
                raise RuntimeError(
                    f"HTTP {resp.status_code} on {resp.request.url}\n{resp.text[:800]}"
                )
            else:
                return resp.json()
        if i + 1 < attempts:
            log.warning("Attempt %s of %s on %s failed with %s. Retry in %s s.",
                        i + 1, attempts, target, last, wait)
            time.sleep(wait)
    raise RuntimeError(f"All {attempts} attempts on {target} failed. The last error was {last}.")


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
    """An unused temporary path beside `dest`, unique per call, so two fetches of
    the same date do not share a name and race on the rename. The directory
    matches `dest`, because os.replace is atomic only within one filesystem."""
    fd, name = tempfile.mkstemp(dir=dest.parent, prefix=f"{dest.name}.", suffix=".part")
    os.close(fd)
    # mkstemp creates the file with mode 0600 and os.replace keeps that mode.
    # The lake is not a secret, so give the published file the usual mode.
    os.chmod(name, 0o644)
    return Path(name)


# The overloads make the return type None only when allow_empty is True, so the
# call sites that never pass it do not test for a None they cannot receive.
@overload
def dump_ndjson(dataset: str, path: str, params: dict[str, Any],
                pull_date: dt.date, *, force: bool = ...,
                allow_empty: Literal[False] = ...,
                compress: bool = ...) -> Path: ...


@overload
def dump_ndjson(dataset: str, path: str, params: dict[str, Any],
                pull_date: dt.date, *, force: bool = ...,
                allow_empty: Literal[True],
                compress: bool = ...) -> Path | None: ...


def dump_ndjson(dataset: str, path: str, params: dict[str, Any],
                pull_date: dt.date, *, force: bool = False,
                allow_empty: bool = False,
                compress: bool = False) -> Path | None:
    """Write every record of an endpoint to one NDJSON file in vendor/.

    allow_empty: for a dataset with no record on some dates (short interest's off
    weeks, short volume before 2024-02-06), write no file and return None.
    compress: write gzip NDJSON (.ndjson.gz), which DuckDB reads directly, for a
    large daily full pull like dividends.
    """
    stem = f"{pull_date:%Y-%m-%d}"
    dest = settings.vendor_dir / dataset / f"{stem}{'.ndjson.gz' if compress else '.ndjson'}"
    other = settings.vendor_dir / dataset / f"{stem}{'.ndjson' if compress else '.ndjson.gz'}"
    if not force:
        for candidate in (dest, other):
            if candidate.exists():
                log.info("The vendor file is already present: %s", candidate)
                return candidate
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = _temp_beside(dest)

    n = 0
    try:
        def _open():
            if compress:
                return gzip.open(tmp, "wt", encoding="utf-8")
            return tmp.open("w", encoding="utf-8")

        with _open() as fh:
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