# src/sdp/ingest/common.py
"""The parts that the ingest modules share: the audit error, the XNYS calendar, the
publish step, the two modes that publish a date again, and the download of a public
file."""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import exchange_calendars as xcals
import httpx

from sdp.config import settings
from sdp.ingest.rest import _temp_beside

log = logging.getLogger(__name__)

XNYS = xcals.get_calendar("XNYS")


class AuditFailure(RuntimeError):
    """A staged file failed a fatal check. The partition is not published."""


def get_public(url: str, attempts: int = 4, *, user_agent: str | None = "Mozilla/5.0") -> bytes:
    """Return the body of a public URL. A transport error or HTTP 5xx causes a retry
    with backoff. A 4xx error raises at once. The client sends no credentials. With
    user_agent None, the client sends its own default, which FRED needs: FRED stalls a
    request with a set User-Agent."""
    last = ""
    headers = {"User-Agent": user_agent} if user_agent else {}
    with httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0), follow_redirects=True,
                      headers=headers) as client:
        for i in range(attempts):
            try:
                resp = client.get(url)
            except httpx.TransportError as exc:
                last = type(exc).__name__
            else:
                if resp.status_code < 500 and resp.is_error:
                    raise RuntimeError(f"HTTP {resp.status_code} on {url}")
                if not resp.is_error:
                    return resp.content
                last = f"HTTP {resp.status_code}"
            if i + 1 < attempts:
                log.warning("Attempt %s of %s on %s failed with %s.", i + 1, attempts, url, last)
                time.sleep(2 ** i)
    raise RuntimeError(f"All {attempts} attempts on {url} failed. The last error was {last}.")


def one(sql: str) -> tuple:
    """Run a query and return its first row. Raise when the query returns no row."""
    row = duckdb.execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"The query returned no row: {sql[:120]}")
    return row


def is_session(d: dt.date) -> bool:
    """Return True when the date is an XNYS session."""
    return XNYS.is_session(d.isoformat())


def check_mode(refetch: bool, rebuild: bool) -> None:
    """Refuse a call that sets both modes."""
    if refetch and rebuild:
        raise ValueError("Set refetch or rebuild. Do not set both.")


def require_file(path: Path) -> Path:
    """Return the vendor file. Raise when it does not exist, because a rebuild
    does not download."""
    if not path.exists():
        raise FileNotFoundError(
            f"The vendor file does not exist: {path}. A rebuild reads only vendor/ "
            f"and does not download. Use --refetch to download the date."
        )
    return path


def vendor_ndjson(dataset: str, d: dt.date) -> Path | None:
    """Return the vendor NDJSON file of one date, gzip or plain. Return None when
    no file exists."""
    for suffix in (".ndjson.gz", ".ndjson"):
        path = settings.vendor_dir / dataset / f"{d:%Y-%m-%d}{suffix}"
        if path.exists():
            return path
    return None


def require_vendor_ndjson(dataset: str, d: dt.date) -> Path:
    """Return the vendor NDJSON file of one date. Raise when no file exists."""
    return vendor_ndjson(dataset, d) or require_file(
        settings.vendor_dir / dataset / f"{d:%Y-%m-%d}.ndjson.gz"
    )


@contextmanager
def keep_on_failure(path: Path | None) -> Iterator[None]:
    """Keep a copy of a vendor file while a refetch replaces it. Put the copy
    back when the block raises, so a failed refetch keeps the earlier bytes."""
    if path is None or not path.exists():
        yield
        return
    backup = _temp_beside(path)
    shutil.copy2(path, backup)
    try:
        yield
    except BaseException:
        os.replace(backup, path)
        log.warning("The refetch failed. The earlier vendor file is back: %s", path)
        raise
    finally:
        backup.unlink(missing_ok=True)


def publish(staged: Path, dest: Path) -> Path:
    """Move a staged file into raw/. The move is atomic, because both paths are
    on one filesystem."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staged, dest)
    log.info("Published %s", dest)
    return dest


def add_mode_flags(parser: argparse.ArgumentParser) -> None:
    """Add the two modes as flags that exclude each other."""
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--refetch", action="store_true",
        help="Download each date again, replace its vendor file and publish it again.",
    )
    mode.add_argument(
        "--rebuild", action="store_true",
        help="Build each date again from its vendor file and publish it. Do not download.",
    )
