"""Map the CUSIPs of N-PORT equity lines to FIGIs with OpenFIGI.

    python -m sdp.ingest.openfigi [--limit N]

Current state: each run adds the CUSIPs of sec_nport that have no row yet, and publishes the
whole table again. A CUSIP OpenFIGI cannot match is stored with null FIGIs and asked again
after RETRY_DAYS. The keyless limits are 10 jobs a request and 25 requests a minute.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import time

import duckdb
import httpx

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import AuditFailure, publish, rows_to_parquet
from sdp.ingest.rest import _RETRY_STATUS, _retry_after

log = logging.getLogger(__name__)

DATASET = dal.OPENFIGI_CUSIPS
API_URL = "https://api.openfigi.com/v3/mapping"
BATCH = 10
MIN_INTERVAL = 2.5  # 25 requests a minute is the limit; 24 leaves room.
CHECKPOINT = 25  # Requests between two publishes, so an interrupted run keeps its work.
RETRY_DAYS = 30
STOCK_TYPES = ("Common Stock", "REIT", "ADR", "Depositary Receipt")

COLUMNS = {
    "cusip": "varchar", "share_class_figi": "varchar", "composite_figi": "varchar",
    "ticker": "varchar", "name": "varchar", "security_type": "varchar", "mapped_on": "date",
}


def pick_us(listings: list[dict] | None) -> dict | None:
    """Return the US composite listing, a stock type before any other, else None."""
    us = [d for d in listings or [] if d.get("exchCode") == "US"]
    if not us:
        return None

    def rank(d: dict) -> int:
        t = d.get("securityType")
        return STOCK_TYPES.index(t) if t in STOCK_TYPES else len(STOCK_TYPES)

    return min(us, key=rank)


def parse_result(cusip: str, item: dict, today: dt.date) -> tuple:
    """Return the table row for one job's answer. No match gives null FIGIs."""
    hit = pick_us(item.get("data"))
    if hit is None:
        return (cusip, None, None, None, None, None, today)
    return (cusip, hit.get("shareClassFIGI"), hit.get("compositeFIGI"), hit.get("ticker"),
            hit.get("name"), hit.get("securityType"), today)


class OpenFigi:
    """The keyless mapping endpoint, paced to its request limit."""

    def __init__(self, transport: httpx.BaseTransport | None = None,
                 interval: float = MIN_INTERVAL):
        self.interval = interval
        self._next = 0.0
        self._client = httpx.Client(transport=transport, timeout=httpx.Timeout(60.0, connect=10.0))

    def close(self) -> None:
        self._client.close()

    def map(self, cusips: list[str], *, attempts: int = 6) -> list[dict]:
        """Return one answer per CUSIP, in order. Retry 429, 5xx and transport errors."""
        if len(cusips) > BATCH:
            raise ValueError(f"A request takes at most {BATCH} jobs. It got {len(cusips)}.")
        jobs = [{"idType": "ID_CUSIP", "idValue": c, "exchCode": "US"} for c in cusips]
        last = ""
        for i in range(attempts):
            time.sleep(max(0.0, self._next - time.monotonic()))
            wait = min(2**i * 5, 120)
            try:
                resp = self._client.post(API_URL, json=jobs)
            except httpx.TransportError as exc:
                last = type(exc).__name__
            else:
                self._next = time.monotonic() + self.interval
                if resp.status_code in _RETRY_STATUS:
                    last = f"HTTP {resp.status_code}"
                    reset = resp.headers.get("ratelimit-reset", "")
                    wait = _retry_after(resp) or (int(reset) + 1 if reset.isdigit() else wait)
                elif resp.is_error:
                    raise RuntimeError(f"HTTP {resp.status_code} on {API_URL}: {resp.text[:200]}")
                else:
                    answers = resp.json()
                    if len(answers) != len(cusips):
                        raise RuntimeError(f"OpenFIGI answered {len(answers)} jobs for "
                                           f"{len(cusips)}.")
                    remaining = resp.headers.get("ratelimit-remaining", "")
                    reset = resp.headers.get("ratelimit-reset", "")
                    if remaining == "0" and reset.isdigit():
                        self._next = max(self._next, time.monotonic() + int(reset) + 1)
                    return answers
            if i + 1 < attempts:
                log.warning("Attempt %s of %s on OpenFIGI failed with %s. Retry in %s s.",
                            i + 1, attempts, last, wait)
                time.sleep(wait)
        raise RuntimeError(f"All {attempts} attempts on OpenFIGI failed. The last error was "
                           f"{last}.")


def mapped() -> dict[str, tuple]:
    """Return the rows of the published table by CUSIP."""
    try:
        rel = dal.openfigi_cusips()
    except dal.MissingPartition:
        return {}
    return {r[0]: r for r in rel.project(", ".join(COLUMNS)).fetchall()}


def pending(today: dt.date, have: dict[str, tuple]) -> list[str]:
    """Return the equity CUSIPs of sec_nport that need a lookup, sorted."""
    try:
        rel = dal.sec_nport()
    except dal.MissingPartition:
        return []
    wanted = {r[0] for r in rel.filter(
        "asset_cat = 'EC' and cusip is not null and length(cusip) = 9"
    ).project("cusip").distinct().fetchall()}
    stale = today - dt.timedelta(days=RETRY_DAYS)
    return sorted(c for c in wanted
                  if c not in have or (have[c][2] is None and have[c][6] <= stale))


def audit(staged) -> int:
    n, dup, no_date, odd = duckdb.execute("""
        select count(*),
               count(*) - count(distinct cusip),
               count(*) filter (where mapped_on is null or cusip is null),
               count(*) filter (where (share_class_figi is null) <> (composite_figi is null))
        from read_parquet(?)""", [str(staged)]).fetchone()
    fatal = []
    if dup:
        fatal.append(f"{dup} duplicate CUSIPs")
    if no_date:
        fatal.append("a null CUSIP or mapping date")
    if odd:
        fatal.append(f"{odd} rows with one FIGI and not the other")
    if fatal:
        raise AuditFailure(f"{DATASET.name}: " + ". ".join(fatal) + ".")
    return n


def _publish(rows: dict[str, tuple]) -> None:
    staged = settings.staging_dir / DATASET.name / "data.parquet"
    rows_to_parquet((rows[c] for c in sorted(rows)), COLUMNS, staged)
    try:
        audit(staged)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    publish(staged, DATASET.table_file)


def ingest(limit: int | None = None, *, client: OpenFigi | None = None,
           today: dt.date | None = None) -> int:
    """Map the CUSIPs that lack a row, up to `limit`. Return how many were asked."""
    today = today or dt.datetime.now(dt.UTC).date()
    rows = mapped()
    todo = pending(today, rows)[:limit]
    if not todo:
        return 0
    own = client or OpenFigi()
    log.info("Mapping %s CUSIPs in %s requests.", len(todo), -(-len(todo) // BATCH))
    try:
        for n, i in enumerate(range(0, len(todo), BATCH), 1):
            batch = todo[i:i + BATCH]
            for cusip, item in zip(batch, own.map(batch), strict=True):
                rows[cusip] = parse_result(cusip, item, today)
            if n % CHECKPOINT == 0:
                _publish(rows)
                log.info("Mapped %s of %s.", i + len(batch), len(todo))
    finally:
        if rows:
            _publish(rows)
        if client is None:
            own.close()
    return len(todo)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.openfigi",
                                description="Map N-PORT CUSIPs to FIGIs.")
    p.add_argument("--limit", type=int, help="Ask for at most this many CUSIPs.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(f"asked {ingest(args.limit)} CUSIPs")


if __name__ == "__main__":
    main()
