"""Map the identifiers of N-PORT equity lines to FIGIs with OpenFIGI.

    python -m sdp.ingest.openfigi [--limit N]

Current state: each run adds the identifiers of sec_nport that have no row yet, and publishes
the whole table again. A row has a cusip or, for a line that states none, an isin. An identifier
OpenFIGI cannot match is stored with null FIGIs and asked again after RETRY_DAYS. The keyless
limits are 10 jobs a request and 25 requests a minute.

A code that starts with a letter is a CINS (Linde, Accenture and other foreign-domiciled issuers
of US-listed stock), which OpenFIGI looks up under its own id type. iShares states no code at all
for such a name, only its ISIN.

The share-class FIGI belongs to the security, not to a listing. A stock delisted from the US keeps
it on its foreign listings, so a CUSIP with no US listing still gets its share-class FIGI; its
composite FIGI and ticker are the US listing's and stay null.
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
ISIN_LENGTH = 12
STOCK_TYPES = ("Common Stock", "REIT", "ADR", "Depositary Receipt")

COLUMNS = {
    "cusip": "varchar", "isin": "varchar", "share_class_figi": "varchar",
    "composite_figi": "varchar", "ticker": "varchar", "name": "varchar",
    "security_type": "varchar", "mapped_on": "date",
}


def id_type(code: str) -> str:
    """Return OpenFIGI's id type for a 12-character ISIN or a 9-character CUSIP or CINS."""
    if len(code) == ISIN_LENGTH:
        return "ID_ISIN"
    return "ID_CINS" if code[:1].isalpha() else "ID_CUSIP"


def _ids(code: str) -> tuple[str | None, str | None]:
    return (code, None) if len(code) != ISIN_LENGTH else (None, code)


def pick(listings: list[dict] | None) -> tuple[dict | None, bool]:
    """Return the listing that names the security, and whether it is the US listing.

    The US composite listing comes first, a stock type before any other. Without one, any
    listing with a share-class FIGI names the security.
    """
    rows = [d for d in listings or [] if d.get("shareClassFIGI")]
    us = [d for d in rows if d.get("exchCode") == "US"]

    def rank(d: dict) -> int:
        t = d.get("securityType")
        return STOCK_TYPES.index(t) if t in STOCK_TYPES else len(STOCK_TYPES)

    if us:
        return min(us, key=rank), True
    return (min(rows, key=rank), False) if rows else (None, False)


def parse_result(code: str, item: dict, today: dt.date) -> tuple:
    """Return the table row for one job's answer. No match gives null FIGIs."""
    hit, is_us = pick(item.get("data"))
    if hit is None:
        return (*_ids(code), None, None, None, None, None, today)
    return (*_ids(code), hit["shareClassFIGI"], hit.get("compositeFIGI") if is_us else None,
            hit.get("ticker") if is_us else None, hit.get("name"), hit.get("securityType"), today)


class OpenFigi:
    """The keyless mapping endpoint, paced to its request limit."""

    def __init__(self, transport: httpx.BaseTransport | None = None,
                 interval: float = MIN_INTERVAL):
        self.interval = interval
        self._next = 0.0
        self._client = httpx.Client(transport=transport, timeout=httpx.Timeout(60.0, connect=10.0))

    def close(self) -> None:
        self._client.close()

    def map(self, codes: list[str], *, attempts: int = 6) -> list[dict]:
        """Return one answer per code, in order. Retry 429, 5xx and transport errors."""
        if len(codes) > BATCH:
            raise ValueError(f"A request takes at most {BATCH} jobs. It got {len(codes)}.")
        jobs = [{"idType": id_type(c), "idValue": c} for c in codes]
        last = ""
        for i in range(attempts):
            time.sleep(max(0.0, self._next - time.monotonic()))
            wait = min(2**i * 5, 120)
            sent = time.monotonic()
            try:
                resp = self._client.post(API_URL, json=jobs)
            except httpx.TransportError as exc:
                last = type(exc).__name__
            else:
                self._next = sent + self.interval
                if resp.status_code in _RETRY_STATUS:
                    last = f"HTTP {resp.status_code}"
                    reset = resp.headers.get("ratelimit-reset", "")
                    wait = _retry_after(resp) or (int(reset) + 1 if reset.isdigit() else wait)
                elif resp.is_error:
                    raise RuntimeError(f"HTTP {resp.status_code} on {API_URL}: {resp.text[:200]}")
                else:
                    answers = resp.json()
                    if len(answers) != len(codes):
                        raise RuntimeError(f"OpenFIGI answered {len(answers)} jobs for "
                                           f"{len(codes)}.")
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
    """Return the rows of the published table by CUSIP, or by ISIN where there is no CUSIP."""
    try:
        rel = dal.openfigi_cusips()
    except dal.MissingPartition:
        return {}
    return {r[0] or r[1]: r for r in rel.project(", ".join(COLUMNS)).fetchall()}


def pending(today: dt.date, have: dict[str, tuple]) -> list[str]:
    """Return the codes of sec_nport equity lines that need a lookup, sorted.

    A line's code is its CUSIP, else its ISIN.
    """
    try:
        rel = dal.sec_nport()
    except dal.MissingPartition:
        return []
    wanted = {r[0] for r in rel.filter("asset_cat = 'EC'").project(
        "coalesce(cusip, isin) as code").filter(
        "code is not null and length(code) in (9, 12)").distinct().fetchall()}
    stale = today - dt.timedelta(days=RETRY_DAYS)
    return sorted(c for c in wanted
                  if c not in have or (have[c][2] is None and have[c][7] <= stale))


def audit(staged) -> int:
    n, dup, no_code, no_date, odd = duckdb.execute("""
        select count(*),
               count(coalesce(cusip, isin)) - count(distinct coalesce(cusip, isin)),
               count(*) filter (where (cusip is null) = (isin is null)),
               count(*) filter (where mapped_on is null),
               count(*) filter (where share_class_figi is null and composite_figi is not null)
        from read_parquet(?)""", [str(staged)]).fetchone()
    fatal = []
    if dup:
        fatal.append(f"{dup} duplicate codes")
    if no_code:
        fatal.append(f"{no_code} rows with both a CUSIP and an ISIN, or neither")
    if no_date:
        fatal.append("a null mapping date")
    if odd:
        fatal.append(f"{odd} rows with a composite FIGI and no share-class FIGI")
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
    """Map the codes that lack a row, up to `limit`. Return how many were asked."""
    today = today or dt.datetime.now(dt.UTC).date()
    rows = mapped()
    todo = pending(today, rows)[:limit]
    if not todo:
        return 0
    own = client or OpenFigi()
    log.info("Mapping %s codes in %s requests.", len(todo), -(-len(todo) // BATCH))
    try:
        for n, i in enumerate(range(0, len(todo), BATCH), 1):
            batch = todo[i:i + BATCH]
            for code, item in zip(batch, own.map(batch), strict=True):
                rows[code] = parse_result(code, item, today)
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
                                description="Map N-PORT identifiers to FIGIs.")
    p.add_argument("--limit", type=int, help="Ask for at most this many codes.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(f"asked {ingest(args.limit)} codes")


if __name__ == "__main__":
    main()
