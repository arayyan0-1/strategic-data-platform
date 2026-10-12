"""Ingest the holdings of Form N-PORT filings for the funds in the seed nport_funds.

    python -m sdp.ingest.sec_nport [--funds T1,T2] [--since 2021-01-01] [--rebuild]

Per fund: list its original NPORT-P filings on EDGAR, keep each primary_doc.xml gzipped in
vendor/sec_nport/<series_id>/, and publish one file per report date under date=<report date>.
filed is when the public first saw the holdings. A fund that files twice for one report date
keeps the earlier filing. --rebuild builds from vendor/ and needs no network.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import json
import logging
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import duckdb
import httpx

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import AuditFailure, eastern, publish, rows_to_parquet, write_gzip
from sdp.ingest.rest import _RETRY_STATUS, _retry_after, _temp_beside

log = logging.getLogger(__name__)

DATASET = dal.SEC_NPORT
FUNDS_FILE = settings.seeds_dir / "nport_funds.csv"
SINCE = dt.date(2021, 1, 1)
FORM = "NPORT-P"
MIN_INTERVAL = 0.2  # SEC allows 10 requests a second; stay at half.
WORKERS = 4
ATOM_COUNT = 100  # EDGAR's largest page.
MAX_PAGES = 20
MIN_PCT, MAX_PCT = 0.5, 1.5
MIN_LINES = 10

NO_AGENT = ("Set SEC_USER_AGENT in .env to 'Name email'. "
            "SEC refuses a request without a contact.")

COLUMNS = {
    "fund": "varchar", "series_id": "varchar", "accession": "varchar", "filed": "date",
    "report_date": "date", "cusip": "varchar", "isin": "varchar", "name": "varchar",
    "title": "varchar", "balance": "double", "units": "varchar", "val_usd": "double",
    "pct_val": "double", "asset_cat": "varchar", "issuer_cat": "varchar",
    "inv_country": "varchar", "payoff": "varchar",
}
LINE_FIELDS = list(COLUMNS)[5:]

NPORT = "{http://www.sec.gov/edgar/nport}"
ATOM = "{http://www.w3.org/2005/Atom}"
_HREF = re.compile(r"/Archives/edgar/data/(\d+)/(\d{18})/")


# ---------- EDGAR client ----------

class Throttle:
    """Space the starts of requests at least `interval` seconds apart, across threads."""

    def __init__(self, interval: float):
        self.interval = interval
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            at = max(now, self._next)
            self._next = at + self.interval
        if at > now:
            time.sleep(at - now)


class MissingContact(RuntimeError):
    """SEC_USER_AGENT is unset."""


class Sec:
    """A throttled EDGAR client that sends the contact User-Agent from settings."""

    def __init__(self, transport: httpx.BaseTransport | None = None,
                 interval: float = MIN_INTERVAL):
        agent = (settings.sec_user_agent or "").strip()
        if not agent:
            raise MissingContact(NO_AGENT)
        self._throttle = Throttle(interval)
        self._client = httpx.Client(headers={"User-Agent": agent}, transport=transport,
                                    timeout=httpx.Timeout(60.0, connect=10.0),
                                    follow_redirects=True)

    def close(self) -> None:
        self._client.close()

    def get(self, url: str, params: dict | None = None, *, attempts: int = 5) -> bytes:
        """Return the body of a GET, retrying transport errors, 429 and 5xx with backoff.

        Any other error status raises at once.
        """
        last = ""
        for i in range(attempts):
            wait = min(2**i, 30)
            self._throttle.wait()
            try:
                resp = self._client.get(url, params=params)
            except httpx.TransportError as exc:
                last = type(exc).__name__
            else:
                if resp.status_code in _RETRY_STATUS:
                    last = f"HTTP {resp.status_code}"
                    wait = _retry_after(resp) or wait
                elif resp.is_error:
                    hint = (" SEC blocks a client that is over its rate or has no valid "
                            "contact in its User-Agent." if resp.status_code == 403 else "")
                    raise RuntimeError(f"HTTP {resp.status_code} on {resp.request.url}.{hint}")
                else:
                    return resp.content
            if i + 1 < attempts:
                log.warning("Attempt %s of %s on %s failed with %s. Retry in %s s.",
                            i + 1, attempts, url, last, wait)
                time.sleep(wait)
        raise RuntimeError(f"All {attempts} attempts on {url} failed. The last error was {last}.")


# ---------- the filing list ----------

@dataclass(frozen=True)
class Filing:
    accession: str
    filed: dt.date
    form: str
    cik: str | None

    @property
    def url(self) -> str:
        if self.cik is None:
            raise ValueError(f"{self.accession}: the filing link states no CIK.")
        return (f"{settings.sec_www_url}/Archives/edgar/data/{self.cik}/"
                f"{self.accession.replace('-', '')}/primary_doc.xml")


def parse_atom(data: bytes) -> list[Filing]:
    """Return the filings of one page of an EDGAR Atom list, newest first."""
    out = []
    for entry in ET.fromstring(data).iter(f"{ATOM}entry"):
        content = entry.find(f"{ATOM}content")
        if content is None:
            continue

        def text(tag: str, content=content) -> str | None:
            e = content.find(f"{ATOM}{tag}")
            return e.text.strip() if e is not None and e.text and e.text.strip() else None

        acc, filed, form = text("accession-number"), text("filing-date"), text("filing-type")
        if not (acc and filed and form):
            continue
        m = _HREF.search(text("filing-href") or "")
        out.append(Filing(acc, dt.date.fromisoformat(filed), form, str(int(m.group(1))) if m
                          else None))
    return out


def list_filings(sec: Sec, series_id: str, since: dt.date) -> list[Filing]:
    """Return the original NPORT-P filings of a series filed on or after `since`."""
    found: dict[str, Filing] = {}
    start = 0
    for _ in range(MAX_PAGES):
        page = parse_atom(sec.get(f"{settings.sec_www_url}/cgi-bin/browse-edgar", {
            "action": "getcompany", "CIK": series_id, "type": FORM, "dateb": "",
            "owner": "include", "count": ATOM_COUNT, "start": start, "output": "atom"}))
        for f in page:
            found.setdefault(f.accession, f)
        if len(page) < ATOM_COUNT or min(f.filed for f in page) < since:
            break
        start += ATOM_COUNT
    else:
        raise RuntimeError(f"{series_id}: the filing list ran past {MAX_PAGES} pages.")
    return sorted((f for f in found.values() if f.form == FORM and f.filed >= since),
                  key=lambda f: (f.filed, f.accession))


# ---------- parsing ----------

@dataclass(frozen=True)
class Parsed:
    series_id: str | None
    report_date: dt.date | None
    lines: list[tuple]


def _text(el: ET.Element, path: str) -> str | None:
    e = el.find(path)
    s = e.text.strip() if e is not None and e.text else ""
    return s or None


def _num(s: str | None) -> float | None:
    try:
        return float(s) if s is not None else None
    except ValueError:
        return None


def _id(s: str | None) -> str | None:
    """Return an identifier, or None for the placeholders filers write."""
    return None if s is None or s.upper() == "N/A" or not s.strip("0") else s


def parse_filing(data: bytes) -> Parsed:
    """Return the series, the report date and the lines of one primary_doc.xml.

    A line is the tuple of LINE_FIELDS. pct_val becomes a fraction of net assets.
    """
    root = ET.fromstring(data)
    gen = root.find(f"{NPORT}formData/{NPORT}genInfo")
    series = _text(gen, f"{NPORT}seriesId") if gen is not None else None
    rep = _text(gen, f"{NPORT}repPdDate") if gen is not None else None
    lines = []
    for el in root.iterfind(f"{NPORT}formData/{NPORT}invstOrSecs/{NPORT}invstOrSec"):
        isin = el.find(f"{NPORT}identifiers/{NPORT}isin")
        pct = _num(_text(el, f"{NPORT}pctVal"))
        lines.append((
            _id(_text(el, f"{NPORT}cusip")),
            _id((isin.get("value") or "").strip() or None) if isin is not None else None,
            _text(el, f"{NPORT}name"), _text(el, f"{NPORT}title"),
            _num(_text(el, f"{NPORT}balance")), _text(el, f"{NPORT}units"),
            _num(_text(el, f"{NPORT}valUSD")), None if pct is None else pct / 100,
            _text(el, f"{NPORT}assetCat"), _text(el, f"{NPORT}issuerCat"),
            _text(el, f"{NPORT}invCountry"), _text(el, f"{NPORT}payoffProfile"),
        ))
    return Parsed(series, dt.date.fromisoformat(rep) if rep else None, lines)


# ---------- the vendor archive ----------

def read_funds() -> list[dict]:
    if not FUNDS_FILE.exists():
        return []
    with FUNDS_FILE.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def vendor_file(series_id: str, accession: str) -> Path:
    return settings.vendor_dir / DATASET.name / series_id / f"{accession}.xml.gz"


def _index_file(series_id: str) -> Path:
    return settings.vendor_dir / DATASET.name / series_id / "filings.json"


def read_index(series_id: str) -> dict[str, dict]:
    """Return {accession: {filed, report_date}} for the filings kept in vendor/."""
    try:
        return json.loads(_index_file(series_id).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _temp_beside(path)
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


# ---------- write, audit, publish ----------

def build(fund: str, accession: str, filed: dt.date, parsed: Parsed) -> Path:
    d = parsed.report_date
    staged = settings.staging_dir / DATASET.name / f"date={d:%Y-%m-%d}" / f"{fund}.parquet"
    rows_to_parquet(
        ((fund, parsed.series_id, accession, filed, d, *line) for line in parsed.lines),
        COLUMNS, staged)
    return staged


def audit(staged: Path, fund: str, series_id: str, d: dt.date) -> int:
    n, wrong_series, no_date, wrong_date, total, early = duckdb.execute("""
        select count(*),
               count(*) filter (where series_id is distinct from ?),
               count(*) filter (where report_date is null),
               count(*) filter (where report_date is distinct from ?),
               sum(pct_val),
               count(*) filter (where filed < report_date)
        from read_parquet(?)""", [series_id, d, str(staged)]).fetchone()
    fatal = []
    if wrong_series:
        fatal.append(f"the filing is for another series than {series_id}")
    if no_date:
        fatal.append("a null report date")
    if wrong_date:
        fatal.append(f"a report date other than {d}")
    if early:
        fatal.append("a filing date before the report date")
    if n == 0 or total is None:
        fatal.append("no holdings with a pct_val")
    elif not MIN_PCT <= total <= MAX_PCT:
        fatal.append(f"pct_val sums to {total:.3f}, outside {MIN_PCT} to {MAX_PCT}")
    if fatal:
        raise AuditFailure(f"{DATASET.name} {fund} {d}: " + ". ".join(fatal) + ".")
    if n < MIN_LINES:
        log.warning("%s %s: only %s holdings.", fund, d, n)
    return n


def _earliest_by_date(series_id: str, index: dict[str, dict], since: dt.date
                      ) -> dict[dt.date, tuple[dt.date, str]]:
    """Return {report_date: (filed, accession)} of the earliest kept filing of each date."""
    best: dict[dt.date, tuple[dt.date, str]] = {}
    for acc, e in index.items():
        if not (e.get("filed") and e.get("report_date") and vendor_file(series_id, acc).exists()):
            continue
        filed, rd = dt.date.fromisoformat(e["filed"]), dt.date.fromisoformat(e["report_date"])
        if filed >= since and (rd not in best or (filed, acc) < best[rd]):
            best[rd] = (filed, acc)
    return best


def publish_fund(fund: str, series_id: str, index: dict[str, dict], since: dt.date, *,
                 rebuild: bool = False, cache: dict[str, Parsed] | None = None
                 ) -> tuple[Counter, list[str]]:
    """Publish a file for each report date of a fund that has none (all, when rebuilding)."""
    counts: Counter = Counter()
    problems: list[str] = []
    for rd, (filed, acc) in sorted(_earliest_by_date(series_id, index, since).items()):
        dest = DATASET.member_file(rd, fund)
        if dest.exists() and not rebuild:
            counts["unchanged"] += 1
            continue
        staged = None
        try:
            parsed = (cache or {}).get(acc) or parse_filing(
                gzip.decompress(vendor_file(series_id, acc).read_bytes()))
            if not parsed.lines:
                raise AuditFailure(f"{DATASET.name} {fund} {rd}: no holdings.")
            staged = build(fund, acc, filed, parsed)
            audit(staged, fund, series_id, rd)
            publish(staged, dest)
            counts["published"] += 1
        except Exception as exc:  # noqa: BLE001 -- one date must not stop the rest
            if staged is not None:
                staged.unlink(missing_ok=True)
            problems.append(f"sec_nport {fund} {rd}: {type(exc).__name__}: {exc}")
            log.error("sec_nport %s %s FAILED %s: %s", fund, rd, type(exc).__name__, exc)
    return counts, problems


def sync_series(sec: Sec, series_id: str, since: dt.date
                ) -> tuple[dict[str, dict], dict[str, Parsed], list[str]]:
    """Fetch the filings of a series that vendor/ lacks. Return its index and what it parsed."""
    index = read_index(series_id)
    cache: dict[str, Parsed] = {}
    problems: list[str] = []
    for fl in list_filings(sec, series_id, since):
        vf = vendor_file(series_id, fl.accession)
        known = index.get(fl.accession, {})
        if vf.exists() and known.get("report_date") and known.get("filed") == fl.filed.isoformat():
            continue
        try:
            data = (gzip.decompress(vf.read_bytes()) if vf.exists()
                    else sec.get(fl.url))
            if not vf.exists():
                write_gzip(data, vf)
            parsed = parse_filing(data)
            if parsed.report_date is None:
                raise AuditFailure("the filing states no report date")
        except Exception as exc:  # noqa: BLE001 -- one filing must not stop the rest
            problems.append(f"sec_nport {series_id} {fl.accession}: {type(exc).__name__}: {exc}")
            log.error("sec_nport %s %s FAILED %s: %s", series_id, fl.accession,
                      type(exc).__name__, exc)
            continue
        cache[fl.accession] = parsed
        index[fl.accession] = {"filed": fl.filed.isoformat(),
                               "report_date": parsed.report_date.isoformat()}
        _write_json(_index_file(series_id), index)
    return index, cache, problems


# ---------- the run ----------

def _listed_file() -> Path:
    return settings.data_root / "_logs" / "sec_nport_listed.json"


def _read_listed() -> dict[str, str]:
    try:
        return json.loads(_listed_file().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def ingest(funds: list[str] | None = None, since: dt.date = SINCE, *, rebuild: bool = False,
           min_age_hours: float = 0, sec: Sec | None = None,
           now: dt.datetime | None = None) -> list[str]:
    """Download, build and publish the funds' filings. Return one problem string per failure.

    Funds listed less than min_age_hours ago are left alone, so a scheduled run costs nothing
    between filing seasons. rebuild reads vendor/ only and needs no SEC_USER_AGENT.
    """
    et = eastern(now)
    seed = read_funds()
    if funds is not None:
        unknown = sorted(set(funds) - {f["fund"] for f in seed})
        if unknown:
            raise ValueError(f"Not in {FUNDS_FILE.name}: {', '.join(unknown)}.")
        seed = [f for f in seed if f["fund"] in funds]
    listed = _read_listed()
    groups: dict[str, list[str]] = {}
    for f in seed:
        tried = listed.get(f["series_id"])
        if (not rebuild and tried and min_age_hours
                and (et - dt.datetime.fromisoformat(tried)).total_seconds()
                < min_age_hours * 3600):
            continue
        groups.setdefault(f["series_id"], []).append(f["fund"])
    if not groups:
        return []
    own = None
    if not rebuild and sec is None:
        own = sec = Sec()  # Fails here, before any work, when the contact is unset.

    def one_series(item: tuple[str, list[str]]) -> tuple[Counter, list[str]]:
        series_id, names = item
        counts: Counter = Counter()
        problems: list[str] = []
        try:
            if rebuild:
                index, cache = read_index(series_id), {}
            else:
                index, cache, problems = sync_series(sec, series_id, since)
            for fund in names:
                c, p = publish_fund(fund, series_id, index, since, rebuild=rebuild, cache=cache)
                counts += c
                problems += p
            if not problems:
                listed[series_id] = et.isoformat()
        except Exception as exc:  # noqa: BLE001 -- one series must not stop the rest
            problems.append(f"sec_nport {series_id}: {type(exc).__name__}: {exc}")
            log.error("sec_nport %s FAILED %s: %s", series_id, type(exc).__name__, exc)
        return counts, problems

    try:
        with ThreadPoolExecutor(WORKERS) as pool:
            results = list(pool.map(one_series, groups.items()))
    finally:
        if own is not None:
            own.close()
    if not rebuild:
        _write_json(_listed_file(), listed)
    total: Counter = Counter()
    problems = []
    for c, p in results:
        total += c
        problems += p
    log.info("%s: %s series, %s published, %s already there, %s problems.", DATASET.name,
             len(groups), total["published"], total["unchanged"], len(problems))
    return problems


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.sec_nport",
                                description="Publish the N-PORT holdings of the seed funds.")
    p.add_argument("--funds", help="Comma-separated tickers. Default: every fund of the seed.")
    p.add_argument("--since", type=dt.date.fromisoformat, default=SINCE,
                   help="Earliest filing date (default 2021-01-01).")
    p.add_argument("--rebuild", action="store_true",
                   help="Build from vendor/ and publish again. Do not download.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        problems = ingest(args.funds.split(",") if args.funds else None, args.since,
                          rebuild=args.rebuild)
    except MissingContact as exc:
        raise SystemExit(str(exc)) from None
    print("\n".join(problems) if problems else "ok")
    raise SystemExit(1 if problems else 0)


if __name__ == "__main__":
    main()
