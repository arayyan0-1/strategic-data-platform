"""Ingest issuer holdings files for a selected list of ETFs.

    python -m sdp.ingest.etf_holdings                     # pull every fund that is due
    python -m sdp.ingest.etf_holdings --select            # add funds that widen coverage
    python -m sdp.ingest.etf_holdings --rebuild DATE FUND

Each file is published under the as-of date it states, one file per fund, so a fund that
publishes late doesn't hold the others back. The list lives in the dbt seed
etf_holdings_funds.csv. --select adds the largest candidates whose holdings overlap every
listed fund by less than OVERLAP_MAX, so coverage spreads across indexes and markets
rather than stacking S&P 500 trackers. Funds are only ever added, so no series breaks.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import io
import json
import logging
import os
import shutil
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import openpyxl

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import (
    AuditFailure,
    eastern,
    get_public,
    one,
    previous_session,
    publish,
    require_file,
    rows_to_parquet,
)
from sdp.ingest.rest import _temp_beside

log = logging.getLogger(__name__)

DATASET = dal.ETF_HOLDINGS
FUNDS_FILE = settings.seeds_dir / "etf_holdings_funds.csv"
FUND_FIELDS = ["fund", "issuer", "name", "asset_class", "holdings_url",
               "total_net_assets_bn", "max_overlap", "overlaps_with", "added_on"]

SELECT_CLASSES = ("Equity", "Fixed Income", "Real Estate")
MIN_ASSETS = 1e9
OVERLAP_MAX = 0.9
REFETCH_HOURS = 8  # A fund whose file hasn't moved is tried again after this long.
WORKERS = 6
VENDOR_KEEP_DAYS = 30

COLUMNS = {
    "date": "date", "fund": "varchar", "issuer": "varchar", "position": "integer",
    "ticker": "varchar", "name": "varchar", "sector": "varchar", "asset_class": "varchar",
    "market_value": "double", "weight": "double", "notional_value": "double",
    "quantity": "double", "price": "double", "location": "varchar", "exchange": "varchar",
    "currency": "varchar", "market_currency": "varchar", "fx_rate": "double",
    "cusip": "varchar", "isin": "varchar", "sedol": "varchar", "coupon": "double",
    "maturity": "date", "duration": "double", "ytm": "double",
    "fund_shares_outstanding": "double",
}
FIELDS = list(COLUMNS)[4:-1]  # The per-holding fields a parser fills.
NUMERIC = {k for k, v in COLUMNS.items() if v == "double"}

ISHARES_HEADERS = {
    "Ticker": "ticker", "Name": "name", "Sector": "sector", "Asset Class": "asset_class",
    "Market Value": "market_value", "Weight (%)": "weight", "Notional Value": "notional_value",
    "Quantity": "quantity", "Par Value": "quantity", "Price": "price",
    "Location": "location", "Exchange": "exchange", "Currency": "currency",
    "Market Currency": "market_currency", "FX Rate": "fx_rate", "CUSIP": "cusip",
    "ISIN": "isin", "SEDOL": "sedol", "Coupon (%)": "coupon", "Maturity": "maturity",
    "Duration": "duration", "YTM (%)": "ytm",
}
SSGA_HEADERS = {
    "Name": "name", "Ticker": "ticker", "Identifier": "identifier", "SEDOL": "sedol",
    "Weight": "weight", "Sector": "sector", "Shares Held": "quantity",
    "Par Value": "quantity", "Market Value": "market_value", "Local Currency": "currency",
    "Coupon": "coupon", "Maturity": "maturity",
}
NOT_HOLDINGS = {"Cash", "Futures", "FX", "Money Market", "Cash Collateral and Margins",
                "Cash and/or Derivatives"}


# ---------- parsing ----------

def _num(v) -> float | None:
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v or "").strip().replace(",", "")
    if s in ("", "-", "--"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _day(v) -> dt.date | None:
    if isinstance(v, dt.datetime):
        return v.date()
    s = str(v or "").strip()
    for fmt in ("%b %d, %Y", "%m/%d/%Y", "%d-%b-%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _clean(rec: dict) -> dict:
    """Convert a raw record to typed fields. Weights arrive in percent."""
    out = {}
    for k in FIELDS:
        v = rec.get(k)
        if k in NUMERIC:
            out[k] = _num(v)
        elif k == "maturity":
            out[k] = _day(v)
        else:
            s = str(v).strip() if v is not None else ""
            out[k] = None if s in ("", "-") else s
    if out["weight"] is not None:
        out["weight"] /= 100
    return out


def parse_ishares(text: str) -> tuple[dt.date | None, float | None, list[dict]]:
    """Return the as-of date, shares outstanding and holdings of an iShares CSV."""
    meta: dict[str, str] = {}
    header: list[str] | None = None
    rows = []
    for r in csv.reader(io.StringIO(text.lstrip("﻿"))):
        if header is None:
            if r and r[0] in ("Ticker", "Name") and "Weight (%)" in r:
                header = r
            elif len(r) >= 2:
                meta[r[0].strip()] = r[1].strip()
            continue
        if len(r) != len(header):
            break  # Footnotes follow the table.
        rows.append(_clean({ISHARES_HEADERS[h]: v for h, v in zip(header, r, strict=True)
                            if h in ISHARES_HEADERS}))
    return (_day(meta.get("Fund Holdings as of")), _num(meta.get("Shares Outstanding")), rows)


def parse_ssga(data: bytes) -> tuple[dt.date | None, None, list[dict]]:
    """Return the as-of date and holdings of an SSGA holdings spreadsheet."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    try:
        as_of = None
        header: list | None = None
        rows = []
        for r in wb.worksheets[0].iter_rows(values_only=True):
            first = str(r[0]).strip() if r and r[0] is not None else ""
            if header is None:
                if first == "Holdings:" and len(r) > 1:
                    as_of = _day(str(r[1]).replace("As of", "").strip())
                elif first == "Name":
                    header = [str(c).strip() if c is not None else "" for c in r]
                continue
            if not first:
                break  # A blank row ends the table; disclaimers follow.
            rec = {SSGA_HEADERS[h]: v for h, v in zip(header, r, strict=False) if h in SSGA_HEADERS}
            ident = str(rec.pop("identifier", "") or "").strip()
            if len(ident) == 12:
                rec["isin"] = ident
            elif len(ident) == 9:
                rec["cusip"] = ident
            rows.append(_clean(rec))
        return as_of, None, rows
    finally:
        wb.close()


def parse(issuer: str, data: bytes):
    if issuer == "iShares":
        return parse_ishares(data.decode("utf-8", "replace"))
    return parse_ssga(data)


# ---------- the fund list ----------

def read_funds() -> list[dict]:
    if not FUNDS_FILE.exists():
        return []
    with FUNDS_FILE.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write_funds(funds: list[dict]) -> None:
    tmp = _temp_beside(FUNDS_FILE)
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FUND_FIELDS)
        w.writeheader()
        for f in sorted(funds, key=lambda f: (f["issuer"], f["fund"])):
            w.writerow({k: f.get(k, "") for k in FUND_FIELDS})
    os.replace(tmp, FUNDS_FILE)


def holdings_url(issuer: str, fund: str, product_url: str | None) -> str | None:
    if issuer == "iShares":
        return f"{settings.ishares_base_url}{product_url}/latest-holdings.csv" \
            if product_url else None
    return f"{settings.ssga_fund_data_url}/holdings-daily-us-en-{fund.lower()}.xlsx"


# ---------- write, audit, publish ----------

def vendor_file(d: dt.date, fund: str, issuer: str) -> Path:
    suffix = ".csv.gz" if issuer == "iShares" else ".xlsx"
    return settings.vendor_dir / DATASET.name / f"{d:%Y-%m-%d}" / f"{fund}{suffix}"


def _vendor_bytes(path: Path) -> bytes:
    data = path.read_bytes()
    return gzip.decompress(data) if path.suffix == ".gz" else data


def build(fund: str, issuer: str, d: dt.date, so: float | None, rows: list[dict]) -> Path:
    staged = settings.staging_dir / DATASET.name / f"{d:%Y-%m-%d}" / f"{fund}.parquet"
    rows_to_parquet(
        ((d, fund, issuer, i, *(r[k] for k in FIELDS), so) for i, r in enumerate(rows, 1)),
        COLUMNS, staged)
    return staged


def audit(staged: Path, fund: str, d: dt.date, today: dt.date) -> int:
    n, no_weight, total = one(f"""
        select count(*), count(*) filter (where weight is null), sum(weight)
        from read_parquet('{staged}')""")
    fatal = []
    if n == 0:
        fatal.append("no holdings")
    elif no_weight == n:
        fatal.append("no weights")
    if d > today:
        fatal.append(f"an as-of date {d} after today {today}")
    if fatal:
        raise AuditFailure(f"{DATASET.name} {fund} {d}: " + ". ".join(fatal) + ".")
    if total is not None and not 0.8 <= total <= 1.2:
        log.info("%s %s: the weights sum to %.3f.", fund, d, total)
    return n


def _publish(fund: str, issuer: str, data: bytes, today: dt.date) -> tuple[str, dt.date | None]:
    """Parse one downloaded file and publish it unless its as-of date is already in."""
    d, so, rows = parse(issuer, data)
    if d is None:
        raise AuditFailure(f"{DATASET.name} {fund}: the file states no as-of date.")
    dest = DATASET.member_file(d, fund)
    if dest.exists():
        return "unchanged", d
    vf = vendor_file(d, fund, issuer)
    vf.parent.mkdir(parents=True, exist_ok=True)
    tmp = _temp_beside(vf)
    tmp.write_bytes(gzip.compress(data) if vf.suffix == ".gz" else data)
    os.replace(tmp, vf)
    staged = build(fund, issuer, d, so, rows)
    try:
        audit(staged, fund, d, today)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    publish(staged, dest)
    return "published", d


def newest_dates() -> dict[str, dt.date]:
    """Return the newest published as-of date of each fund."""
    out: dict[str, dt.date] = {}
    for d in dal.partitions(DATASET):
        for f in DATASET.files(d):
            out[f.stem] = max(d, out.get(f.stem, d))
    return out


def _state_file() -> Path:
    return settings.data_root / "_logs" / "etf_holdings_fetch.json"


def _read_state() -> dict[str, str]:
    try:
        return json.loads(_state_file().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _write_state(state: dict[str, str]) -> None:
    p = _state_file()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = _temp_beside(p)
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
    os.replace(tmp, p)


def due_funds(now: dt.datetime | None = None) -> list[dict]:
    """Return the listed funds whose newest file is older than the last session, skipping
    any tried within REFETCH_HOURS."""
    et = eastern(now)
    expected = previous_session(et.date())
    newest = newest_dates()
    state = _read_state()
    out = []
    for f in read_funds():
        if newest.get(f["fund"], dt.date.min) >= expected:
            continue
        tried = state.get(f["fund"])
        if tried and (et - dt.datetime.fromisoformat(tried)).total_seconds() < REFETCH_HOURS * 3600:
            continue
        out.append(f)
    return out


def ingest(now: dt.datetime | None = None) -> list[str]:
    """Pull and publish every due fund. Return one problem string per failed fund."""
    et = eastern(now)
    funds = due_funds(now)
    if not funds:
        return []
    state = _read_state()
    problems: list[str] = []
    counts = {"published": 0, "unchanged": 0}

    def one_fund(f: dict) -> None:
        try:
            status, d = _publish(f["fund"], f["issuer"], get_public(f["holdings_url"], 3),
                                 et.date())
            counts[status] += 1
        except Exception as exc:  # noqa: BLE001 -- one fund must not stop the rest
            problems.append(f"etf_holdings {f['fund']}: {type(exc).__name__}: {exc}")
            log.error("etf_holdings %s FAILED %s: %s", f["fund"], type(exc).__name__, exc)
        state[f["fund"]] = et.isoformat()

    with ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(one_fund, funds))
    _write_state(state)
    log.info("%s: %s funds tried, %s published, %s not yet updated, %s failed.",
             DATASET.name, len(funds), counts["published"], counts["unchanged"], len(problems))
    thin_vendor(et.date())
    return problems


def thin_vendor(today: dt.date, keep_days: int = VENDOR_KEEP_DAYS) -> list[Path]:
    """Delete vendor dates older than keep_days except the last date of each month.

    Holdings files are never restated, so the published Parquet is the record; month-end
    vendor files stay so a parser fix can still be replayed monthly.
    """
    root = settings.vendor_dir / DATASET.name
    dates = []
    for p in sorted(root.glob("????-??-??")) if root.exists() else []:
        try:
            dates.append((dt.date.fromisoformat(p.name), p))
        except ValueError:
            continue
    month_last: dict[tuple[int, int], dt.date] = {}
    for d, _ in dates:
        month_last[(d.year, d.month)] = d
    doomed = [p for d, p in dates
              if (today - d).days > keep_days and month_last[(d.year, d.month)] != d]
    for p in doomed:
        shutil.rmtree(p)
        log.info("%s: deleted %s.", DATASET.name, p)
    return doomed


def rebuild(d: dt.date, fund: str) -> Path:
    """Build one fund's file of one date again from vendor/, with no network."""
    issuer = next((f["issuer"] for f in read_funds() if f["fund"] == fund), None)
    if issuer is None:
        raise ValueError(f"{fund} is not in {FUNDS_FILE.name}.")
    vf = require_file(vendor_file(d, fund, issuer))
    as_of, so, rows = parse(issuer, _vendor_bytes(vf))
    staged = build(fund, issuer, d, so, rows)
    audit(staged, fund, d, eastern().date())
    return publish(staged, DATASET.member_file(d, fund))


# ---------- selection ----------

def _key(r: dict) -> str | None:
    """Identify a holding across funds and issuers: the ticker for a stock, else ISIN, else
    CUSIP. iShares stock files carry no identifier, and bond files no ticker."""
    if (r.get("asset_class") in NOT_HOLDINGS or r.get("sector") in NOT_HOLDINGS
            or (r.get("weight") or 0) <= 0):
        return None
    t = r.get("ticker")
    if t:
        return f"T:{t.replace(' ', '.').replace('/', '.')}"
    return r.get("isin") or r.get("cusip")


def weights(rows: Iterable[dict]) -> dict[str, float]:
    """Return holding weights keyed by _key and normalized to sum to 1."""
    w: dict[str, float] = {}
    for r in rows:
        k = _key(r)
        if k:
            w[k] = w.get(k, 0.0) + r["weight"]
    total = sum(w.values())
    return {k: v / total for k, v in w.items()} if total > 0 else {}


def overlap(a: dict[str, float], b: dict[str, float]) -> float:
    """Return the weight two portfolios hold in common: the sum of min(w_a, w_b)."""
    if len(a) > len(b):
        a, b = b, a
    return sum(min(v, b[k]) for k, v in a.items() if k in b)


def candidates() -> list[dict]:
    """Return unlisted funds from the newest screener snapshot, largest first."""
    parts = dal.partitions(dal.ETF_FUNDS)
    if not parts:
        raise FileNotFoundError("etf_funds has no snapshot. Run python -m sdp.ingest.etf_funds.")
    listed = {f["fund"] for f in read_funds()}
    classes = ", ".join(f"'{c}'" for c in SELECT_CLASSES)
    rows = dal.on_date(dal.ETF_FUNDS, parts[-1]).filter(
        f"asset_class in ({classes}) and total_net_assets >= {MIN_ASSETS}"
    ).order("total_net_assets desc").project(
        "issuer, fund, name, asset_class, total_net_assets, product_url").fetchall()
    out = []
    for issuer, fund, name, cls, tna, url in rows:
        if fund in listed:
            continue
        hu = holdings_url(issuer, fund, url)
        if hu:
            out.append({"issuer": issuer, "fund": fund, "name": name, "asset_class": cls,
                        "total_net_assets": tna, "holdings_url": hu})
    return out


def select(now: dt.datetime | None = None) -> list[dict]:
    """Add to the fund list every candidate that widens coverage; publish its first file.

    Return the added funds.
    """
    today = eastern(now).date()
    funds = read_funds()
    newest = newest_dates()
    chosen: dict[str, dict[str, float]] = {}
    for f in funds:
        d = newest.get(f["fund"])
        if d is not None:
            rows = dal.on_date(DATASET, d).filter(f"fund = '{f['fund']}'") \
                .project(", ".join(COLUMNS)).fetchall()
            cols = list(COLUMNS)
            chosen[f["fund"]] = weights(dict(zip(cols, r, strict=True)) for r in rows)
    cands = candidates()
    log.info("Downloading the holdings of %s candidates.", len(cands))

    def download(c: dict):
        try:
            return c, get_public(c["holdings_url"], 3)
        except Exception as exc:  # noqa: BLE001 -- a missing file just drops the candidate
            log.info("%s: no holdings file (%s).", c["fund"], exc)
            return c, None

    with ThreadPoolExecutor(WORKERS) as pool:
        downloads = list(pool.map(download, cands))

    added = []
    for c, data in downloads:
        if data is None:
            continue
        try:
            _, _, rows = parse(c["issuer"], data)
        except Exception as exc:  # noqa: BLE001 -- an unreadable file drops the candidate
            log.info("%s: unreadable holdings (%s).", c["fund"], exc)
            continue
        w = weights(rows)
        if len(w) < 5:
            continue  # Too few holdings to say anything about coverage.
        best, best_fund = 0.0, ""
        for fund, cw in chosen.items():
            o = overlap(w, cw)
            if o > best:
                best, best_fund = o, fund
        if best >= OVERLAP_MAX:
            log.info("%s overlaps %s by %.2f. Skipped.", c["fund"], best_fund, best)
            continue
        chosen[c["fund"]] = w
        row = {"fund": c["fund"], "issuer": c["issuer"], "name": c["name"],
               "asset_class": c["asset_class"], "holdings_url": c["holdings_url"],
               "total_net_assets_bn": f"{c['total_net_assets'] / 1e9:.1f}",
               "max_overlap": f"{best:.2f}", "overlaps_with": best_fund,
               "added_on": today.isoformat()}
        funds.append(row)
        added.append(row)
        try:
            _publish(c["fund"], c["issuer"], data, today)
        except Exception as exc:  # noqa: BLE001 -- the daily run tries again
            log.warning("%s: the first file did not publish (%s).", c["fund"], exc)
    write_funds(funds)
    log.info("Added %s funds; the list has %s.", len(added), len(funds))
    return added


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.etf_holdings",
                                description="Publish ETF holdings files.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--select", action="store_true",
                      help="Add funds whose holdings widen coverage, then publish them.")
    mode.add_argument("--rebuild", nargs=2, metavar=("DATE", "FUND"),
                      help="Build one fund's file again from vendor/. Do not download.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.select:
        print(f"added {len(select())} funds")
    elif args.rebuild:
        print(rebuild(dt.date.fromisoformat(args.rebuild[0]), args.rebuild[1]))
    else:
        problems = ingest()
        print("\n".join(problems) if problems else "ok")


if __name__ == "__main__":
    main()
