"""Ingest fund-level data for every iShares and SSGA US ETF from the issuers' screeners.

    python -m sdp.ingest.etf_funds            # pull if today's snapshot is missing
    python -m sdp.ingest.etf_funds --rebuild DATE

One snapshot per US Eastern date: NAV, net assets, their as-of dates, asset class and,
for SSGA, the benchmark, close and premium. Shares outstanding is net assets over NAV.
The screeners keep no history, so a day the machine is off is lost for iShares; SSGA's
own history fills it (etf_nav_history).
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import logging
import re
from pathlib import Path

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import (
    AuditFailure,
    eastern,
    get_public,
    one,
    publish,
    require_file,
    rows_to_parquet,
    write_gzip,
)

log = logging.getLogger(__name__)

DATASET = dal.ETF_FUNDS
MIN_ISHARES, MIN_SSGA = 300, 100

COLUMNS = {
    "date": "date", "issuer": "varchar", "fund": "varchar", "name": "varchar",
    "asset_class": "varchar", "sub_asset_class": "varchar", "market_type": "varchar",
    "region": "varchar", "country": "varchar", "benchmark": "varchar",
    "inception_date": "date", "expense_ratio": "double", "nav": "double", "nav_date": "date",
    "total_net_assets": "double", "assets_date": "date", "close_price": "double",
    "premium_discount": "double", "cusip": "varchar", "isin": "varchar",
    "portfolio_id": "bigint", "product_url": "varchar",
}

# SSGA's asset-class tree, mapped to the iShares names so one column serves both.
SSGA_CLASSES = {"Equity": "Equity", "Fixed Income Sector": "Fixed Income",
                "Multi-Asset": "Multi Asset", "Cash": "Cash"}
SSGA_ALTERNATIVES = {"Commodities & CTAs": "Commodity", "Public Real Estate": "Real Estate",
                     "Infrastructure": "Equity"}


def pull_dir(d: dt.date) -> Path:
    return settings.vendor_dir / DATASET.name / f"{d:%Y-%m-%d}"


def fetch(d: dt.date) -> Path:
    """Download both screeners into the vendor directory of d."""
    out = pull_dir(d)
    write_gzip(get_public(settings.ishares_screener_url), out / "ishares.json.gz")
    write_gzip(get_public(settings.ssga_fundfinder_url), out / "ssga.json.gz")
    return out


def _r(v):
    """Return the raw value of an iShares {"d": display, "r": raw} field, else None."""
    return v.get("r") if isinstance(v, dict) else None


def _yyyymmdd(v) -> dt.date | None:
    r = _r(v)
    return dt.datetime.strptime(str(int(r)), "%Y%m%d").date() if r else None


def _text(v) -> str | None:
    return v.strip() if isinstance(v, str) and v.strip() not in ("", "-") else None


def parse_ishares(doc: dict, d: dt.date) -> list[tuple]:
    rows = []
    for p in doc.values():
        if "etf" not in (p.get("productView") or []):
            continue  # The screener also lists mutual funds.
        rows.append((
            d, "iShares", _text(p.get("localExchangeTicker")), _text(p.get("fundName")),
            _text(p.get("aladdinAssetClass")), _text(p.get("aladdinSubAssetClass")),
            _text(p.get("aladdinMarketType")), _text(p.get("aladdinRegion")),
            _text(p.get("aladdinCountry")), None, _yyyymmdd(p.get("inceptionDate")),
            _r(p.get("fees")), _r(p.get("navAmount")), _yyyymmdd(p.get("navAmountAsOf")),
            _r(p.get("totalNetAssetsFund")), _yyyymmdd(p.get("totalNetAssetsFundAsOf")),
            None, None, _text(p.get("cusip")), _text(p.get("isin")), p.get("portfolioId"),
            _text(p.get("productPageUrl")),
        ))
    return rows


def _pair(v):
    """Return the raw value of an SSGA [display, raw] field; a display of "-" is None."""
    if not isinstance(v, list) or len(v) < 2 or str(v[0]).strip() in ("", "-"):
        return None
    return v[1]


def _ssga_tree(categories: list) -> dict[str, dict[str, str]]:
    """Map each SSGA ticker to its asset class, sub-class, market type and region."""
    out: dict[str, dict[str, str]] = {}

    def leaves(node):
        subs = node.get("subCategories") or []
        if not subs:
            yield node
        for s in subs:
            yield from leaves(s)

    def tag(funds: str, field: str, value: str) -> None:
        for t in funds.split("|"):
            if t:
                out.setdefault(t, {}).setdefault(field, value)

    for cat in categories:
        if cat.get("key") == "assetclass":
            for top in cat.get("subCategories") or []:
                name = top.get("name", "")
                for leaf in leaves(top):
                    cls = SSGA_ALTERNATIVES.get(leaf.get("name", ""), SSGA_CLASSES.get(name))
                    tag(leaf.get("funds", ""), "asset_class", cls or name)
                    if leaf is not top and leaf.get("name"):
                        tag(leaf.get("funds", ""), "sub_asset_class", leaf["name"])
        elif cat.get("key") == "geographical-exposure":
            for leaf in leaves(cat):
                if leaf.get("name", "").endswith("Markets") or leaf.get("name") == "All (Broad)":
                    tag(leaf.get("funds", ""), "market_type", leaf["name"])
                elif leaf.get("name"):
                    tag(leaf.get("funds", ""), "region", leaf["name"])
    return out


def _ticker(v) -> str | None:
    t = re.sub(r"[^A-Z0-9.]", "", str(v or "").upper())
    return t or None


def parse_ssga(doc: dict, d: dt.date) -> list[tuple]:
    etfs = doc["data"]["funds"]["etfs"]
    tree = _ssga_tree(etfs.get("categories") or [])
    rows = []
    for f in etfs["datas"]:
        fund = _ticker(f.get("fundTicker"))
        info = tree.get(fund or "", {})
        aum = _pair(f.get("aum"))
        as_of = _pair(f.get("asOfDate"))
        inception = _pair(f.get("inceptionDate"))
        bench = (f.get("perfIndex") or [{}])[0].get("glossary")
        rows.append((
            d, "SSGA", fund, (f.get("fundName") or "").replace("®", "").strip() or None,
            info.get("asset_class"), info.get("sub_asset_class"), info.get("market_type"),
            info.get("region"), None, _text(bench),
            dt.date.fromisoformat(inception) if inception else None,
            _pair(f.get("ter")), _pair(f.get("nav")),
            dt.date.fromisoformat(as_of) if as_of else None,
            aum * 1e6 if aum is not None else None,  # SSGA states assets in millions.
            dt.date.fromisoformat(as_of) if as_of else None,
            _pair(f.get("closePrice")), _pair(f.get("premiumDiscount")),
            None, None, None, _text(f.get("fundUri")),
        ))
    return rows


def build(src: Path, d: dt.date) -> Path:
    """Parse both screeners of one pull into staged Parquet."""
    ishares = json.loads(gzip.decompress((src / "ishares.json.gz").read_bytes()))
    ssga = json.loads(gzip.decompress((src / "ssga.json.gz").read_bytes()))
    staged = settings.staging_dir / DATASET.name / f"{d:%Y-%m-%d}.parquet"
    rows_to_parquet(parse_ishares(ishares, d) + parse_ssga(ssga, d), COLUMNS, staged)
    return staged


def audit(staged: Path, d: dt.date) -> int:
    n, n_ish, n_ssga, null_fund, dups, bad_nav, no_nav = one(f"""
        select count(*), count(*) filter (where issuer = 'iShares'),
               count(*) filter (where issuer = 'SSGA'), count(*) filter (where fund is null),
               count(*) - count(distinct (issuer, fund)),
               count(*) filter (where nav <= 0), count(*) filter (where nav is null)
        from read_parquet('{staged}')""")
    fatal = []
    if n_ish < MIN_ISHARES:
        fatal.append(f"{n_ish} iShares funds. The minimum is {MIN_ISHARES}")
    if n_ssga < MIN_SSGA:
        fatal.append(f"{n_ssga} SSGA funds. The minimum is {MIN_SSGA}")
    if null_fund:
        fatal.append(f"{null_fund} rows with no ticker")
    if dups:
        fatal.append(f"{dups} duplicate (issuer, fund) rows")
    if bad_nav:
        fatal.append(f"{bad_nav} NAVs at or below zero")
    if no_nav > n * 0.1:
        fatal.append(f"{no_nav} of {n} funds have no NAV")
    if fatal:
        raise AuditFailure(f"{DATASET.name} {d}: " + ". ".join(fatal) + ".")
    return n


def _publish(src: Path, d: dt.date) -> Path:
    staged = build(src, d)
    try:
        n = audit(staged, d)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    path = publish(staged, DATASET.partition_file(d))
    log.info("%s %s: %s funds.", DATASET.name, d, n)
    return path


def ingest(now: dt.datetime | None = None) -> Path:
    """Publish today's snapshot (US Eastern date) unless it is already published."""
    d = eastern(now).date()
    dest = DATASET.partition_file(d)
    if dest.exists():
        return dest
    return _publish(fetch(d), d)


def rebuild(d: dt.date) -> Path:
    """Build one pull again from its vendor files, with no network."""
    src = pull_dir(d)
    require_file(src / "ishares.json.gz")
    return _publish(src, d)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.etf_funds",
                                description="Publish today's ETF screener snapshot.")
    p.add_argument("--rebuild", type=dt.date.fromisoformat, metavar="DATE",
                   help="Build DATE again from its vendor files. Do not download.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(rebuild(args.rebuild) if args.rebuild else ingest())


if __name__ == "__main__":
    main()
