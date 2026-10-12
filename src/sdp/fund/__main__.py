"""Command line for the paper fund: backtests on development or, once, on the holdout."""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import sys

import numpy as np

from sdp import dal
from sdp.config import settings
from sdp.fund import alpha, backtest
from sdp.fund.book import RULES
from sdp.fund.covariance import METHODS
from sdp.fund.data import BENCHMARK, load, load_factors
from sdp.fund.objectives import STRATEGIES
from sdp.fund.optimizer import Budget

# (summary key, header, width, format) of each printed column.
COLUMNS = (("strategy", "strategy", "<20", ""), ("return", "ret", ">7", ".1%"),
           ("vol", "vol", ">6", ".1%"), ("sharpe", "sharpe", ">6", ".2f"),
           ("max_drawdown", "maxdd", ">7", ".1%"), ("active_return", "active", ">7", ".2%"),
           ("tracking_error", "TE", ">6", ".2%"), ("info_ratio", "IR", ">6", ".2f"),
           ("turnover", "turn", ">5", ".2f"), ("cost_bp", "cost", ">5", ".1f"),
           ("fee_bp", "fee", ">5", ".1f"), ("names", "names", ">5", ".1f"),
           ("locked", "lock", ">5", ".1f"), ("te_bias", "bias", ">5", ".2f"),
           ("cash", "cash", ">5", ".0%"), ("failed", "fail", ">4", ""))


# Which parts of the forecast each --alpha choice turns on: (cross, trend, holdings, signal).
ALPHAS = {
    "none": (False, False, False, "combo"),
    "cross": (True, False, False, "combo"),
    "trend": (False, True, False, "combo"),
    "both": (True, True, False, "combo"),
    "hold": (False, False, True, "combo"),
    "all": (True, True, True, "combo"),
    "mom": (True, False, False, "mom"),
}
DEV_END = dt.date(2021, 12, 31)


def _forecast(wh, args):
    cross, trend, hold, signal = ALPHAS[args.alpha]
    if args.alpha == "none" and not args.trend_rule:
        return None
    f = alpha.load(wh, DEV_END, signal=signal, cross=cross, trend=trend,
                   trend_rule=args.trend_rule)
    if hold:
        f.hold = alpha.load_hold(wh)
    return alpha.source(f)


def _json(x):
    if isinstance(x, dt.date):
        return x.isoformat()
    if isinstance(x, np.generic):
        return x.item()
    if isinstance(x, np.ndarray):
        return x.tolist()
    raise TypeError(type(x))


def _period(p, wh, args) -> tuple[dt.date, dt.date]:
    first_fit = wh.execute("select min(fit_date) from factors.fund_exposures").fetchone()[0]
    if args.period == "development":
        end = max(d for d in p.dates if d < p.holdout_start)
        return first_fit, end
    if not args.read_holdout:
        sys.exit("The holdout is read once, for the strategies the development run chose. "
                 "Add --read-holdout to read it.")
    start = max(d for d in p.dates if d < p.holdout_start)
    return start, p.dates[-1]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m sdp.fund", description=__doc__)
    sub = ap.add_subparsers(dest="command", required=True)
    bt = sub.add_parser("backtest", help="Run strategies and print their results.")
    bt.add_argument("--strategies", default="all",
                    help=f"Comma-separated, or all. Known: {', '.join(STRATEGIES)}.")
    bt.add_argument("--covariance", default="qis", choices=METHODS)
    bt.add_argument("--period", default="development", choices=("development", "holdout"))
    bt.add_argument("--read-holdout", action="store_true")
    bt.add_argument("--benchmark", default=BENCHMARK)
    bt.add_argument("--cap", type=float, default=1.0)
    bt.add_argument("--rule", default="last_buy", choices=RULES)
    bt.add_argument("--risk-aversion", type=float, default=100.0)
    bt.add_argument("--capital", type=float, default=100_000.0)
    bt.add_argument("--te-max", type=float, default=0.03, help="te_budget: tracking-error cap")
    bt.add_argument("--te-aversion", type=float, default=5.0,
                    help="te_budget: risk aversion on annual tracking variance")
    bt.add_argument("--cash-max", type=float, default=0.30)
    bt.add_argument("--band", type=float, default=0.005,
                    help="no-trade band: a trade under this share of NAV is skipped")
    bt.add_argument("--alpha", default="none", choices=tuple(ALPHAS),
                    help="the forecast: none (fees only), cross, trend, both, hold, all, or "
                         "mom (momentum alone, chosen after the long-history results)")
    bt.add_argument("--trend-rule", type=float, default=0.0,
                    help="hold this much cash while ACWI's 12-month trend is negative")
    args = ap.parse_args(argv)

    names = list(STRATEGIES) if args.strategies == "all" else args.strategies.split(",")
    wh = dal.warehouse()
    try:
        p = load(wh, args.benchmark)
        start, end = _period(p, wh, args)
        fd = load_factors(wh, p.keys) if args.covariance == "factor" else None
        source = _forecast(wh, args)
    finally:
        wh.close()
    rows = backtest.decision_rows(p, start, end)
    print(f"{args.period}: {len(rows)} decisions from {p.dates[rows[0]]} to "
          f"{p.dates[rows[-1]]}, returns to {end}, benchmark {args.benchmark}, "
          f"covariance {args.covariance}, rule {args.rule}, cap {args.cap:.0%}, "
          f"risk aversion {args.risk_aversion:g}, alpha {args.alpha}, "
          f"trend rule {args.trend_rule:.0%}")
    print(" ".join(f"{h:{w}}" for _, h, w, _ in COLUMNS))
    out = []
    for name in names:
        cfg = backtest.Config(strategy=name, covariance=args.covariance, cap=args.cap,
                              rule=args.rule, risk_aversion=args.risk_aversion,
                              capital=args.capital, min_trade=args.band,
                              budget=Budget(te_max=args.te_max, risk_aversion=args.te_aversion,
                                            cash_max=args.cash_max))
        r = backtest.run(p, cfg, start, end, fd, source)
        s = backtest.summary(r)
        print(" ".join(f"{s[k]:{w}{f}}" for k, _, w, f in COLUMNS), flush=True)
        out.append({"summary": s, "config": dataclasses.asdict(cfg),
                    "dates": r.dates, "nav": r.nav, "bench": r.bench,
                    "rebalances": r.rebalances})
    folder = settings.data_root / "fund" / "backtests"
    folder.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    path = folder / f"{args.period}-{args.covariance}-{args.alpha}-{stamp}.json"
    path.write_text(json.dumps(out, default=_json))
    if args.period == "holdout":
        with (settings.data_root / "fund" / "holdout_reads.jsonl").open("a") as fh:
            fh.write(json.dumps({"time": stamp, "strategies": names,
                                 "covariance": args.covariance}) + "\n")
    print(f"Saved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
