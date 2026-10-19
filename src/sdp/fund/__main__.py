"""Command line for the paper fund: backtests on development or, once, on the holdout, and the
set-and-forget weights."""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import sys
from concurrent.futures import ProcessPoolExecutor

import numpy as np

from sdp import dal
from sdp.config import settings
from sdp.fund import allocation, alpha, backtest
from sdp.fund.book import RULES
from sdp.fund.covariance import METHODS
from sdp.fund.data import BENCHMARK, load, load_factors, load_pieces
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


# The parts of the forecast each --alpha choice turns on. cross: the momentum and value score;
# trend: the trend view on cash; hold: the holdings forecast; prior: the equilibrium prior of
# the non-equity funds; asset_trend: their trend views where the gate passed.
ALPHAS = {
    "none": {},
    "cross": {"cross": True},
    "trend": {"trend": True},
    "both": {"cross": True, "trend": True},
    "hold": {"hold": True},
    "all": {"cross": True, "trend": True, "hold": True},
    "mom": {"cross": True, "signal": "mom"},
    # Every component that passed its gate: trend and the holdings forecast.
    "headline": {"trend": True, "hold": True},
    "ma_prior": {"hold": True, "prior": True},
    "ma_trend": {"hold": True, "prior": True, "asset_trend": True},
}
ROLES = {"equity": ("equity", "cash"),
         "all": ("equity", "bond", "gold", "commodity", "bitcoin", "cash")}
DEV_END = dt.date(2021, 12, 31)


def _forecast(wh, args):
    parts = ALPHAS[args.alpha]
    if not parts and not args.trend_rule:
        return None
    f = alpha.load(wh, DEV_END, signal=parts.get("signal", "combo"),
                   cross=parts.get("cross", False), trend=parts.get("trend", False),
                   trend_rule=args.trend_rule)
    if parts.get("hold"):
        f.hold = alpha.load_hold(wh)
    if parts.get("prior"):
        f.premium = args.premium
    if parts.get("asset_trend"):
        f.gate, f.fund_key = alpha.load_assets(wh)
        f.asset_trend = True
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


def _arrays(x):
    if isinstance(x, dict):
        return {k: _arrays(v) for k, v in x.items()}
    if isinstance(x, list | tuple):
        return [_arrays(v) for v in x]
    if isinstance(x, np.ndarray):
        return x.tolist()
    if isinstance(x, np.floating | np.integer):
        return x.item()
    return x


# Futures funds earn a roll the spot history does not hold, and it changes by decade.
UNIVERSES = {"no_futures": ("stock", "bond", "cash", "metal", "bitcoin"),
             "all": ("stock", "bond", "cash", "metal", "commodity", "bitcoin")}


def allocate(args) -> int:
    """Find the fixed weights in a process pool: the ranges, the robustness checks and the
    walk-forward. Save one JSON."""
    wh = dal.warehouse()
    try:
        h, meta = load_pieces(wh)
        h90, _ = load_pieces(wh, dt.date(1990, 1, 31))
    finally:
        wh.close()
    kinds = UNIVERSES[args.universe]
    allowed = np.array([k in kinds for k in h.kinds])
    sigmas = allocation.SIGMAS if not args.quick else (0.04, 0.08, 0.12, 0.16)
    rules = {a: ("yearly", "never") for a in allocation.ASSUMPTIONS}
    base = (args.seed, args.paths, args.years, args.block)
    levels = (1.0, 2 / 3, 1 / 3)
    drift_limits = ["min", *sigmas, None, *(("level", f) for f in levels)]
    one = {"assumptions": ("blend",), "rules_for": {"blend": ("yearly",)}, "sigmas": (),
           "allowed": allowed}
    print(f"{len(h.names)} pieces, {int(allowed.sum())} held ({args.universe}), "
          f"{h.months[0]} to {h.months[-1]}, {args.paths} paths of {args.years} years, mean "
          f"block {args.block:g} months, {args.workers} workers", flush=True)
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        main_jobs = {a: pool.submit(allocation.run_ranges, h, *base, assumptions=(a,),
                                    rules_for=rules, sigmas=sigmas, allowed=allowed)
                     for a in allocation.ASSUMPTIONS}
        variants = {f"cost_{c}": (dataclasses.replace(h, gap=meta["gaps"][c]), {})
                    for c in ("good", "bad")}
        variants |= {f"block_{b:g}": (h, {"block": b}) for b in (12.0, 48.0)}
        variants |= {"horizon_10": (h, {"years": 10}), "compounded_vol": (h, {"log_vol": False}),
                     "from_1990": (h90, {})}
        robust_jobs = {}
        for k, (hist, over) in variants.items():
            seed, paths, years, block = base
            robust_jobs[k] = pool.submit(allocation.run_ranges, hist, seed, paths,
                                         over.get("years", years), over.get("block", block),
                                         log_vol=over.get("log_vol", True), **one)
        drift_jobs = [pool.submit(allocation.drift_points, h, *base, "blend", [lim],
                                  allowed=allowed) for lim in drift_limits]
        wf_jobs = [] if args.skip_walk_forward else [
            pool.submit(allocation.walk_forward, h, years=(y,), seed=args.seed + y,
                        allowed_kinds=kinds)
            for y in range(1992, 2017, 2)]
        main_ranges, prem = {}, None
        for a, job in main_jobs.items():
            prem, r = job.result()
            main_ranges |= r
            lv = r[a]["levels"]["yearly"][0]
            top = sorted(zip(lv["weights"], h.names, strict=True), reverse=True)[:6]
            print(f"{a:8s} VT risk {r[a]['vt_risk']:.1%}: growth {lv['growth_median']:.2%} "
                  f"against VT {r[a]['references']['vt']['growth_median']:.2%}; "
                  + ", ".join(f"{n} {w:.0%}" for w, n in top if w >= 0.005), flush=True)
        drift = [job.result()[0] for job in drift_jobs]
        main_ranges["blend"]["frontiers"]["drift"] = [d for d in drift if "fraction" not in d]
        main_ranges["blend"]["levels"]["drift"] = [d for d in drift if "fraction" in d]
        lv = main_ranges["blend"]["levels"]["drift"][0]
        print(f"drift    growth {lv['growth_median']:.2%} at {lv['risk']:.1%}; "
              f"{sum(not d['converged'] for d in drift)} of {len(drift)} points stopped at the "
              f"step limit or failed", flush=True)
        robust = {}
        for k, job in robust_jobs.items():
            robust[k] = job.result()[1]
            lv = robust[k]["blend"]["levels"]["yearly"][0]
            print(f"{k:14s} growth {lv['growth_median']:.2%} at "
                  f"{robust[k]['blend']['vt_risk']:.1%}", flush=True)
        wf = [row for job in wf_jobs for row in job.result()]
    out = {"pieces": h.names, "labels": meta["labels"], "funds": meta["funds"],
           "kinds": h.kinds, "allowed": allowed, "gaps": meta["gaps"],
           "first_month": h.months[0], "last_month": h.months[-1],
           "own_months": h.own_months(),
           "premia": {k: getattr(prem, k) for k in ("history", "equal", "blend", "sigma",
                                                     "years", "s", "tau")},
           "settings": vars(args), "ranges": main_ranges, "robustness": robust,
           "walk_forward": wf}
    folder = settings.data_root / "fund" / "allocation"
    folder.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    path = folder / f"allocation-{args.universe}-{stamp}.json"
    path.write_text(json.dumps(_arrays(out), default=_json))
    print(f"Saved {path}")
    return 0


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
                    help="the forecast: none (fees only), cross, trend, both, hold, all, "
                         "headline (the components that passed their gates), mom "
                         "(momentum alone, chosen after the long-history results), ma_prior "
                         "or ma_trend (the multi-asset forecasts)")
    bt.add_argument("--roles", default="equity", choices=tuple(ROLES),
                    help="equity (equity and cash funds) or all (also bond, gold, commodity "
                         "and bitcoin funds); only the te_budget objectives use it")
    bt.add_argument("--premium", type=float, default=0.05,
                    help="the equity premium of the equilibrium prior, a year")
    bt.add_argument("--trend-rule", type=float, default=0.0,
                    help="hold this much cash while ACWI's 12-month trend is negative")
    al = sub.add_parser("allocate", help="Find fixed weights to hold for the long run.")
    al.add_argument("--paths", type=int, default=2000)
    al.add_argument("--years", type=int, default=20)
    al.add_argument("--block", type=float, default=24.0, help="mean block length, months")
    al.add_argument("--seed", type=int, default=20261014)
    al.add_argument("--quick", action="store_true", help="four risk limits only")
    al.add_argument("--skip-walk-forward", action="store_true")
    al.add_argument("--universe", default="no_futures", choices=tuple(UNIVERSES),
                    help="no_futures holds stock, bond, cash, metal and bitcoin funds; all also "
                         "the futures-based commodity funds")
    al.add_argument("--workers", type=int, default=6)
    args = ap.parse_args(argv)
    if args.command == "allocate":
        return allocate(args)

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
          f"trend rule {args.trend_rule:.0%}, roles {args.roles}, TE cap {args.te_max:.0%}")
    print(" ".join(f"{h:{w}}" for _, h, w, _ in COLUMNS))
    out = []
    for name in names:
        cfg = backtest.Config(strategy=name, covariance=args.covariance, cap=args.cap,
                              rule=args.rule, risk_aversion=args.risk_aversion,
                              capital=args.capital, min_trade=args.band,
                              budget=Budget(te_max=args.te_max, risk_aversion=args.te_aversion,
                                            cash_max=args.cash_max),
                              roles=ROLES[args.roles])
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
                                 "covariance": args.covariance, "alpha": args.alpha,
                                 "trend_rule": args.trend_rule}) + "\n")
    print(f"Saved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
