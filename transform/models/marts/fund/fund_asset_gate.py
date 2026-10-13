"""Gate for the 12-month trend: does its sign order the next month's excess return of an asset."""
import numpy as np

from sdp.evaluation import newey_west

LAG = 5
GATE_T = 2.0
PERIODS = ("development", "all")

COLUMNS = ("index_key", "period", "first_month", "last_month", "n_up", "n_down", "mu_up",
           "mu_down", "mu_all", "diff", "t_nw", "passes")

HISTORY = """
    select index_key, month, state, fwd_excess
    from asset_history
    where trend is not null and fwd_excess is not null
    order by index_key, month"""


def slope_t(up: np.ndarray, y: np.ndarray) -> float:
    """Return the Newey-West t of b in y = a + b * up, with up a 0/1 indicator.

    The error of b is the mean of z = d * e / v, with d the indicator less its mean, e the
    residual and v the mean of d squared. Its standard error is that of a mean, which
    newey_west gives. This is the sandwich estimator of b with no small-sample correction.
    A sample with one state only has no t.
    """
    n = y.size
    if n < 3 or up.min() == up.max():
        return np.nan
    d = up - up.mean()
    v = d @ d / n
    b = d @ y / (d @ d)
    e = y - y.mean() - b * d
    _, se = newey_west(d * e / v, LAG)
    return b / se if se > 0 else np.nan


def _mean12(x: np.ndarray) -> float:
    return x.mean() * 12 if x.size else np.nan


def model(dbt, session):
    dbt.config(materialized="table")
    dev_end = np.datetime64(str(dbt.config.get("fund_signal_dev_end")), "D")
    session.register("asset_history", dbt.ref("fund_asset_history"))
    h = session.sql(HISTORY).fetchnumpy()
    key = np.asarray(h["index_key"], dtype=object)
    month = np.asarray(h["month"]).astype("datetime64[D]")
    state = np.asarray(h["state"], dtype=int)
    y = np.ma.filled(np.ma.asarray(h["fwd_excess"], dtype=float), np.nan)

    out: dict[str, list] = {c: [] for c in COLUMNS}
    for k in dict.fromkeys(key):
        rows = key == k
        dev_t = np.nan
        block = []
        for period in PERIODS:
            use = rows & (month <= dev_end) if period == "development" else rows
            up = state[use] == 1
            ys = y[use]
            t = slope_t(up.astype(float), ys)
            if period == "development":
                dev_t = t
            mu_up, mu_down = _mean12(ys[up]), _mean12(ys[~up])
            block.append((k, period,
                          month[use].min() if use.any() else np.datetime64("NaT"),
                          month[use].max() if use.any() else np.datetime64("NaT"),
                          int(up.sum()), int((~up).sum()), mu_up, mu_down, _mean12(ys),
                          mu_up - mu_down, t))
        for row in block:
            for c, v in zip(COLUMNS, (*row, bool(dev_t >= GATE_T)), strict=True):
                out[c].append(v)

    arrays = {
        "index_key": np.array(out["index_key"], dtype=object),
        "period": np.array(out["period"], dtype=object),
        "first_month": np.array(out["first_month"], dtype="datetime64[us]"),
        "last_month": np.array(out["last_month"], dtype="datetime64[us]"),
        "n_up": np.array(out["n_up"], dtype=np.int64),
        "n_down": np.array(out["n_down"], dtype=np.int64),
        **{c: np.array(out[c], dtype=float) for c in ("mu_up", "mu_down", "mu_all", "diff",
                                                      "t_nw")},
        "passes": np.array(out["passes"], dtype=bool),
    }
    session.register("asset_gate_arrays", arrays)
    nan_to_null = ", ".join(f"case when isnan({c}) then null else {c} end as {c}"
                            for c in ("mu_up", "mu_down", "mu_all", "diff", "t_nw"))
    session.execute(f"""
        create or replace temp table asset_gate_final as
        select index_key, period, first_month::date as first_month, last_month::date as last_month,
               n_up, n_down, {nan_to_null}, passes
        from asset_gate_arrays""")
    return session.table("asset_gate_final")
