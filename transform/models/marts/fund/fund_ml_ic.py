"""Walk-forward ridge and gradient-boosted forecasts of the relative return of an index, judged
against the combo and momentum on the same months and names."""
import numpy as np

from sdp.fund import ml

COLUMNS = ("universe", "model", "first_month", "last_month", "n_months", "mean_names",
           "mean_ic", "t_nw", "spread_ann", "spread_t_nw")


def _column(x) -> np.ndarray:
    return np.ma.filled(np.ma.asarray(x, dtype=float), np.nan)


def _month_end(index) -> np.datetime64:
    """Return the last day of the month with this month index."""
    if index is None:
        return np.datetime64("NaT")
    return np.datetime64(int(index) + 1, "M").astype("datetime64[D]") - np.timedelta64(1, "D")


def model(dbt, session):
    dbt.config(materialized="table")
    dev_end = np.datetime64(str(dbt.config.get("fund_signal_dev_end")), "D")
    dev_index = int(ml.month_index(np.array([dev_end]))[0])
    a = dbt.ref("fund_index_features").fetchnumpy()

    universe = np.array([ml.UNIVERSES.index(u) for u in a["universe"]])
    month = ml.month_index(np.asarray(a["month"]))
    raw = np.column_stack([_column(a[f]) for f in ml.FEATURES])
    target = _column(a["target"])
    mom, val = raw[:, ml.FEATURES.index("mom")], raw[:, ml.FEATURES.index("val")]

    X = ml.matrix(raw, universe, month)
    last_year = 1970 + dev_index // 12
    preds, _ = ml.walk_forward(X, target, month, ml.FIRST_YEAR, last_year)

    # One set of rows for every model: scored months, a target, and both signals of the baselines.
    common = ((month >= (ml.FIRST_YEAR - 1970) * 12) & (month <= dev_index)
              & np.isfinite(target) & np.isfinite(mom) & np.isfinite(val))
    for p in preds.values():
        common &= np.isfinite(p)

    out: dict[str, list] = {c: [] for c in COLUMNS}
    for i, name in enumerate(ml.UNIVERSES):
        rows = common & (universe == i)
        scores = {k: preds[k][rows] for k in ml.MODELS}
        scores["combo"] = ml.combo_score(mom[rows], val[rows], month[rows])
        scores["mom"] = mom[rows]
        stats = ml.evaluate(scores, target[rows], month[rows])
        for k in (*ml.MODELS, *ml.BASELINES):
            s = stats[k]
            row = (name, k, _month_end(s["first_month"]), _month_end(s["last_month"]),
                   s["n_months"], s["mean_names"], s["mean_ic"], s["t_nw"], s["spread_ann"],
                   s["spread_t_nw"])
            for c, v in zip(COLUMNS, row, strict=True):
                out[c].append(v)
    arrays = {
        "universe": np.array(out["universe"], dtype=object),
        "model": np.array(out["model"], dtype=object),
        "first_month": np.array(out["first_month"], dtype="datetime64[us]"),
        "last_month": np.array(out["last_month"], dtype="datetime64[us]"),
        "n_months": np.array(out["n_months"], dtype=np.int64),
        **{c: np.array(out[c], dtype=float) for c in COLUMNS[5:]},
    }
    session.register("ml_ic_arrays", arrays)
    session.execute("""
        create or replace temp table ml_ic_final as
        with r as (
            select universe, model, first_month::date as first_month,
                   last_month::date as last_month, n_months, mean_names,
                   case when isnan(mean_ic) then null else mean_ic end as mean_ic,
                   case when isnan(t_nw) then null else t_nw end as t_nw,
                   case when isnan(spread_ann) then null else spread_ann end as spread_ann,
                   case when isnan(spread_t_nw) then null else spread_t_nw end as spread_t_nw,
                   model in ('ridge', 'gbt') as is_model
            from ml_ic_arrays
        )
        select universe, model, first_month, last_month, n_months, mean_names, mean_ic, t_nw,
               spread_ann, spread_t_nw,
               case when is_model then mean_ic > max(mean_ic) filter (where not is_model)
                                                  over (partition by universe) end
                   as beats_baselines,
               case when is_model then mean_ic > max(mean_ic) filter (where not is_model)
                                                  over (partition by universe)
                                       and t_nw >= 2 end
                   as clears_gate
        from r""")
    return session.table("ml_ic_final")
