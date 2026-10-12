"""Scores of the funds for every month after development, from models fitted once on development."""
import numpy as np

from sdp.fund import ml


def _column(x) -> np.ndarray:
    return np.ma.filled(np.ma.asarray(x, dtype=float), np.nan)


def _panel(rel):
    a = rel.fetchnumpy()
    universe = np.array([ml.UNIVERSES.index(u) for u in a["universe"]])
    month = ml.month_index(np.asarray(a["month"]))
    raw = np.column_stack([_column(a[f]) for f in ml.FEATURES])
    return a, ml.matrix(raw, universe, month), month


def model(dbt, session):
    dbt.config(materialized="table")
    dev_end = np.datetime64(str(dbt.config.get("fund_signal_dev_end")), "D")
    dev_index = int(ml.month_index(np.array([dev_end]))[0])

    a, X, month = _panel(dbt.ref("fund_index_features"))
    target = _column(a["target"])
    # A development row has a target month (month + 1) up to the end of development.
    train = np.isfinite(target) & (month + 1 <= dev_index)

    f, Xf, month_f = _panel(dbt.ref("fund_features"))
    later = month_f > dev_index

    out: dict[str, list] = {"fund": [], "month": [], "model": [], "score": []}
    for kind in ml.MODELS:
        fitted = ml.fit(kind, X[train], target[train], month[train])
        out["fund"].append(np.asarray(f["fund"], dtype=object)[later])
        out["month"].append(np.asarray(f["month"]).astype("datetime64[us]")[later])
        out["model"].append(np.full(int(later.sum()), kind, dtype=object))
        out["score"].append(fitted.predict(Xf[later]))
    arrays = {k: np.concatenate(v) for k, v in out.items()}
    session.register("ml_score_arrays", arrays)
    session.execute("""
        create or replace temp table ml_score_final as
        select fund, month::date as month, model, score
        from ml_score_arrays
        order by model, fund, month""")
    return session.table("ml_score_final")
