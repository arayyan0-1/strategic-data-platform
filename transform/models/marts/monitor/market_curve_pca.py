"""The principal components of the daily changes of the Treasury curve, from 3 months to
30 years, over the history of core.rates. The first three are level, slope and
curvature. Each loading is the move in bps of a maturity for a move of one unit of the
component. The signs are fixed so that level is a rise of every yield, slope a rise of
the long end against the short end, and curvature a rise of the 5-year against the
3-month and the 30-year. var_share is the share of the variance of the daily changes
that the component explains.
"""
import numpy as np

MATURITIES = (("t_3m", 3), ("t_6m", 6), ("t_1y", 12), ("t_2y", 24), ("t_3y", 36),
              ("t_5y", 60), ("t_7y", 84), ("t_10y", 120), ("t_20y", 240), ("t_30y", 360))
NAMES = ("level", "slope", "curvature")


def model(dbt, session):
    dbt.config(materialized="table")
    session.register("pca_rates", dbt.ref("rates"))
    cols = [m for m, _ in MATURITIES]
    changes = session.sql(f"""
        select {", ".join(f"100 * ({c} - lag({c}) over (order by date)) as {c}" for c in cols)}
        from pca_rates
        order by date""").fetchnumpy()
    X = np.column_stack([np.asarray(changes[c], dtype=float) for c in cols])
    X = X[np.isfinite(X).all(axis=1)]
    values, vectors = np.linalg.eigh(np.cov(X, rowvar=False))
    order = np.argsort(values)[::-1][:len(NAMES)]
    share = values[order] / values.sum()
    V = vectors[:, order]

    # Fix the sign of each component so that its name reads the right way round.
    i3m, i5y, i30y = 0, cols.index("t_5y"), len(cols) - 1
    tests = (V[:, 0].sum(), V[i30y, 1] - V[i3m, 1], V[i5y, 2] - (V[i3m, 2] + V[i30y, 2]) / 2)
    for j, t in enumerate(tests):
        if t < 0:
            V[:, j] = -V[:, j]

    rows = [(j + 1, NAMES[j], m, months, float(V[i, j]), float(share[j]), len(X))
            for j in range(len(NAMES)) for i, (m, months) in enumerate(MATURITIES)]
    session.execute("""
        create or replace temp table curve_pca (
            component integer, name varchar, measure varchar, maturity integer,
            loading double, var_share double, n_days integer)""")
    session.executemany("insert into curve_pca values (?, ?, ?, ?, ?, ?, ?)", rows)
    return session.table("curve_pca")
