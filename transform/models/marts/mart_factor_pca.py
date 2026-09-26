"""Statistical factor returns: the top five eigenportfolios of the trailing correlation
matrix of the in-universe names (sdp.factors.pca_returns). Each rebalance uses the 1,000
most liquid names with a full year of returns, and the weights earn the next 21 sessions
out of sample. pc1 is close to the market. share_k is the variance share of component k
at its rebalance, and pc1_share is the absorption ratio: a high value means the stocks
move together. A row is dated on the session that earns the return."""
import numpy as np

from sdp.factors import filled, pca_returns

K = 5
WINDOW = 252
STEP = 21
N_NAMES = 1000


def _nulls(cols) -> str:
    """Return SQL that turns each NaN into a NULL, so an average downstream skips it."""
    return ", ".join(f"case when isnan(o.{c}) then null else o.{c} end as {c}" for c in cols)


def model(dbt, session):
    dbt.config(materialized="table")
    session.register("pca_signals", dbt.ref("mart_signals"))
    session.register("pca_universe", dbt.ref("stg_universe"))

    session.execute("""
        create or replace temp table pca_days as
        select date, row_number() over (order by date) - 1 as day
        from (select distinct date from pca_signals)""")
    session.execute("""
        create or replace temp table pca_keys as
        select security_key, row_number() over (order by security_key) - 1 as col
        from (select distinct security_key from pca_signals where in_universe)""")
    long = session.sql("""
        select d.day, k.col, s.ret_1 as r, s.in_universe as u, u.adv as a
        from pca_signals s
        join pca_keys k using (security_key)
        join pca_days d using (date)
        join pca_universe u on u.ticker = s.ticker and u.date = s.date
    """).fetchnumpy()
    (T,) = session.sql("select count(*) from pca_days").fetchone()
    (N,) = session.sql("select count(*) from pca_keys").fetchone()

    day, col = np.asarray(long["day"]), np.asarray(long["col"])
    R = np.full((T, N), np.nan)
    U = np.zeros((T, N), dtype=bool)
    A = np.zeros((T, N))
    R[day, col] = filled(long["r"])
    U[day, col] = np.ma.filled(np.ma.asarray(long["u"]), False)
    A[day, col] = np.nan_to_num(filled(long["a"]))

    F, share, used = pca_returns(R, U, A, window=WINDOW, step=STEP, k=K, n_names=N_NAMES)
    out = {"day": np.arange(T, dtype=np.int64), "n_names": used.astype(np.int64)}
    for j in range(K):
        out[f"pc{j + 1}"] = F[:, j]
    for j in range(K):
        out[f"pc{j + 1}_share"] = share[:, j]

    session.register("pca_arrays", out)
    session.execute("create or replace temp table pca_out as select * from pca_arrays")
    cols = [f"pc{j + 1}" for j in range(K)] + [f"pc{j + 1}_share" for j in range(K)]
    session.execute(f"""
        create or replace temp table pca_final as
        select d.date, o.n_names, {_nulls(cols)}
        from pca_out o join pca_days d using (day)
        where not isnan(o.pc1)
        order by date""")
    return session.table("pca_final")
