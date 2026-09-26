"""The IC of each signal, target and horizon over the holdout, by the rules of
mart_signal_ic_summary. Read it once for a signal, when the signal is final, and record
the result. A signal that meets the holdout many times is fitted to it.
"""
from sdp.evaluation import summarize


def model(dbt, session):
    dbt.config(materialized="table")
    session.register("ho_ic", dbt.ref("mart_signal_ic"))
    session.register("ho_sessions", dbt.ref("mart_eval_sessions"))
    session.execute("""
        create or replace temp table ho_groups as
        select *, row_number() over (order by signal, target, horizon) - 1 as g
        from (select distinct signal, target, horizon from ho_ic)""")
    (k,) = session.sql(
        "select max(fold) from ho_sessions where period = 'holdout'").fetchone()
    p = session.sql("""
        select g.g, i.horizon, i.ic, s.fold, s.session,
               coalesce(case i.horizon when 1 then s.label_fold_1
                                       when 5 then s.label_fold_5
                                       when 21 then s.label_fold_21 end, -1) as label_fold
        from ho_ic i
        join ho_groups g using (signal, target, horizon)
        join ho_sessions s using (date)
        order by g.g, i.date""").fetchnumpy()
    rows = summarize(p["g"], p["horizon"], p["ic"], p["fold"], p["label_fold"],
                     p["session"], {1: [int(k)]} if k is not None else {})
    session.register("ho_rows", rows)
    session.execute("""
        create or replace temp table ho_stats as
        select
            g.signal, g.target, g.horizon,
            f.date as first_date, l.date as last_date,
            r.n_days::integer as n_days,
            case when isnan(r.mean_ic) then null else r.mean_ic end as mean_ic,
            case when isnan(r.t_nw) then null else r.t_nw end as t_nw
        from ho_rows r
        join ho_groups g on g.g = r."group"
        left join ho_sessions f on f.session = r.first
        left join ho_sessions l on l.session = r.last
        order by g.signal, g.target, g.horizon""")
    return session.table("ho_stats")
