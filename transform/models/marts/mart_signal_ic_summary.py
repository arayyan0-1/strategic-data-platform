"""The IC of each signal, target and horizon over development and over each
development fold. A period uses a session only when its label ends inside the period
(sdp.evaluation), so no label reaches into the next fold or into the holdout.

t_nw is the Newey-West t, which allows for the overlap of the labels. t_naive assumes
independent days and is too high when ic_autocorr_lag1 is high. folds_same_sign counts
the folds whose mean IC has the sign of the development mean. The holdout is in
mart_signal_ic_holdout.
"""
from sdp.evaluation import summarize

QUERY = """
    select g.g, i.horizon, i.ic, s.fold, s.session,
           coalesce(case i.horizon when 1 then s.label_fold_1
                                   when 5 then s.label_fold_5
                                   when 21 then s.label_fold_21 end, -1) as label_fold
    from ic_ic i
    join ic_groups g using (signal, target, horizon)
    join ic_sessions s using (date)
    order by g.g, i.date"""


def load(dbt, session):
    """Register the inputs and return the rows for summarize and the count of folds."""
    session.register("ic_ic", dbt.ref("mart_signal_ic"))
    session.register("ic_sessions", dbt.ref("mart_eval_sessions"))
    session.execute("""
        create or replace temp table ic_groups as
        select *, row_number() over (order by signal, target, horizon) - 1 as g
        from (select distinct signal, target, horizon from ic_ic)""")
    (k,) = session.sql(
        "select max(fold) from ic_sessions where period = 'development'").fetchone()
    return session.sql(QUERY).fetchnumpy(), int(k)


def model(dbt, session):
    dbt.config(materialized="table")
    p, k = load(dbt, session)
    periods = {0: list(range(1, k + 1)), **{j: [j] for j in range(1, k + 1)}}
    session.register("ic_rows", summarize(p["g"], p["horizon"], p["ic"], p["fold"],
                                          p["label_fold"], p["session"], periods))
    session.execute("""
        create or replace temp table ic_stats as
        with r as (
            select *, max(case when period = 0 then mean_ic end)
                          over (partition by "group") as dev_mean
            from ic_rows
        )
        select
            g.signal, g.target, g.horizon,
            case when r.period = 0 then 'development' else 'fold ' || r.period end as period,
            f.date as first_date, l.date as last_date,
            r.n_days::integer as n_days,
            case when isnan(r.mean_ic) then null else r.mean_ic end as mean_ic,
            case when isnan(r.ic_std) then null else r.ic_std end as ic_std,
            case when isnan(r.t_naive) then null else r.t_naive end as t_naive,
            case when isnan(r.t_nw) then null else r.t_nw end as t_nw,
            case when isnan(r.ic_autocorr_lag1) then null else r.ic_autocorr_lag1 end
                as ic_autocorr_lag1,
            case when r.period = 0 then sum(case when r.period > 0
                    and sign(r.mean_ic) = sign(r.dev_mean) then 1 else 0 end)
                    over (partition by r."group") end::integer as folds_same_sign
        from r
        join ic_groups g on g.g = r."group"
        left join ic_sessions f on f.session = r.first
        left join ic_sessions l on l.session = r.last
        order by g.signal, g.target, g.horizon, r.period""")
    return session.table("ic_stats")
