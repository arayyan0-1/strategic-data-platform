{# Long-form signal panel: one row per in-universe name, session and signal with a value, plus
   the forward labels. avg_rank is the tie-averaged rank in the session and signal; quintile
   comes from it. A signal whose holdout is closed (holdout_closed) has no label that reaches
   into the holdout. #}
{%- set holdout = var('eval_folds') | int + 1 %}
{%- set labels = ['fwd_ret_1', 'fwd_ret_5', 'fwd_ret_21',
                  'fwd_resid_1', 'fwd_resid_5', 'fwd_resid_21'] %}

with unpivoted as (

    -- Labels join before the unpivot, while there is one row per name and session.
    select date, security_key, ticker, signal, value, {{ labels | join(', ') }}
    from (
        unpivot (
            select s.*, r.* exclude (security_key, date),
                   f.fwd_ret_1, f.fwd_ret_5, f.fwd_ret_21,
                   x.fwd_resid_1, x.fwd_resid_5, x.fwd_resid_21
            from {{ ref('signals') }} s
            left join {{ ref('residual_signals') }} r
                on r.security_key = s.security_key and r.date = s.date
            left join {{ ref('forward_returns') }} f
                on f.security_key = s.security_key and f.date = s.date
            left join {{ ref('forward_residuals') }} x
                on x.security_key = s.security_key and x.date = s.date
            where s.in_universe
        )
        on {{ signal_columns() }}
        into name signal value value
    )
    where value is not null

), sig as (

    select
        u.date, u.security_key, u.ticker, u.signal, u.value,
        {%- for l in labels %}
        case when {{ holdout_closed('u.signal') }}
                  and (e.fold = {{ holdout }}
                       or coalesce(e.label_fold_{{ l.split('_')[-1] }}, {{ holdout }}) = {{ holdout }})
             then null else u.{{ l }} end as {{ l }}{{ "," if not loop.last }}
        {%- endfor %}
    from unpivoted u
    join {{ ref('eval_sessions') }} e using (date)

), ranked as (

    -- rank() is 1 + the count of lower values and count(*) over w the count at or below this
    -- value; their mean is the average rank. Both windows share one sort.
    select
        *,
        count(*) over (
            partition by date, sid order by value
            rows between unbounded preceding and unbounded following) as n_names,
        (rank() over w + count(*) over w) / 2.0                       as avg_rank
    from (select *, {{ signal_id('signal') }} as sid from sig)
    window w as (
        partition by date, sid order by value
        range between unbounded preceding and current row)

)

select
    date,
    security_key,
    ticker,
    signal,
    value,
    n_names,
    avg_rank,
    cast(ceil(avg_rank * 5.0 / n_names) as integer) as quintile,
    fwd_ret_1,
    fwd_ret_5,
    fwd_ret_21,
    fwd_resid_1,
    fwd_resid_5,
    fwd_resid_21
from ranked
