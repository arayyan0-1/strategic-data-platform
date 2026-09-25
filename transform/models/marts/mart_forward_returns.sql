{#
  The labels. Forward total return over 1, 5 and 21 sessions, kept apart from
  the factors so a label cannot leak into one. lead() looks strictly forward.
#}

with base as (

    select
        u.security_key,
        p.date,
        p.adj_close_total
    from {{ ref('stg_prices_adjusted') }} p
    inner join {{ ref('stg_universe') }} u
        on u.ticker = p.ticker and u.date = p.date

    -- One row per security and session: its primary line (stg_security_lines).
    where u.is_primary_line

)

select
    security_key,
    date,
    lead(adj_close_total, 1)  over w / nullif(adj_close_total, 0) - 1 as fwd_ret_1,
    lead(adj_close_total, 5)  over w / nullif(adj_close_total, 0) - 1 as fwd_ret_5,
    lead(adj_close_total, 21) over w / nullif(adj_close_total, 0) - 1 as fwd_ret_21
from base
window w as (partition by security_key order by date)
