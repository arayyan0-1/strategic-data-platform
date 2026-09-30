{#
  Relations between assets that a trader reads as one number, from the monitor ETFs,
  one row per relation and session over two years (market_relation_values). A ratio is
  the price of the first ETF over the second, rebased to 1 at the start. A correlation
  is over 63 sessions of daily returns. change_21 is the change over 21 sessions (a
  return for a ratio, a difference for a correlation). z_21 is the score of that change
  (market_scores): a score of 3 is as rare as a 3 sigma move of a normal distribution on
  the history of the relations. pctile is the share of the window with a lower or equal
  value.

  Credit spreads, the VIX term structure and the dollar are measured, not proxied, in
  market_rates.
#}

with last_session as (

    select max(date) as d from {{ ref('market_relation_values') }}

), changed as (

    select
        *,
        case when kind = 'corr' then value - lag(value, 21) over w
             else value / lag(value, 21) over w - 1 end         as change_21
    from {{ ref('market_relation_values') }}
    window w as (partition by relation order by date)

), windowed as (

    select
        relation, label, kind, date,
        case when kind = 'corr' then value
             else value / first_value(value) over (partition by relation order by date) end as value,
        change_21
    from changed
    where date > (select d from last_session) - interval 2 year

)

select
    w.*,
    s.z                                                                        as z_21,
    percent_rank() over (partition by w.relation order by w.value)             as pctile
from windowed w
left join {{ ref('market_scores') }} s
    on s.family = 'relation' and s.subject = w.relation and s.h = 21 and s.date = w.date
