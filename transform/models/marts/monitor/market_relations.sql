{# market_relation_values over two years: a ratio rebased to 1 at the start, change_21 over 21
   sessions, z_21 the market_scores score of that change, pctile the share of the window at or
   below the value. Credit, VIX term and the dollar are in market_rates. #}

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
