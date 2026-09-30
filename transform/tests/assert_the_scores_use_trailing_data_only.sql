-- The trailing z of the last 30 sessions of SPY and of the market factor, over one
-- and 21 sessions, recomputed by a weighted sum over the sessions before each date.
-- A z that used a later session, or one session too many, would differ.
{% set lam = 0.5 ** (1.0 / var('monitor_halflife')) %}
{% set k = var('monitor_vol_window') %}

with series as (

    select 'SPY' as subject, date,
           row_number() over (order by date) as rn,
           ln(adj_close / lag(adj_close) over (order by date)) as x
    from {{ ref('market_history') }}
    where ticker = 'SPY'

    union all

    select 'market', date,
           row_number() over (order by date),
           ln(1 + ret)
    from {{ ref('factor_returns') }}
    where family = 'style' and factor = 'market' and ret is not null

), picked as (

    select 'SPY' as subject, h, date, z_raw
    from {{ ref('market_scores') }}
    where family = 'etf' and subject = 'SPY' and h in (1, 21)
    qualify row_number() over (partition by h order by date desc) <= 30

    union all

    select 'market', h, date, z_raw
    from {{ ref('factor_scores') }}
    where family = 'style' and factor = 'market' and h in (1, 21)
    qualify row_number() over (partition by h order by date desc) <= 30

), expected as (

    select
        p.subject, p.h, p.date, p.z_raw,
        (select sum(x) from series m
         where m.subject = p.subject and m.rn between c.rn - p.h + 1 and c.rn)
        / sqrt(sum(pow({{ lam }}, c.rn - 1 - v.rn) * v.x * v.x) / sum(pow({{ lam }}, c.rn - 1 - v.rn))
               * p.h) as z_check
    from picked p
    join series c on c.subject = p.subject and c.date = p.date
    join series v on v.subject = p.subject and v.rn between c.rn - {{ k }} and c.rn - 1 and v.x is not null
    group by p.subject, p.h, p.date, p.z_raw, c.rn

)

select *
from expected
where z_check is null or abs(z_raw - z_check) > 1e-6 * abs(z_check)
