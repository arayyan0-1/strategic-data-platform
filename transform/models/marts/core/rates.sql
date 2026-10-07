{# FRED series per session, each the latest observation dated before the session (FRED publishes
   a day late). Rates and spreads in percent, the balance sheet in billions of dollars. rf is the
   return of cash on the session. #}

{% set series = fred_columns() %}

with sessions as (

    select date, lag(date) over (order by date) as prev_date
    from (select distinct date from {{ ref('int_universe') }})

), wide as (

    pivot (select series_id, date,
                  value / case when series_id in ({% for id in fred_in_millions() %}'{{ id }}'{{ ", " if not loop.last }}{% endfor %})
                               then 1000 else 1 end as value
           from {{ ref('stg_fred__series') }}
           where value is not null)
    on series_id in ({% for id in series %}'{{ id }}'{{ ", " if not loop.last }}{% endfor %})
    using any_value(value)
    group by date

), filled as (

    select
        date,
        {% for id, col in series.items() %}last_value("{{ id }}" ignore nulls) over w as {{ col }}{{ "," if not loop.last }}
        {% endfor %}
    from wide
    window w as (order by date rows between unbounded preceding and current row)

), known as (

    select s.date, s.prev_date, f.date as obs_date, f.* exclude (date)
    from sessions s
    asof left join filled f on s.date > f.date

)

select
    date,
    obs_date,
    -- 4-week bill rate known before the session, so a Monday earns the weekend.
    coalesce(bill_4w, t_1m) / 100 * date_diff('day', prev_date, date) / 360   as rf,
    {% for col in series.values() %}{{ col }},
    {% endfor %}t_10y - t_2y                                                         as curve_10y_2y,
    t_10y - t_3m                                                                     as curve_10y_3m,
    t_30y - t_5y                                                                     as curve_30y_5y,
    -- Positive when the belly is high against the wings.
    2 * t_5y - t_2y - t_10y                                                          as fly_2_5_10,
    2 * t_2y - t_1y                                                                  as fwd_1y1y,
    2 * t_10y - t_5y                                                                 as fwd_5y5y,
    -- Negative when the bill curve prices cuts; includes a term premium.
    2 * t_1y - t_6m - t_3m                                                           as policy_priced_1y,
    -- Bills trade above fed funds when their supply is heavy.
    t_3m - effr                                                                      as bill_effr,
    t_7y - real_7y                                                                   as breakeven_7y,
    t_20y - real_20y                                                                 as breakeven_20y,
    t_30y - real_30y                                                                 as breakeven_30y,
    fed_assets - tga - rrp                                                           as net_liquidity,
    sofr - iorb                                                                      as sofr_iorb,
    effr - iorb                                                                      as effr_iorb,
    sofr_p99 - sofr                                                                  as sofr_tail,
    tgcr - rrp_rate                                                                  as tgcr_rrp,
    cp_fin_3m - t_3m                                                                 as cp_bill,
    vix / nullif(vix_3m, 0)                                                          as vix_term
from known
order by date
