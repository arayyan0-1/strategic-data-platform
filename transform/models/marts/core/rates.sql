{#
  The FRED series, one row per session: rates, credit spreads, money markets, the Federal
  Reserve balance sheet, volatility, financial conditions, currencies and commodities.
  Each value is the latest FRED observation dated before the session: FRED publishes most
  daily series on the next business day, so a value of D is not known on D. A series
  with no new observation carries its last value. Rates and spreads are in percent, as
  in FRED. The balance sheet is in billions of dollars.

  rf is the return of cash on the session: the 4-week Treasury bill rate before the
  session, times the calendar days since the session before, over 360. So a Monday
  earns the weekend. The rate is on a discount basis, a little below the investment
  yield.

  - curve_10y_2y, curve_10y_3m and curve_30y_5y are slopes of the Treasury curve.
    fly_2_5_10 is the 5-year less the mean of the 2 and the 10-year, times 2: positive
    when the belly is high against the wings.
  - fwd_1y1y and fwd_5y5y are the forward rates implied by the constant-maturity
    yields, by the simple approximation (2 x the long yield less the short yield).
  - policy_priced_1y is the 6-month rate 6 months ahead, implied by the 6-month and
    1-year yields, less the 3-month yield: negative when the bill curve prices cuts. It
    also holds a term premium. bill_effr is the 3-month yield less the effective fed
    funds rate: bills trade above fed funds when their supply is heavy.
  - breakeven_7y, breakeven_20y and breakeven_30y are the nominal less the real yield.
  - net_liquidity is the assets of the Federal Reserve less the Treasury General
    Account and the overnight reverse repo.
  - sofr_iorb, effr_iorb, sofr_tail (the 99th percentile of SOFR less SOFR), tgcr_rrp,
    cp_bill (3-month financial paper less the 3-month bill) and bill_effr measure funding
    pressure.
  - vix_term is VIX over VIX3M: above 1 when near-term fear exceeds the three-month view.
#}

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
    coalesce(bill_4w, t_1m) / 100 * date_diff('day', prev_date, date) / 360   as rf,
    {% for col in series.values() %}{{ col }},
    {% endfor %}t_10y - t_2y                                                         as curve_10y_2y,
    t_10y - t_3m                                                                     as curve_10y_3m,
    t_30y - t_5y                                                                     as curve_30y_5y,
    2 * t_5y - t_2y - t_10y                                                          as fly_2_5_10,
    2 * t_2y - t_1y                                                                  as fwd_1y1y,
    2 * t_10y - t_5y                                                                 as fwd_5y5y,
    2 * t_1y - t_6m - t_3m                                                           as policy_priced_1y,
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
