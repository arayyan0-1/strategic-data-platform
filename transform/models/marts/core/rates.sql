{#
  Rates, credit spreads, volatility and financial conditions, one row per session. Each
  value is the latest FRED observation dated before the session: FRED publishes most
  daily series on the next business day, so a value of D is not known on D. A series
  with no new observation carries its last value. Rates and spreads are in percent,
  as in FRED.

  rf is the return of cash on the session: the 4-week Treasury bill rate before the
  session, times the calendar days since the session before, over 360. So a Monday
  earns the weekend. The rate is on a discount basis, a little below the investment
  yield. curve_10y_2y and curve_10y_3m are the slopes of the Treasury curve, and
  vix_term is VIX over VIX3M: above 1 when near-term fear exceeds the three-month view.
#}

{% set series = fred_columns() %}

with sessions as (

    select date, lag(date) over (order by date) as prev_date
    from (select distinct date from {{ ref('int_universe') }})

), wide as (

    pivot (select series_id, date, value
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
    vix / nullif(vix_3m, 0)                                                          as vix_term
from known
order by date
