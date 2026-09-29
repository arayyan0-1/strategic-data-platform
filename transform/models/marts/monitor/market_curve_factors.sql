{#
  Level, slope and curvature of the Treasury curve per session over two years. Each
  daily move is the daily change of the yields, in bps, on the loadings of
  market_curve_pca. value is the sum of the moves since the start of the window. z_1d
  is the move of the day over the standard deviation of the moves of the 252 sessions
  before. The rows of core.rates hold the yields of the close before the session, as in
  market_rates.
#}

with changes as (

    unpivot (
        select date,
               {% for m in ['t_3m', 't_6m', 't_1y', 't_2y', 't_3y', 't_5y', 't_7y', 't_10y', 't_20y', 't_30y'] %}
               100 * ({{ m }} - lag({{ m }}) over (order by date)) as {{ m }}{{ "," if not loop.last }}
               {% endfor %}
        from {{ ref('rates') }})
    on columns(* exclude (date))
    into name measure value change

), moves as (

    select c.date, p.component, p.name, sum(c.change * p.loading) as move
    from changes c
    join {{ ref('market_curve_pca') }} p using (measure)
    group by all

), scaled as (

    select
        *,
        move / nullif(stddev_samp(move) over (partition by component order by date
            rows between 252 preceding and 1 preceding), 0)          as z_1d
    from moves

)

select
    date,
    component,
    name,
    move,
    sum(move) over (partition by component order by date)         as value,
    z_1d
from scaled
where date > (select max(date) from {{ ref('rates') }}) - interval 2 year
order by component, date
