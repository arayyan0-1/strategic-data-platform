{#
  The state of the US stock market from the in-universe common stock, one row per
  session. Each name is its security series (the primary line), and the moving
  averages and 52-week extremes use the total-adjusted close over the full series.

  - ew_ret is the equal-weighted return, with each return winsorized at the 1st and
    99th percentile of the session, so one microcap spike does not move the index.
    dv_ret weights by trailing dollar volume, a proxy for a cap-weighted index until
    market cap arrives.
  - dispersion is the interquartile range of the returns over 1.349, a robust
    cross-sectional standard deviation.
  - avg_corr_21 is the variance of ew_ret over 21 sessions over the square of the
    average single-name volatility, an estimate of the average pairwise correlation.
  - resid_dispersion is the robust cross-sectional standard deviation of the residuals
    of the style and industry model: the room for stock picking. median_spread is the
    median estimated spread (mart_spreads). style_r2 is the share of the cross-section
    that the model explains, and absorption the variance share of the first principal
    component. The _21 columns are their 21-session means.
#}

with s as (

    select
        u.security_key, p.date, u.in_universe, u.adv, p.dollar_volume,
        coalesce(p.adj_close_total, p.adj_close_split) as px
    from {{ ref('stg_prices_adjusted') }} p
    join {{ ref('stg_universe') }} u using (ticker, date)
    where u.is_primary_line and u.type_filled = 'CS'

), f as (

    select
        *,
        px / lag(px) over w - 1                                             as ret,
        avg(px)   over (partition by security_key order by date
                        rows between 49 preceding and current row)          as ma50,
        count(px) over (partition by security_key order by date
                        rows between 49 preceding and current row)          as n50,
        avg(px)   over (partition by security_key order by date
                        rows between 199 preceding and current row)         as ma200,
        count(px) over (partition by security_key order by date
                        rows between 199 preceding and current row)         as n200,
        max(px)   over (partition by security_key order by date
                        rows between 251 preceding and current row)         as hi252,
        min(px)   over (partition by security_key order by date
                        rows between 251 preceding and current row)         as lo252,
        count(px) over (partition by security_key order by date
                        rows between 251 preceding and current row)         as n252
    from s
    window w as (partition by security_key order by date)

), g as (

    select
        *,
        stddev_samp(ret) over (partition by security_key order by date
                               rows between 20 preceding and current row)  as sigma21
    from f

), bounds as (

    select date, quantile_cont(ret, 0.01) as lo, quantile_cont(ret, 0.99) as hi
    from g
    where in_universe and ret is not null
    group by date

), daily as (

    select
        date,
        count(*)                                                    as n_names,
        count(*) filter (where ret > 0)                             as advancers,
        count(*) filter (where ret < 0)                             as decliners,
        avg(least(greatest(ret, b.lo), b.hi))                       as ew_ret,
        sum(ret * adv) / nullif(sum(adv) filter (where ret is not null), 0) as dv_ret,
        median(ret)                                                 as median_ret,
        (quantile_cont(ret, 0.75) - quantile_cont(ret, 0.25)) / 1.349 as dispersion,
        avg((px > ma50)::int) filter (where n50 = 50)               as pct_above_ma50,
        avg((px > ma200)::int) filter (where n200 = 200)            as pct_above_ma200,
        count(*) filter (where n252 = 252 and px >= hi252)          as new_highs,
        count(*) filter (where n252 = 252 and px <= lo252)          as new_lows,
        sum(dollar_volume) filter (where ret > 0)
            / nullif(sum(dollar_volume) filter (where ret is not null), 0) as up_volume_share,
        avg(sigma21)                                                as avg_sigma21
    from g
    left join bounds b using (date)
    where in_universe
    group by date

), resid as (

    select date,
           (quantile_cont(resid, 0.75) - quantile_cont(resid, 0.25)) / 1.349 as resid_dispersion
    from {{ ref('mart_residuals') }}
    group by date

), spreads as (

    select date, median(spread) as median_spread
    from {{ ref('mart_spreads') }}
    where in_universe and spread is not null
    group by date

), joined as (

    select d.*, r.resid_dispersion, s.median_spread, f.r2 as style_r2, p.pc1_share as absorption
    from daily d
    left join resid r using (date)
    left join spreads s using (date)
    left join {{ ref('mart_factor_style') }} f using (date)
    left join {{ ref('mart_factor_pca') }} p using (date)

)

select
    * exclude (avg_sigma21),
    new_highs - new_lows                                            as net_highs,
    avg(dispersion) over w21                                        as dispersion_21,
    avg(resid_dispersion) over w21                                  as resid_dispersion_21,
    avg(style_r2) over w21                                          as style_r2_21,
    avg(up_volume_share) over w21                                   as up_volume_share_21,
    sum(advancers - decliners) over w                               as ad_line,
    exp(sum(ln(1 + coalesce(ew_ret, 0))) over w)                    as ew_index,
    exp(sum(ln(1 + coalesce(dv_ret, 0))) over w)                    as dv_index,
    stddev_samp(ew_ret) over w21 * sqrt(252)                        as ew_vol_21,
    var_samp(ew_ret) over w21 / nullif(avg_sigma21 * avg_sigma21, 0) as avg_corr_21
from joined
window w   as (order by date rows between unbounded preceding and current row),
       w21 as (order by date rows between 20 preceding and current row)
