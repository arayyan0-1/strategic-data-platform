{#
  The bid-ask spread of each common stock as a fraction of the price. The plan has no
  quotes, so spread comes from a published cost model: market microstructure invariance
  (Kyle and Obizhaeva 2016). Half the spread is

      spread_kappa0 · (σ / 0.02) · (W / W*)^(-1/3),   W = σ · dollar volume,

  with σ the root mean square of the daily returns over the 21 sessions before D, the
  dollar volume over the 20 sessions before D (int_universe.adv), and the benchmark
  stock of the paper: $40, one million shares a day and 2% a day, so W* = 800,000.
  spread_kappa0 is the calibration of the linear model on 439,765 portfolio transition
  orders (2001 to 2005). For the benchmark stock it implies a spread of 16.4 bps against
  a quoted 12.0 bps. The spread is at least one tick (tick_size over the close).

  spread_ar is the estimate from daily prices (Abdi and Ranaldo 2017): 4 E[(c − η)(c − η')]
  over the 21 terms that the close of D makes known, with c the log close and η the mean
  of the log high and the log low. It is noise for a liquid stock, where the daily range
  is far wider than the spread, and it stays only as a diagnostic.
#}

with s as (

    -- The windows run over the full series of the security. A type that the vendor
    -- changes for some days (an ADR that is CS on two days) must not cut a window.
    select
        u.security_key,
        p.date,
        u.in_universe,
        u.type_filled,
        p.close,
        u.adv,
        ln(p.adj_close_split)                                   as c,
        (ln(p.adj_high_split) + ln(p.adj_low_split)) / 2        as eta
    from {{ ref('int_prices_adjusted') }} p
    join {{ ref('int_universe') }} u using (ticker, date)
    where u.is_primary_line
      and p.adj_low_split > 0 and p.adj_high_split > 0 and p.adj_close_split > 0

), terms as (

    select
        s.*,
        (c - eta) * (c - lead(eta) over (partition by security_key order by date)) as term,
        sqrt(avg(g.ret_1 * g.ret_1) over (
            partition by security_key order by date
            rows between 21 preceding and 1 preceding))            as sigma
    from s
    left join {{ ref('signals') }} g using (security_key, date)

), estimated as (

    select
        security_key,
        date,
        in_universe,
        type_filled,
        close,
        adv,
        sigma,
        sqrt(greatest(4 * avg(term) over (
            partition by security_key order by date
            rows between 21 preceding and 1 preceding), 0))         as spread_ar
    from terms

)

select
    security_key,
    date,
    in_universe,
    sigma,
    -- An unknown input gives an unknown spread, not the tick floor.
    case when sigma > 0 and adv > 0 and close > 0 then
        greatest(
            2 * {{ var('spread_kappa0') }} * (sigma / 0.02)
              * pow(sigma * adv / 800000.0, -1.0 / 3),
            {{ var('tick_size') }} / close)
    end                                                             as spread,
    spread_ar
from estimated
where type_filled = 'CS'
