{#
  The bid-ask spread of each common stock, estimated from daily prices, because the
  plan has no quotes (Abdi and Ranaldo 2017): s² = 4 E[(c_t − η_t)(c_t − η_t+1)], with
  c the log close and η the mean of the log high and the log low. The mean runs over the
  21 terms that the close of D makes known, and a negative estimate is 0. The prices are
  split-adjusted, so a split does not enter. spread is a fraction of the price: 0.002 is
  20 basis points, and a trade pays about half of it.
#}

with s as (

    select
        u.security_key,
        p.date,
        u.in_universe,
        ln(p.adj_close_split)                                   as c,
        (ln(p.adj_high_split) + ln(p.adj_low_split)) / 2        as eta
    from {{ ref('stg_prices_adjusted') }} p
    join {{ ref('stg_universe') }} u using (ticker, date)
    where u.is_primary_line
      and u.type_filled = 'CS'
      and p.adj_low_split > 0 and p.adj_high_split > 0 and p.adj_close_split > 0

), terms as (

    select
        *,
        (c - eta) * (c - lead(eta) over (partition by security_key order by date)) as term
    from s

)

select
    security_key,
    date,
    in_universe,
    sqrt(greatest(4 * avg(term) over (
        partition by security_key order by date
        rows between 21 preceding and 1 preceding), 0))       as spread
from terms
