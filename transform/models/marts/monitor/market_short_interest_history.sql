{#
  Short interest across the in-universe common stock, per settlement: the dollar value
  sold short (shares short times the close of the settlement date), its share of the
  total cap, the median days to cover, and the count of names with 10 days to cover or
  more. A settlement is public about eight sessions later (effective_date).
#}

select
    s.settlement_date,
    max(s.effective_date)                                         as effective_date,
    count(*)                                                      as n_names,
    sum(s.short_interest * u.close)                               as short_value,
    sum(s.short_interest * u.close) / nullif(sum(u.market_cap), 0) as short_share_of_cap,
    median(s.days_to_cover)                                       as median_days_to_cover,
    count(*) filter (where s.days_to_cover >= 10)                 as n_days_to_cover_10
from {{ ref('int_short_interest') }} s
join {{ ref('int_universe') }} u
    on u.ticker = s.ticker and u.date = s.settlement_date
where u.in_universe and u.market_cap > 0
group by s.settlement_date
order by s.settlement_date
