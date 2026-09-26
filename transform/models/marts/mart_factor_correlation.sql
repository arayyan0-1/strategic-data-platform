{#
  The correlation of the daily returns of each pair of factors over the last two
  years, on the sessions that both have. A lagged factor (ff) has fewer sessions,
  and n says how many.
#}

with r as (

    select date, family, factor, ret
    from {{ ref('mart_factor_returns') }}
    where ret is not null
      and date > (select max(date) from {{ ref('mart_factor_returns') }}) - interval 2 year

)

select
    a.family as family_a,
    a.factor as factor_a,
    b.family as family_b,
    b.factor as factor_b,
    corr(a.ret, b.ret) as corr,
    count(*)           as n
from r a
join r b using (date)
group by all
