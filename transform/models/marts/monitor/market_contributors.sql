{#
  The names that moved the cap-weighted in-universe market most, over the last session
  (span 1d) and the last 21 sessions (span 1m). contribution is the weight (the cap
  of the session before over the total) times the return, summed over the span, so
  the contributions of all names sum to the market return. specific is the part of it
  that the style and industry model does not explain. The top 12 and the bottom 12 of
  each span.
#}

with names as (

    select
        r.date, r.security_key, r.ticker, r.ret, r.resid,
        r.weight_cap / sum(r.weight_cap) over (partition by r.date) as w,
        dense_rank() over (order by r.date desc) as back
    from {{ ref('style_residuals') }} r
    where r.date > (select max(date) from {{ ref('style_residuals') }}) - interval 2 month
      and r.weight_cap > 0

), spans as (

    select '1d' as span, security_key, sum(w * ret) as contribution,
           sum(w * resid) as specific, exp(sum(ln(1 + ret))) - 1 as ret
    from names where back = 1
    group by security_key
    union all
    select '1m', security_key, sum(w * ret), sum(w * resid), exp(sum(ln(1 + ret))) - 1
    from names where back <= 21
    group by security_key

), ranked as (

    select *,
           row_number() over (partition by span order by contribution desc) as rank_up,
           row_number() over (partition by span order by contribution) as rank_down
    from spans

)

select
    r.span,
    case when r.rank_up <= 12 then 'up' else 'down' end         as side,
    case when r.rank_up <= 12 then r.rank_up else r.rank_down end as rank,
    s.ticker,
    s.name,
    u.industry_name,
    u.market_cap                                                  as cap,
    r.ret,
    r.contribution,
    r.specific
from ranked r
join {{ ref('securities') }} s using (security_key)
left join {{ ref('int_universe') }} u
    on u.security_key = r.security_key and u.is_primary_line
   and u.date = (select max(date) from {{ ref('style_residuals') }})
where r.rank_up <= 12 or r.rank_down <= 12
order by r.span, side desc, rank
