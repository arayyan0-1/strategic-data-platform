{# Top 12 and bottom 12 names by contribution to the cap-weighted market over the last session
   (1d) and the last 21 (1m). contribution is the previous session's cap weight times the return;
   specific is the part the style model leaves. #}

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
