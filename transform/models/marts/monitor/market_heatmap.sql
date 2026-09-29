{#
  The 500 largest in-universe names on the last session, for the heat map: the cap, the
  Fama-French 12 industry, and the total returns over 1, 5 and 21 sessions and the year
  to date. resid_z is the stock-specific move of the last session in sigma.
#}

with last_session as (

    select max(date) as d from {{ ref('signals') }}

), top as (

    select security_key, ticker, name, industry, industry_name, market_cap as cap
    from {{ ref('int_universe') }}
    where date = (select d from last_session) and in_universe and is_primary_line
      and market_cap is not null
    order by market_cap desc
    limit 500

), rets as (

    select
        s.security_key,
        s.ret_1,
        row_number() over (partition by s.security_key order by s.date desc) as back,
        year(s.date) = year((select d from last_session))                    as this_year
    from {{ ref('signals') }} s
    where s.security_key in (select security_key from top)
      and s.date > (select d from last_session) - interval 13 month

)

select
    t.*,
    exp(sum(ln(1 + r.ret_1)) filter (where r.back <= 1)) - 1   as ret_1d,
    exp(sum(ln(1 + r.ret_1)) filter (where r.back <= 5)) - 1   as ret_1w,
    exp(sum(ln(1 + r.ret_1)) filter (where r.back <= 21)) - 1  as ret_1m,
    exp(sum(ln(1 + r.ret_1)) filter (where r.this_year)) - 1   as ret_ytd,
    any_value(x.resid_z)                                       as resid_z
from top t
join rets r using (security_key)
left join {{ ref('style_residuals') }} x
    on x.security_key = t.security_key and x.date = (select d from last_session)
group by all
order by t.cap desc
