{# Borrow on the newest IBKR session for every keyed security, with the fee and shares
   available five snapshots earlier, and the security's universe state and cap. #}

with borrow as (

    select
        *,
        lag(fee_rate, 5) over w     as fee_rate_5,
        lag(available, 5) over w    as available_5
    from {{ ref('borrow') }}
    window w as (partition by security_key order by date)

), latest_universe as (

    select security_key, in_universe, market_cap, industry_name
    from {{ ref('int_universe') }}
    where date = (select max(date) from {{ ref('int_universe') }}) and is_primary_line

)

select
    b.date,
    b.security_key,
    s.ticker,
    s.name,
    s.type,
    b.fee_rate,
    b.fee_rate_5,
    b.rebate_rate,
    b.available,
    b.available_capped,
    b.available_5,
    coalesce(u.in_universe, false)  as in_universe,
    u.market_cap,
    u.industry_name
from borrow b
join {{ ref('securities') }} s using (security_key)
left join latest_universe u using (security_key)
where b.date = (select max(date) from borrow)
