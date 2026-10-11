{# Holdings per fund, as-of date and line, with the security_key of a US-listed stock line:
   its ticker on the newest ticker reference at or before the as-of date. #}

with holdings as (

    select * from {{ ref('stg_issuers__etf_holdings') }}

), keyed as (

    select date, ticker, security_key
    from {{ ref('int_tickers_keyed') }}
    where date >= (select coalesce(min(date), date '2100-01-01') - 10 from holdings)

), us as (

    select h.fund, h.date, h.position, k.security_key
    from holdings h
    asof join keyed k
        on k.ticker = h.ticker and k.date <= h.date
    where h.date - k.date <= 10
      -- Stock lines only: a bond or a swap can carry its issuer's ticker.
      and coalesce(h.asset_class, 'Equity') = 'Equity'
      and h.coupon is null and h.maturity is null
      -- iShares states the listing country; SSGA only the currency of the line.
      and coalesce(h.location, case when h.currency = 'USD' then 'United States' end)
          = 'United States'

)

select
    h.*,
    us.security_key
from holdings h
left join us using (fund, date, position)
