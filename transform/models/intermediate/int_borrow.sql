{# IBKR borrow per symbol and session, keyed to a security by composite FIGI on the newest
   ticker reference at or before the date, else by ticker. #}

with borrow as (

    select * from {{ ref('stg_ibkr__borrow') }}

), keyed as (

    select date, ticker, composite_figi, security_key
    from {{ ref('int_tickers_keyed') }}
    where date >= (select coalesce(min(date), date '2100-01-01') - 10 from borrow)

), by_figi as (

    select b.date, b.symbol, k.security_key
    from borrow b
    asof join (select * from keyed where composite_figi is not null) k
        on k.composite_figi = b.composite_figi and k.date <= b.date
    where b.date - k.date <= 10

), by_ticker as (

    select b.date, b.symbol, k.security_key
    from borrow b
    asof join keyed k
        on k.ticker = b.ticker and k.date <= b.date
    where b.date - k.date <= 10

)

select
    b.*,
    coalesce(f.security_key, t.security_key)                         as security_key,
    case when f.security_key is not null then 'figi'
         when t.security_key is not null then 'ticker' end           as matched_on
from borrow b
left join by_figi f using (date, symbol)
left join by_ticker t using (date, symbol)
