{# Material corporate actions up to 45 days after the last session, for liquid tickers ($20M
   dollar volume): every split and each special or 1%+ dividend. Reads staging, as a pending
   split has no factor yet. dividend_yield is not annualized. #}

with last_session as (

    select max(date) as d from {{ ref('int_prices_adjusted') }}

), live as (

    select u.ticker, u.name, u.type_filled as type, u.in_universe, u.close, u.dollar_volume
    from {{ ref('int_universe') }} u
    where u.date = (select d from last_session)

), splits as (

    select
        cast(execution_date as date) as event_date,
        ticker,
        'split' as kind,
        adjustment_type as detail,
        split_from,
        split_to,
        cast(null as double) as cash_amount
    from {{ ref('stg_massive__splits') }}
    where cast(execution_date as date) > (select d from last_session)
      and cast(execution_date as date) <= (select d from last_session) + interval 45 day

), dividends as (

    select
        cast(ex_dividend_date as date) as event_date,
        ticker,
        'dividend' as kind,
        distribution_type as detail,
        cast(null as double) as split_from,
        cast(null as double) as split_to,
        cash_amount
    from {{ ref('stg_massive__dividends') }}
    where cast(ex_dividend_date as date) > (select d from last_session)
      and cast(ex_dividend_date as date) <= (select d from last_session) + interval 45 day
      and currency = 'USD'
      and cash_amount > 0

)

select
    e.*,
    l.name, l.type, l.in_universe, l.close, l.dollar_volume,
    e.cash_amount / nullif(l.close, 0) as dividend_yield
from (select * from splits union all by name select * from dividends) e
join live l using (ticker)
where l.dollar_volume >= 2e7
  and (e.kind = 'split'
       or e.detail = 'special'
       or e.cash_amount / nullif(l.close, 0) >= 0.01)
