{# A returned row means COST's special dividend (15 on 2023-12-27) is missing or reaches the
   trailing 12-month cash, which should hold regular dividends only. #}
with action as (

    select cash_amount, regular_cash_amount
    from {{ ref('int_corporate_actions') }}
    where ticker = 'COST'
      and kind = 'dividend'
      and event_date = date '2023-12-27'

), window_cash as (

    select s.dividend_yield * p.close as trailing_cash
    from {{ ref('signals') }} s
    inner join {{ ref('int_prices_adjusted') }} p
        on p.ticker = s.ticker and p.date = s.date
    where s.ticker = 'COST'
      and s.date = date '2024-01-02'

)

select a.cash_amount, a.regular_cash_amount, t.trailing_cash
from (select 1 as one) o
left join action a on true
left join window_cash t on true
where a.cash_amount is null
   or t.trailing_cash is null
   or abs(a.cash_amount - 15) > 1e-9
   or a.regular_cash_amount is not null
   or abs(t.trailing_cash - 3.96) > 1e-6
