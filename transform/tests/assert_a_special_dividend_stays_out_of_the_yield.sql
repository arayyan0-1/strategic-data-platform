-- COST paid a special dividend of 15 with ex-date 2023-12-27, and the vendor types the
-- row special. The 12 months to 2024-01-02 also hold four regular dividends of 0.90,
-- 1.02, 1.02 and 1.02, which sum to 3.96. The special row has no regular amount. The
-- yield counts 3.96 of cash and not 18.96. A missing row is a failure.
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
