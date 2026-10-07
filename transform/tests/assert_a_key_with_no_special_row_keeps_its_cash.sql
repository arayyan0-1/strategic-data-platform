{# A returned row is a dividend key whose regular_cash_amount disagrees with the vendor rows:
   equal to cash with no special row, null with only special rows, at most cash otherwise. #}
with vendor as (

    select
        ticker,
        cast(ex_dividend_date as date)                             as event_date,
        bool_or(distribution_type is not distinct from 'special')  as has_special,
        bool_and(distribution_type is not distinct from 'special') as all_special
    from {{ ref('stg_massive__dividends') }}
    where ticker is not null
      and ex_dividend_date is not null
      and currency = 'USD'
      and (historical_adjustment_factor is null or historical_adjustment_factor > 0)
    group by 1, 2

)

select v.*, a.cash_amount, a.regular_cash_amount
from vendor v
left join {{ ref('int_corporate_actions') }} a
    on a.ticker = v.ticker
   and a.event_date = v.event_date
   and a.kind = 'dividend'
where a.ticker is null
   or (not v.has_special and a.regular_cash_amount is distinct from a.cash_amount)
   or (v.all_special and a.regular_cash_amount is not null)
   or (v.has_special and not v.all_special
       and (a.regular_cash_amount is null or a.regular_cash_amount > a.cash_amount))
