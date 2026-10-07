{# A returned row is a universe cap outside (0, $10T], or an AAPL cap outside $1T to $10T on the
   last session. #}
select 'range' as check_name, ticker, date, market_cap
from {{ ref('int_universe') }}
where in_universe and (market_cap <= 0 or market_cap > 1e13)
union all
select 'apple', ticker, date, market_cap
from {{ ref('int_universe') }}
where ticker = 'AAPL' and date = (select max(date) from {{ ref('int_universe') }})
  and (market_cap is null or market_cap not between 1e12 and 1e13)
