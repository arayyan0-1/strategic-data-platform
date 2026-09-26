-- A market cap from the ticker details must be positive and below $10T, and Apple must
-- be between $1T and $10T on the last session. A wrong share count or a split that the
-- price move does not cancel breaks one of these.
select 'range' as check_name, ticker, date, market_cap
from {{ ref('stg_universe') }}
where in_universe and (market_cap <= 0 or market_cap > 1e13)
union all
select 'apple', ticker, date, market_cap
from {{ ref('stg_universe') }}
where ticker = 'AAPL' and date = (select max(date) from {{ ref('stg_universe') }})
  and (market_cap is null or market_cap not between 1e12 and 1e13)
