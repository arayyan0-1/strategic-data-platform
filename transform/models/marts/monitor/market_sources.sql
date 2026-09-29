{#
  How fresh each source of the warehouse is: the last date of its data, the date of the
  pull behind it where the source is current state, and the age in days at which the
  data is late (max_age_days). The page shows a source in red past that age.
#}

select 'Massive day bars' as source, max(date) as last_date, null::date as pulled,
       4 as max_age_days, 'the last session in the lake' as note
from {{ ref('int_universe') }}
union all
select 'Massive ticker details', max(month_end), null, 40, 'market caps and industries, month-ends'
from {{ ref('int_security_details') }}
union all
select 'Massive short interest', max(settlement_date), null, 30,
       'settlement date; public about eight sessions later'
from {{ ref('int_short_interest') }}
union all
select 'Massive splits', max(execution_date) filter (where execution_date <= vendor_pull_date),
       max(vendor_pull_date), 2, 'current state, pulled daily'
from {{ ref('stg_massive__splits') }}
union all
select 'Massive dividends', max(ex_dividend_date) filter (where ex_dividend_date <= vendor_pull_date),
       max(vendor_pull_date), 2, 'current state, pulled daily'
from {{ ref('stg_massive__dividends') }}
union all
select 'FRED', max(date) filter (where series_id = 'DGS10' and value is not null),
       max(vendor_pull_date), 4, 'the 10-year yield; each measure shows its own date'
from {{ ref('stg_fred__series') }}
union all
select 'French library', max(date), max(vendor_pull_date), 75, 'the library lags about two months'
from {{ ref('stg_french__factors') }}
union all
select 'global-q', max(date), max(vendor_pull_date), 400, 'one vintage a year'
from {{ ref('stg_global_q__factors') }}
