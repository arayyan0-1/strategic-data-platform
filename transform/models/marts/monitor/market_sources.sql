{#
  How fresh each source of the warehouse is: the last date of its data, the date of the
  pull behind it where the source is current state, and the age in days at which the
  data is late (max_age_days). The page shows a source in red past that age. The last
  row counts the changes of FIGI key that need a reviewed link decision (pending).
#}

select 'Massive day bars' as source, max(date) as last_date, null::date as pulled,
       4 as max_age_days, 'the last session in the lake' as note, null::bigint as pending
from {{ ref('int_universe') }}
union all
select 'Massive ticker details', max(month_end), null, 40, 'market caps and industries, month-ends', null
from {{ ref('int_security_details') }}
union all
select 'Massive short interest', max(settlement_date), null, 30,
       'settlement date; public about eight sessions later', null
from {{ ref('int_short_interest') }}
union all
select 'Massive splits', max(execution_date) filter (where execution_date <= vendor_pull_date),
       max(vendor_pull_date), 2, 'current state, pulled daily', null
from {{ ref('stg_massive__splits') }}
union all
select 'Massive dividends', max(ex_dividend_date) filter (where ex_dividend_date <= vendor_pull_date),
       max(vendor_pull_date), 2, 'current state, pulled daily', null
from {{ ref('stg_massive__dividends') }}
union all
select 'FRED', max(date) filter (where series_id = 'DGS10' and value is not null),
       max(vendor_pull_date), 4, 'the 10-year yield; each measure shows its own date', null
from {{ ref('stg_fred__series') }}
union all
select 'French library', max(date), max(vendor_pull_date), 75, 'the library lags about two months', null
from {{ ref('stg_french__factors') }}
union all
select 'global-q', max(date), max(vendor_pull_date), 400, 'one vintage a year', null
from {{ ref('stg_global_q__factors') }}
union all
select 'Security links', max(link_date), null, null,
       'FIGI changes that look like one security and need a link decision (seed)',
       count(*)
from {{ ref('int_key_changes') }}
where needs_review
