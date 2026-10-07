{# The market_rates measures over the last two years, one row per measure and session, for the
   monitor charts. Values are those known before the session, as in core.rates. #}

select r.measure, r.date, r.value
from (
    unpivot (select * exclude (obs_date, rf) from {{ ref('rates') }}
             where date > (select max(date) from {{ ref('rates') }}) - interval 2 year)
    on columns(* exclude (date))
    into name measure value value
) r
where r.measure in (select measure from {{ ref('market_rates') }})
order by r.measure, r.date
