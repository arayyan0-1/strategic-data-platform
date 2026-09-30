-- The coverage models hold common stock that is not in the universe. No row is in the
-- universe, in the estimation (style_exposures, style_residuals) or of another type.
select 'in the universe' as problem, e.security_key, e.date
from {{ ref('coverage_exposures') }} e
inner join {{ ref('int_universe') }} u
    on u.ticker = e.ticker and u.date = e.date
where u.in_universe

union all

select 'in the estimation', e.security_key, e.date
from {{ ref('coverage_exposures') }} e
inner join {{ ref('style_exposures') }} s
    on s.security_key = e.security_key and s.date = e.date

union all

select 'in the estimation residuals', r.security_key, r.date
from {{ ref('coverage_residuals') }} r
inner join {{ ref('style_residuals') }} s
    on s.security_key = r.security_key and s.date = r.date

union all

-- A null type is not in the list.
select 'wrong type', security_key, date
from {{ ref('coverage_exposures') }}
where type_filled is null or type_filled not in ({{ sql_list('coverage_types') }})

union all

select 'wrong type', security_key, date
from {{ ref('coverage_residuals') }}
where type_filled is null or type_filled not in ({{ sql_list('coverage_types') }})
