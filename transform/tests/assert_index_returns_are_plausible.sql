{# A returned row is a month where an index has a null return or risk-free rate, or a return of
   150% or more in absolute value: a percent read as a fraction, or a missing-value code. #}
select index_key, month, ret, rf
from {{ ref('int_index_returns') }}
where ret is null or rf is null or abs(ret) >= 1.5
