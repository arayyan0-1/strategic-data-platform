{# A returned row is a month where an index has a null return or risk-free rate, or a return of
   150% or more in absolute value: a percent read as a fraction, or a missing-value code. Two
   exemptions, both real prices. Bitcoin may gain more than 150% in a month, so a large return is
   not evidence of a bad input. The energy index of the World Bank rose 186% in January 1974,
   when the price of crude oil went from 4.1 to 13 dollars a barrel. #}
select index_key, month, ret, rf
from {{ ref('int_index_returns') }}
where ret is null or rf is null
   or (abs(ret) >= 1.5
       and index_key <> 'crypto:bitcoin'
       and not (index_key = 'commodity:energy' and month = date '1974-01-31'))
