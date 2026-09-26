-- One return per session, family and factor. A second row would count a return twice
-- in every compounded window.
select date, family, factor, count(*) as n
from {{ ref('mart_factor_returns') }}
group by all
having count(*) > 1
