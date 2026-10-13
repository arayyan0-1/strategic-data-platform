{# A returned row is a code on two rows, or a row with both a CUSIP and an ISIN or neither. #}
select coalesce(cusip, isin) as code, count(*) as n
from {{ ref('stg_openfigi__cusips') }}
group by 1
having count(*) > 1 or coalesce(cusip, isin) is null or count(cusip) = count(isin)
