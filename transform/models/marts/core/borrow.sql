{# IBKR borrow per security and session. When two symbols map to one security, the FIGI
   match wins, then the shorter ticker. #}

select
    date,
    security_key,
    symbol,
    ticker,
    name,
    fee_rate,
    rebate_rate,
    available,
    available_capped,
    snapshot_at,
    matched_on
from {{ ref('int_borrow') }}
where security_key is not null
qualify row_number() over (
    partition by date, security_key
    order by matched_on = 'figi' desc, length(ticker), ticker) = 1
