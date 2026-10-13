{# The FIGIs OpenFIGI gives each code: a CUSIP, or an ISIN for a line that states no CUSIP.
   share_class_figi names the security and survives a US delisting. composite_figi and ticker are
   the US listing's, null without one. Null FIGIs: no match. #}

select
    cusip,
    isin,
    share_class_figi,
    composite_figi,
    ticker,
    name,
    security_type,
    mapped_on
from {{ from_lake(source('openfigi', 'cusips'), 'openfigi_cusips', {
    'cusip': 'varchar', 'isin': 'varchar', 'share_class_figi': 'varchar',
    'composite_figi': 'varchar', 'ticker': 'varchar', 'name': 'varchar',
    'security_type': 'varchar', 'mapped_on': 'date'},
    files='data.parquet') }}
