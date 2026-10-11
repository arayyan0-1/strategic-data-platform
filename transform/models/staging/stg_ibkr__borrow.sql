{# IBKR borrow snapshot per symbol and session. Rates are annual percentages. #}

select
    cast(date as date)      as date,
    snapshot_at,
    symbol,
    ticker,
    name,
    conid,
    composite_figi,
    fee_rate,
    rebate_rate,
    available,
    available_capped
from {{ from_lake(source('ibkr', 'borrow'), 'ibkr_borrow', {
    'date': 'date', 'snapshot_at': 'timestamptz', 'symbol': 'varchar', 'ticker': 'varchar',
    'name': 'varchar', 'conid': 'bigint', 'composite_figi': 'varchar', 'fee_rate': 'double',
    'rebate_rate': 'double', 'available': 'bigint', 'available_capped': 'boolean'}) }}
