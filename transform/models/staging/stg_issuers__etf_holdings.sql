{# Holdings per fund, as-of date and line. weight is a fraction of net assets; quantity is
   shares for equity and par for bonds. Tickers use a dot for a share class (BRK.B). #}

select
    cast(date as date)                                                 as date,
    fund,
    issuer,
    position,
    nullif(replace(replace(upper(trim(ticker)), ' ', '.'), '/', '.'), '') as ticker,
    name,
    sector,
    asset_class,
    market_value,
    weight,
    quantity,
    price,
    location,
    exchange,
    currency,
    cusip,
    isin,
    sedol,
    coupon,
    maturity,
    duration,
    ytm,
    fund_shares_outstanding
from {{ from_lake(source('issuers', 'etf_holdings'), 'etf_holdings', {
    'date': 'date', 'fund': 'varchar', 'issuer': 'varchar', 'position': 'integer',
    'ticker': 'varchar', 'name': 'varchar', 'sector': 'varchar', 'asset_class': 'varchar',
    'market_value': 'double', 'weight': 'double', 'quantity': 'double', 'price': 'double',
    'location': 'varchar', 'exchange': 'varchar', 'currency': 'varchar', 'cusip': 'varchar',
    'isin': 'varchar', 'sedol': 'varchar', 'coupon': 'double', 'maturity': 'date',
    'duration': 'double', 'ytm': 'double', 'fund_shares_outstanding': 'double'},
    files='date=*/*.parquet') }}
