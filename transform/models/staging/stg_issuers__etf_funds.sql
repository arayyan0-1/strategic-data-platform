{# Issuer screener rows per pull date and fund. premium_discount is in percent (SSGA only). #}

select
    cast(date as date)      as pull_date,
    issuer,
    fund,
    name,
    asset_class,
    sub_asset_class,
    market_type,
    region,
    country,
    benchmark,
    inception_date,
    expense_ratio,
    nav,
    nav_date,
    total_net_assets,
    assets_date,
    close_price,
    premium_discount
from {{ from_lake(source('issuers', 'etf_funds'), 'etf_funds', {
    'date': 'date', 'issuer': 'varchar', 'fund': 'varchar', 'name': 'varchar',
    'asset_class': 'varchar', 'sub_asset_class': 'varchar', 'market_type': 'varchar',
    'region': 'varchar', 'country': 'varchar', 'benchmark': 'varchar',
    'inception_date': 'date', 'expense_ratio': 'double', 'nav': 'double', 'nav_date': 'date',
    'total_net_assets': 'double', 'assets_date': 'date', 'close_price': 'double',
    'premium_discount': 'double'}) }}
