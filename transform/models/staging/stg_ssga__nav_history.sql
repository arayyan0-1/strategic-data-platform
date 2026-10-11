{# SSGA daily NAV, shares outstanding and net assets per fund. #}

select
    fund,
    date,
    nav,
    shares_outstanding,
    total_net_assets,
    vendor_pull_date
from {{ from_lake(source('ssga', 'nav_history'), 'etf_nav_history', {
    'fund': 'varchar', 'date': 'date', 'nav': 'double', 'shares_outstanding': 'double',
    'total_net_assets': 'double', 'vendor_pull_date': 'date'}, files='data.parquet') }}
