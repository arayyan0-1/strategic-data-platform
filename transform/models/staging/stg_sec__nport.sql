{# N-PORT holdings per fund, report date and line. pct_val is a fraction of net assets and
   val_usd the value in dollars. filed is the date the public could first see the report. #}

select
    fund,
    series_id,
    accession,
    filed,
    report_date,
    cusip,
    isin,
    name,
    title,
    balance,
    units,
    val_usd,
    pct_val,
    asset_cat,
    issuer_cat,
    inv_country,
    payoff
from {{ from_lake(source('sec', 'nport'), 'sec_nport', {
    'fund': 'varchar', 'series_id': 'varchar', 'accession': 'varchar', 'filed': 'date',
    'report_date': 'date', 'cusip': 'varchar', 'isin': 'varchar', 'name': 'varchar',
    'title': 'varchar', 'balance': 'double', 'units': 'varchar', 'val_usd': 'double',
    'pct_val': 'double', 'asset_cat': 'varchar', 'issuer_cat': 'varchar',
    'inv_country': 'varchar', 'payoff': 'varchar'}, files='date=*/*.parquet') }}
