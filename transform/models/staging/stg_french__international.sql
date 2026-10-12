{# Monthly returns of regions, countries and long US portfolios from the French library, as
   fractions. series is the table inside a file: monthly, value_weighted, or a country. #}

select
    cast(file as varchar)               as file,
    cast(series as varchar)             as series,
    cast(column_name as varchar)        as column_name,
    cast(month as date)                 as month,
    cast(value as double)               as value,
    cast(vendor_pull_date as date)      as vendor_pull_date
from {{ from_lake(source('french', 'international'), 'french_international', {
    'file': 'varchar', 'series': 'varchar', 'column_name': 'varchar', 'month': 'date',
    'value': 'double', 'vendor_pull_date': 'date'}, files='data.parquet') }}
