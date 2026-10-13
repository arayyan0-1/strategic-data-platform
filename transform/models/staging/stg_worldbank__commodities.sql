{# Monthly commodity prices and price indices from the World Bank Pink Sheet, one row per series
   and month, in the unit of the workbook. A month with no value has no row. #}

select
    cast(sheet as varchar)              as sheet,
    cast(series as varchar)             as series,
    cast(unit as varchar)               as unit,
    cast(month as date)                 as month,
    cast(value as double)               as value,
    cast(vendor_pull_date as date)      as vendor_pull_date
from {{ from_lake(source('worldbank', 'commodities'), 'worldbank_commodities', {
    'sheet': 'varchar', 'series': 'varchar', 'unit': 'varchar', 'month': 'date',
    'value': 'double', 'vendor_pull_date': 'date'}, files='data.parquet') }}
