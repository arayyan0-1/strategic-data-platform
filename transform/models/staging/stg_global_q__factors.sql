{# The daily q5 factors as fractions. r_mkt is the market less the risk-free rate. #}

select
    cast(date as date)      as date,
    r_mkt,
    r_me,
    r_ia,
    r_roe,
    r_eg,
    r_f,
    vintage,
    vendor_pull_date
from {{ source('global_q', 'factors') }}
