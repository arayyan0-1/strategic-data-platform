{# How much of each fund's long common-equity value maps to a security_key, per report date. #}

select
    fund,
    report_date,
    count(*)                                                                as n_lines,
    count(security_key)                                                     as n_mapped,
    sum(val_usd) filter (where security_key is not null) / nullif(sum(val_usd), 0)
                                                                            as weight_mapped
from {{ ref('int_fund_holdings_lines') }}
group by fund, report_date
