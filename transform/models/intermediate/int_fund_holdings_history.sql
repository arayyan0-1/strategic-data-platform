{# The long common-equity holdings of each fund on each N-PORT report date, by security. weight is
   the value of the security's lines over the fund's whole long common-equity value on the date,
   so the lines with no key stay out and the weights sum to less than 1 (int_fund_holdings_coverage
   says by how much). filed is when the public could first see the report. #}

with totals as (

    select fund, report_date, sum(val_usd) as total_usd
    from {{ ref('int_fund_holdings_lines') }}
    group by fund, report_date

)

select
    l.fund,
    l.report_date,
    l.filed,
    l.security_key,
    sum(l.val_usd) / t.total_usd as weight
from {{ ref('int_fund_holdings_lines') }} l
join totals t on t.fund = l.fund and t.report_date = l.report_date
where l.security_key is not null and t.total_usd > 0
group by l.fund, l.report_date, l.filed, l.security_key, t.total_usd
