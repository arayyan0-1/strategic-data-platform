{# For each security, the listed ETFs that hold it in their newest file: weight, shares held
   and the share of the class's shares outstanding (from the newest month-end details). #}

with newest as (

    select fund, max(date) as date
    from {{ ref('int_etf_holdings') }}
    group by fund

), shares as (

    select security_key, shares
    from {{ ref('int_security_details') }}
    qualify row_number() over (partition by security_key order by month_end desc) = 1

)

select
    h.security_key,
    h.fund,
    e.name                                      as fund_name,
    h.date,
    h.weight,
    h.quantity,
    h.quantity / nullif(s.shares, 0)            as share_of_class
from {{ ref('int_etf_holdings') }} h
join newest n using (fund, date)
left join {{ ref('etfs') }} e on e.fund = h.fund
left join shares s using (security_key)
where h.security_key is not null
