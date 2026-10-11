{# The ETF master: one row per fund from its newest screener row, whether its holdings are
   collected, and the span of its daily series. #}

with latest as (

    select *
    from {{ ref('stg_issuers__etf_funds') }}
    qualify row_number() over (partition by fund order by pull_date desc) = 1

), series as (

    select fund, min(date) as first_date, max(date) as last_date, count(*) as n_days
    from {{ ref('int_etf_daily') }}
    group by fund

)

select
    l.fund,
    l.issuer,
    l.name,
    l.asset_class,
    l.sub_asset_class,
    l.market_type,
    l.region,
    l.country,
    l.benchmark,
    l.inception_date,
    l.expense_ratio,
    l.total_net_assets,
    l.nav_date                                   as last_nav_date,
    h.fund is not null                           as holdings_tracked,
    s.first_date,
    s.last_date,
    s.n_days
from latest l
left join {{ ref('etf_holdings_funds') }} h using (fund)
left join series s using (fund)
