{# One row per ETF and NAV date. SSGA's history where it has the day, else the first screener
   pull that showed that NAV. Shares outstanding come from the history, else the iShares
   holdings file of that date, else net assets over NAV. #}

with history as (

    select fund, date, nav, shares_outstanding, total_net_assets, 'ssga_history' as source
    from {{ ref('stg_ssga__nav_history') }}
    where nav is not null

), screener as (

    select fund, nav_date as date, nav, total_net_assets
    from {{ ref('stg_issuers__etf_funds') }}
    where nav is not null and nav_date is not null
    qualify row_number() over (partition by fund, nav_date order by pull_date) = 1

), file_shares as (

    select distinct fund, date, fund_shares_outstanding as shares_outstanding
    from {{ ref('stg_issuers__etf_holdings') }}
    where fund_shares_outstanding is not null

)

select * from history

union all

select
    s.fund,
    s.date,
    s.nav,
    coalesce(f.shares_outstanding, s.total_net_assets / s.nav)            as shares_outstanding,
    s.total_net_assets,
    case when f.shares_outstanding is not null then 'holdings_file' else 'screener' end
                                                                          as source
from screener s
left join file_shares f using (fund, date)
where not exists (select 1 from history h where h.fund = s.fund and h.date = s.date)
