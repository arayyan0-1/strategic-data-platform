{# Daily NAV, shares outstanding, flow and premium per ETF. A flow is the change in shares
   outstanding times NAV between consecutive NAV dates, so a gap in the series lumps days.
   premium is the close over NAV, less one. #}

with daily as (

    select
        *,
        -- A share split moves NAV and shares inversely with net assets flat; its share
        -- change is not a flow.
        abs(ln(nav / lag(nav) over w)) > 0.2
            and abs(ln(total_net_assets / nullif(lag(total_net_assets) over w, 0))) < 0.1
            as split_like
    from {{ ref('int_etf_daily') }}
    window w as (partition by fund order by date)

), prices as (

    select ticker, date, close
    from {{ ref('int_prices_adjusted') }}
    where ticker in (select distinct fund from daily)

)

select
    d.fund,
    d.date,
    d.nav,
    d.shares_outstanding,
    d.total_net_assets,
    d.source,
    p.close,
    p.close / d.nav - 1                                                         as premium,
    case when not coalesce(d.split_like, false) then
        (d.shares_outstanding - lag(d.shares_outstanding) over w) * d.nav end    as flow,
    case when not coalesce(d.split_like, false) then
        d.shares_outstanding / nullif(lag(d.shares_outstanding) over w, 0) - 1 end
                                                                                as flow_share
from daily d
left join prices p on p.ticker = d.fund and p.date = d.date
window w as (partition by d.fund order by d.date)
