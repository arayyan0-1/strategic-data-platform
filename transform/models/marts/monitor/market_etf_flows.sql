{# The newest NAV date of each ETF with net assets, flows over 1, 5 and 21 NAV dates, and the
   premium with its z-score against the 63 NAV dates before. #}

with daily as (

    select
        *,
        sum(flow) over (partition by fund order by date
                        rows between 4 preceding and current row)       as flow_5,
        sum(flow) over (partition by fund order by date
                        rows between 20 preceding and current row)      as flow_21,
        avg(premium) over w63                                           as premium_mean,
        stddev_samp(premium) over w63                                   as premium_sd,
        row_number() over (partition by fund order by date desc)        as recency
    from {{ ref('etf_daily') }}
    window w63 as (partition by fund order by date rows between 63 preceding and 1 preceding)

)

select
    d.fund,
    e.issuer,
    e.name,
    e.asset_class,
    e.sub_asset_class,
    e.benchmark,
    e.holdings_tracked,
    d.date,
    d.nav,
    d.total_net_assets,
    d.shares_outstanding,
    d.flow                                                              as flow_1,
    d.flow_5,
    d.flow_21,
    d.flow_21 / nullif(d.total_net_assets - d.flow_21, 0)               as flow_21_share,
    d.close,
    d.premium,
    (d.premium - d.premium_mean) / nullif(d.premium_sd, 0)              as premium_z
from daily d
join {{ ref('etfs') }} e using (fund)
where d.recency = 1
