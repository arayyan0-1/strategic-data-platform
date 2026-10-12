{# The features of fund_index_features for each candidate fund and month, from its spliced
   monthly series (int_fund_monthly_returns). mom, val and trend are those of fund_signals. The
   universe is the one of the index the fund maps to in fund_index_map (a fund of us:market is in
   us_segments); a fund with no mapped equity index is left out. A row needs mom and trend, as in
   the index panel. There is no target: the table also holds the months to forecast. #}

with returns as (

    select
        fund,
        month,
        year(month) * 12 + month(month)       as month_number,
        ret
    from {{ ref('int_fund_monthly_returns') }}

), windowed as (

    select
        fund,
        month,
        product(1 + ret) over w3 - 1          as r3,
        count(ret) over w3                    as n3,
        product(1 + ret) over w6 - 1          as r6,
        count(ret) over w6                    as n6,
        stddev_samp(ret) over w12             as vol12,
        count(ret) over w12                   as n12
    from returns
    window
        w3 as (partition by fund order by month_number
               range between 2 preceding and current row),
        w6 as (partition by fund order by month_number
               range between 5 preceding and current row),
        w12 as (partition by fund order by month_number
                range between 11 preceding and current row)

), paths as (

    -- The wealth path of the 12 months that end at each month.
    select
        a.fund,
        a.month,
        b.month_number,
        product(1 + b.ret) over (partition by a.fund, a.month order by b.month_number)
                                              as wealth
    from returns a
    join returns b
        on b.fund = a.fund and b.month_number between a.month_number - 11 and a.month_number

), peaks as (

    select
        fund,
        month,
        wealth,
        greatest(1.0, max(wealth) over (partition by fund, month order by month_number))
                                              as peak
    from paths

), drawdown as (

    select fund, month, min(wealth / peak - 1) as dd12
    from peaks
    group by 1, 2

), universes as (

    select
        fund,
        case when index_key like 'region:%'  then 'regions'
             when index_key like 'country:%' then 'countries'
             when index_key like 'us:%'      then 'us_segments'
        end                                   as universe
    from {{ ref('fund_index_map') }}

)

select
    s.fund,
    u.universe,
    s.month,
    w.r3,
    w.r6,
    s.mom,
    s.val,
    w.vol12,
    d.dd12,
    s.trend
from {{ ref('fund_signals') }} s
join universes u using (fund)
join windowed w using (fund, month)
join drawdown d using (fund, month)
where u.universe is not null
  and s.mom is not null
  and s.trend is not null
  and w.n3 = 3 and w.n6 = 6 and w.n12 = 12
order by s.fund, s.month
