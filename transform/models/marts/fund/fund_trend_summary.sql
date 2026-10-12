{# A timing rule against buy and hold on each region and on the US market. At the end of month t
   the timed book holds the index in month t+1 when trend is above 0, and the T-bill otherwise.
   Both books earn their returns over the months from the first with a trend to the last with a
   next-month return. Returns are annualized geometrically, volatility is the monthly standard
   deviation times the root of 12, and the Sharpe ratio is on the return over the T-bill return.
   max_drawdown is the largest fall from a peak of the wealth of the book (negative). development
   is every formation month up to fund_signal_dev_end. #}

with months as (

    select
        index_key,
        month,
        trend > 0                                       as invested,
        fwd_ret                                         as hold_ret,
        fwd_ret - fwd_excess                            as cash_ret
    from {{ ref('fund_signal_history') }}
    where (universe = 'regions' or index_key = 'us:market')
      and trend is not null and fwd_ret is not null

), periods (period) as (

    values ('development'), ('all')

), sample as (

    select
        m.index_key,
        p.period,
        m.month,
        m.invested,
        m.hold_ret,
        m.cash_ret,
        case when m.invested then m.hold_ret else m.cash_ret end   as timed_ret
    from months m
    join periods p
        on p.period = 'all' or m.month <= cast('{{ var("fund_signal_dev_end") }}' as date)

), wealth as (

    select
        *,
        product(1 + timed_ret) over running                 as timed_wealth,
        product(1 + hold_ret) over running                  as hold_wealth
    from sample
    window running as (partition by index_key, period order by month
                       rows between unbounded preceding and current row)

), drawdown as (

    select
        *,
        timed_wealth / greatest(1, max(timed_wealth) over running) - 1   as timed_drawdown,
        hold_wealth / greatest(1, max(hold_wealth) over running) - 1     as hold_drawdown
    from wealth
    window running as (partition by index_key, period order by month
                       rows between unbounded preceding and current row)

)

select
    index_key,
    period,
    min(month)                                              as first_month,
    max(month)                                              as last_month,
    count(*)                                                as n_months,
    avg(invested::integer)                                  as share_invested,
    pow(product(1 + timed_ret), 12.0 / count(*)) - 1        as ann_return,
    stddev_samp(timed_ret) * sqrt(12)                       as ann_vol,
    avg(timed_ret - cash_ret) / stddev_samp(timed_ret - cash_ret) * sqrt(12) as sharpe,
    min(timed_drawdown)                                     as max_drawdown,
    pow(product(1 + hold_ret), 12.0 / count(*)) - 1         as bh_ann_return,
    stddev_samp(hold_ret) * sqrt(12)                        as bh_ann_vol,
    avg(hold_ret - cash_ret) / stddev_samp(hold_ret - cash_ret) * sqrt(12) as bh_sharpe,
    min(hold_drawdown)                                      as bh_max_drawdown
from drawdown
group by index_key, period
order by index_key, period
