{# The return a year that each piece of the set-and-forget portfolio loses against its long
   history, three ways. gap_measured, for a commodity fund, compares the fund with the World Bank
   price its history uses, both as monthly averages so the timing matches: the log growth of the
   fund's total-return close less that of the price, less the T-bill interest for a futures fund
   (its history already earns it), a year, from the first to the last month in which both exist
   and the fund trades 15 sessions or more. It holds the roll and every fund cost. gap_published
   is the roll of the published long-run study: each contract's carry less the mean T-bill rate
   over its sample, averaged over the piece's contracts. It has no fund cost. gap_mid is the mean
   of the two where both exist, else the measured gap: the measured window is five years and holds
   single events, the published one is long and has no fees. gap_good and gap_bad are the better
   and the worse of the two. A piece with the fee rule loses its expense ratio in all three. #}

with pieces as (

    select * from {{ ref('allocation_pieces') }}

), fees as (

    select p.piece, c.expense_ratio as fee
    from pieces p
    left join {{ ref('fund_candidates') }} c on c.fund = p.fund

), latest_key as (

    select ticker, arg_max(security_key, date) as security_key
    from {{ ref('security_sessions') }}
    where ticker in (select fund from pieces where cost_rule = 'measured')
    group by ticker

), fund_month as (

    select
        s.ticker                                           as fund,
        last_day(s.date)                                   as month,
        avg(s.adj_close_total)                             as price,
        count(*)                                           as n_sessions
    from {{ ref('security_sessions') }} s
    join latest_key k using (ticker, security_key)
    where s.adj_close_total > 0
    group by 1, 2

), spot_month as (

    select series, month, value as price
    from {{ ref('stg_worldbank__commodities') }}

), both_months as (

    select p.piece, p.kind, f.month, f.price as fund_price, w.price as spot_price
    from pieces p
    join fund_month f on f.fund = p.fund and f.n_sessions >= 15
    join spot_month w on w.series = p.price_series and w.month = f.month
    where p.cost_rule = 'measured'

), span as (

    select
        piece,
        kind,
        min(month)                                         as first_month,
        max(month)                                         as last_month,
        count(*)                                           as n_months,
        arg_min(fund_price, month)                         as fund_start,
        arg_max(fund_price, month)                         as fund_end,
        arg_min(spot_price, month)                         as spot_start,
        arg_max(spot_price, month)                         as spot_end
    from both_months
    group by 1, 2

), collateral as (

    select
        s.piece,
        sum(ln(1 + r.rf))                                  as log_cash
    from span s
    join {{ ref('rates') }} r
        on r.date > s.first_month and r.date <= s.last_month and r.rf is not null
    group by 1

), measured as (

    select
        s.piece,
        s.first_month,
        s.last_month,
        s.n_months,
        (ln(s.fund_end / s.fund_start) - ln(s.spot_end / s.spot_start)
         - case when s.kind = 'commodity' then c.log_cash else 0 end)
            / (datediff('month', s.first_month, s.last_month) / 12.0)   as gap_measured
    from span s
    left join collateral c using (piece)

), tbill as (

    select month, rf
    from {{ ref('int_index_returns') }}
    where index_key = 'cash:tbill'

), contract_roll as (

    select
        c.piece,
        c.contract,
        any_value(c.carry) - 12 * avg(t.rf)                as roll
    from {{ ref('published_commodity_carry') }} c
    join tbill t on t.month between c.sample_start and c.sample_end
    group by 1, 2

), published as (

    select piece, avg(roll) as gap_published
    from contract_roll
    group by 1

)

select
    p.piece,
    p.fund,
    p.kind,
    p.cost_rule,
    f.fee,
    m.first_month,
    m.last_month,
    m.n_months,
    m.gap_measured,
    u.gap_published,
    case when p.cost_rule = 'fee' then -f.fee
         else (m.gap_measured + coalesce(u.gap_published, m.gap_measured)) / 2 end    as gap_mid,
    case when p.cost_rule = 'fee' then -f.fee
         else greatest(m.gap_measured, coalesce(u.gap_published, m.gap_measured)) end
                                                                                as gap_good,
    case when p.cost_rule = 'fee' then -f.fee
         else least(m.gap_measured, coalesce(u.gap_published, m.gap_measured)) end
                                                                                as gap_bad
from pieces p
left join fees f using (piece)
left join measured m using (piece)
left join published u using (piece)
order by p.piece
