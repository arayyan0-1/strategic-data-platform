{# Each candidate fund and session: what is known at that close. eligible needs
   fund_universe_min_returns daily returns in the last fund_min_returns rows and
   fund_universe_min_adv of trailing dollar volume. sigma is the root mean square of the last 63
   returns. half_spread is half the Kyle-Obizhaeva spread of spreads, with a spread of at least one
   tick. adj_open is the open on the total-return scale of adj_close. #}

with bars as (

    select
        c.fund,
        r.security_key,
        r.date,
        r.ret,
        r.adv,
        s.open,
        s.close,
        s.adj_close_total                                            as adj_close,
        s.open * s.adj_close_total / nullif(s.close, 0)              as adj_open,
        c.expense_ratio,
        count(r.ret) over w252                                       as n_returns,
        sqrt(avg(r.ret * r.ret) over w63)                            as sigma
    from {{ ref('int_fund_returns') }} r
    inner join {{ ref('fund_candidates') }} c
        on c.security_key = r.security_key and c.is_candidate
    inner join {{ ref('security_sessions') }} s
        on s.security_key = r.security_key and s.date = r.date
    window
        w252 as (partition by r.security_key order by r.date
                 rows between {{ var('fund_min_returns') - 1 }} preceding and current row),
        w63 as (partition by r.security_key order by r.date
                rows between 62 preceding and current row)

)

select
    fund,
    security_key,
    date,
    ret,
    open,
    close,
    adj_open,
    adj_close,
    adv,
    sigma,
    expense_ratio,
    n_returns,
    case when sigma > 0 and adv > 0 and close > 0 then
        greatest(
            {{ var('spread_kappa0') }} * (sigma / 0.02) * pow(sigma * adv / 800000.0, -1.0 / 3),
            {{ var('tick_size') }} / close / 2)
    end                                                              as half_spread,
    n_returns >= {{ var('fund_universe_min_returns') }}
        and coalesce(adv, 0) >= {{ var('fund_universe_min_adv') }}  as eligible
from bars
