{# Signal values, one row per security and session. Windows count rows, not days. Labels are in
   forward_returns. #}

with base as (

    select
        u.security_key,
        p.ticker,
        p.date,
        u.in_universe,
        u.market_cap,
        p.open,
        p.close,
        p.total_factor,
        p.adj_close_total,
        p.dollar_volume
    from {{ ref('int_prices_adjusted') }} p
    inner join {{ ref('int_universe') }} u
        on u.ticker = p.ticker and u.date = p.date

    -- One row per security and session: its primary line (int_security_lines).
    where u.is_primary_line

), returns as (

    -- The overnight return uses the total-adjusted open, so an ex-dividend drop is not an
    -- overnight loss.
    select
        *,
        adj_close_total     / nullif(lag(adj_close_total) over w, 0) - 1 as ret_1,
        open * total_factor / nullif(lag(adj_close_total) over w, 0) - 1 as overnight_ret,
        close               / nullif(open, 0) - 1                        as intraday_ret,
        lag(market_cap) over w                                           as cap_before
    from base
    window w as (partition by security_key order by date)

), mkt as (

    -- Cap-weighted return of the in-universe names, weighted by the previous session's cap.
    select date, sum(ret_1 * cap_before) / sum(cap_before) as mkt_ret
    from returns
    where in_universe and ret_1 is not null and cap_before > 0
    group by date

), sec_divs as (

    -- A dividend belongs to the security that held the ticker on the last session on or before
    -- the ex-date, so a ticker change keeps the history. A key with only special rows drops out.
    select b.security_key, ca.event_date, ca.regular_cash_amount
    from (
        select ticker, event_date, regular_cash_amount
        from {{ ref('int_corporate_actions') }}
        where kind = 'dividend'
          and regular_cash_amount > 0
    ) ca
    asof join base b
        on b.ticker = ca.ticker
       and b.date  <= ca.event_date

), divs as (

    -- Trailing 12 months of regular cash dividends per security and session (nominal cash on
    -- the ex-date, so a split inside the window distorts it slightly). Non-payers get 0.
    select
        b.security_key, b.date,
        sum(d.regular_cash_amount) as div_ttm
    from base b
    left join sec_divs d
        on d.security_key = b.security_key
       and d.event_date  <= b.date
       and d.event_date   > b.date - interval 1 year
    group by b.security_key, b.date

), short as (

    -- Short interest per security and settlement, for the security that held the ticker on the
    -- settlement date. short_share is short value over the guarded cap of that date.
    select
        u.security_key,
        s.settlement_date,
        s.effective_date,
        s.short_interest,
        s.short_interest * u.close / nullif(u.market_cap, 0)   as short_share,
        s.days_to_cover_computed                               as short_days,
        lag(s.short_interest) over w                           as prev_short_interest,
        lag(s.settlement_date) over w                          as prev_settlement_date
    from {{ ref('int_short_interest') }} s
    inner join {{ ref('int_universe') }} u
        on u.ticker = s.ticker and u.date = s.settlement_date and u.is_primary_line
    where s.effective_date is not null
    window w as (partition by u.security_key order by s.settlement_date)

), joined as (

    select r.*, m.mkt_ret, coalesce(d.div_ttm, 0) as div_ttm,
           sh.effective_date as short_as_of, sh.settlement_date, sh.short_interest,
           sh.short_share, sh.short_days, sh.prev_short_interest, sh.prev_settlement_date
    from returns r
    left join mkt m on m.date = r.date
    left join divs d on d.security_key = r.security_key and d.date = r.date
    asof left join short sh
        on sh.security_key = r.security_key
       and sh.effective_date <= r.date

), factors as (

    select
        security_key,
        ticker,
        date,
        in_universe,
        dollar_volume,

        -- The total return of the session, from the close before. Not a signal.
        ret_1,

        -- reversal: the negative past return
        -1 * ret_1                                                as reversal_1,
        -1 * (adj_close_total / nullif(lag(adj_close_total, 5) over w, 0) - 1)
                                                                  as reversal_5,

        -- momentum, 12 months less the last one
        lag(adj_close_total, 21) over w
            / nullif(lag(adj_close_total, 252) over w, 0) - 1     as momentum_12_1,

        stddev_samp(ret_1) over (partition by security_key order by date
            rows between 19 preceding and current row)            as vol_20,
        stddev_samp(ret_1) over (partition by security_key order by date
            rows between 59 preceding and current row)            as vol_60,

        -- Amihud illiquidity
        avg(abs(ret_1) / nullif(dollar_volume, 0)) over (
            partition by security_key order by date
            rows between 19 preceding and current row)            as amihud_20,

        -- market beta over one year, to the cap-weighted market of the universe. Null
        -- with fewer than beta_min_obs returns: a beta from a short history is noise.
        case when count(ret_1 + mkt_ret) over (partition by security_key order by date
                  rows between 251 preceding and current row) >= {{ var('beta_min_obs') }}
             then covar_pop(ret_1, mkt_ret) over (partition by security_key order by date
                      rows between 251 preceding and current row)
                / nullif(var_pop(mkt_ret) over (partition by security_key order by date
                      rows between 251 preceding and current row), 0)
        end                                                       as beta_252,

        overnight_ret,
        intraday_ret,

        -- proximity to the 52 week high
        adj_close_total / nullif(max(adj_close_total) over (
            partition by security_key order by date
            rows between 251 preceding and current row), 0)       as high_52w,

        -- trailing 12-month regular dividend yield (nominal cash over unadjusted price)
        div_ttm / nullif(close, 0)                                as dividend_yield,

        -- Short interest from the last published settlement: share of shares sold short, days
        -- to cover, and the log change since the settlement before (at most 35 days earlier).
        -- Null when the last published settlement is over 45 days old.
        case when date - short_as_of <= 45 then short_share end   as short_ratio,
        case when date - short_as_of <= 45 then short_days end    as days_to_cover,
        case when date - short_as_of <= 45
                  and settlement_date - prev_settlement_date <= 35
                  and short_interest > 0 and prev_short_interest > 0
             then ln(short_interest / prev_short_interest) end     as short_change,
        short_as_of

    from joined
    window w as (partition by security_key order by date)

)

select * from factors
