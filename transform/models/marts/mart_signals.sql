{#
  The factor battery. One row per security and session, keyed on security_key so
  a ticker change is one series. Windows count rows, not calendar days. Forward
  returns are kept out, in mart_forward_returns.
#}

with base as (

    select
        u.security_key,
        p.ticker,
        p.date,
        u.in_universe,
        p.open,
        p.close,
        p.total_factor,
        p.adj_close_total,
        p.dollar_volume
    from {{ ref('stg_prices_adjusted') }} p
    inner join {{ ref('stg_universe') }} u
        on u.ticker = p.ticker and u.date = p.date

    -- One row per security and session: its primary line (stg_security_lines).
    where u.is_primary_line

), returns as (

    -- The overnight return uses the total-adjusted open, so the drop on an
    -- ex-dividend date is not an overnight loss.
    select
        *,
        adj_close_total     / nullif(lag(adj_close_total) over w, 0) - 1 as ret_1,
        open * total_factor / nullif(lag(adj_close_total) over w, 0) - 1 as overnight_ret,
        close               / nullif(open, 0) - 1                        as intraday_ret
    from base
    window w as (partition by security_key order by date)

), mkt as (

    -- Equal-weight market proxy over the in-universe names each session. A
    -- cap-weight proxy waits on market_cap from the ticker-details ingest.
    select date, avg(ret_1) as mkt_ret
    from returns
    where in_universe and ret_1 is not null
    group by date

), sec_divs as (

    -- A dividend belongs to the security that held its ticker on the last session on
    -- or before the ex-date. A ticker change thus keeps the dividend history.
    select b.security_key, ca.event_date, ca.cash_amount
    from (
        select ticker, event_date, cash_amount
        from {{ ref('stg_corporate_actions') }}
        where kind = 'dividend'
          and cash_amount > 0
    ) ca
    asof join base b
        on b.ticker = ca.ticker
       and b.date  <= ca.event_date

), divs as (

    -- Trailing 12 months of cash dividends per security, per session. Nominal cash
    -- on the ex-date over the unadjusted price. A split inside the 12-month window
    -- is a rare, small distortion. Non-payers get 0.
    select
        b.security_key, b.date,
        sum(d.cash_amount) as div_ttm
    from base b
    left join sec_divs d
        on d.security_key = b.security_key
       and d.event_date  <= b.date
       and d.event_date   > b.date - interval 1 year
    group by b.security_key, b.date

), joined as (

    select r.*, m.mkt_ret, coalesce(d.div_ttm, 0) as div_ttm
    from returns r
    left join mkt m on m.date = r.date
    left join divs d on d.security_key = r.security_key and d.date = r.date

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

        -- market beta over one year, to the equal-weight market proxy
        covar_pop(ret_1, mkt_ret) over (partition by security_key order by date
            rows between 251 preceding and current row)
          / nullif(var_pop(mkt_ret) over (partition by security_key order by date
            rows between 251 preceding and current row), 0)       as beta_252,

        overnight_ret,
        intraday_ret,

        -- proximity to the 52 week high
        adj_close_total / nullif(max(adj_close_total) over (
            partition by security_key order by date
            rows between 251 preceding and current row), 0)       as hi_52w,

        -- trailing 12-month dividend yield (nominal cash over unadjusted price)
        div_ttm / nullif(close, 0)                                as div_yield

    from joined
    window w as (partition by security_key order by date)

)

select * from factors
