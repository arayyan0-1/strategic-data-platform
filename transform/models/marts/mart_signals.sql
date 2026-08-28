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
        p.adj_open_split,
        p.adj_close_split,
        p.adj_close_total,
        p.dollar_volume
    from {{ ref('stg_prices_adjusted') }} p
    inner join {{ ref('stg_universe') }} u
        on u.ticker = p.ticker and u.date = p.date

    -- Collapse a when-issued line (KVUEw, JNJ.WD) that shares a share_class_figi
    -- with the regular line. Keep the more liquid line.
    qualify row_number() over (
        partition by u.security_key, p.date
        order by p.dollar_volume desc nulls last, p.ticker
    ) = 1

), returns as (

    select
        *,
        adj_close_total / nullif(lag(adj_close_total) over w, 0) - 1 as ret_1,
        adj_open_split  / nullif(lag(adj_close_split) over w, 0) - 1 as overnight_ret,
        close           / nullif(open, 0) - 1                        as intraday_ret
    from base
    window w as (partition by security_key order by date)

), factors as (

    select
        security_key,
        ticker,
        date,
        in_universe,
        dollar_volume,

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

        overnight_ret,
        intraday_ret,

        -- proximity to the 52 week high
        adj_close_total / nullif(max(adj_close_total) over (
            partition by security_key order by date
            rows between 251 preceding and current row), 0)       as hi_52w

    from returns
    window w as (partition by security_key order by date)

)

select * from factors
