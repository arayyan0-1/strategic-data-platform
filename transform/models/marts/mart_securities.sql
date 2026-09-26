{#
  The security master: one row per security_key, with its current ticker, name and
  type (from its last session), its runs of tickers in order, and its first and last
  session. A reused ticker gives two rows, one for each security.
#}

with lines as (

    select l.security_key, l.ticker, l.date, l.key_rule, t.name, t.type_filled
    from {{ ref('stg_security_lines') }} l
    join {{ ref('stg_tickers') }} t using (ticker, date)
    where l.is_primary_line

), histories as (

    -- The runs of tickers in order, so a ticker that comes back shows twice.
    select security_key, string_agg(ticker, ' > ' order by first_date) as tickers
    from (
        select security_key, ticker, min(date) as first_date
        from (
            select
                security_key, ticker, date,
                row_number() over (partition by security_key order by date)
                  - row_number() over (partition by security_key, ticker order by date) as run
            from lines
        )
        group by security_key, ticker, run
    )
    group by security_key

)

select
    l.security_key,
    arg_max(l.ticker, l.date)          as ticker,
    arg_max(l.name, l.date)            as name,
    arg_max(l.type_filled, l.date)     as type,
    arg_max(l.key_rule, l.date)        as key_rule,
    min(l.date)                        as first_date,
    max(l.date)                        as last_date,
    count(*)                           as n_sessions,
    any_value(h.tickers)               as tickers
from lines l
join histories h using (security_key)
group by l.security_key
