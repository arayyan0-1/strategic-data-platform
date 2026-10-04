{#
  The security master: one row per security_key, with its current ticker, name and
  type (from its last session), its runs of tickers in order, and its first and last
  session. A reused ticker gives two rows, one for each security.
#}

with lines as (

    select security_key, ticker, date, key_rule
    from {{ ref('int_security_lines') }}
    where is_primary_line

), summary as (

    select
        security_key,
        arg_max(ticker, date)     as ticker,
        arg_max(key_rule, date)   as key_rule,
        min(date)                 as first_date,
        max(date)                 as last_date,
        count(*)                  as n_sessions,
        min(ticker) = max(ticker) as one_ticker
    from lines
    group by security_key

), runs as (

    -- The runs of tickers in order, so a ticker that comes back shows twice. A security
    -- with one ticker has one run, so only the other securities need the order.
    select security_key, ticker, date
    from lines
    semi join (select security_key from summary where not one_ticker) using (security_key)
    qualify ticker is distinct from lag(ticker) over (partition by security_key order by date)

), histories as (

    select security_key, string_agg(ticker, ' > ' order by date) as tickers
    from runs
    group by security_key

)

select
    s.security_key,
    s.ticker,
    t.name,
    t.type_filled                 as type,
    s.key_rule,
    s.first_date,
    s.last_date,
    s.n_sessions,
    coalesce(h.tickers, s.ticker) as tickers
from summary s
-- The name and the type of the last session.
inner join {{ ref('int_tickers_keyed') }} t
    on t.ticker = s.ticker
   and t.date   = s.last_date
left join histories h
    on h.security_key = s.security_key
