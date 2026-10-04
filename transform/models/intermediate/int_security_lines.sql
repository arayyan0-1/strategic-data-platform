{#
  One row per ticker and session with a bar, with the security key. Two lines of one
  security can trade on one session (KVUE and KVUEw, JNJ and JNJ.WD, ANGI and ANGIV).
  is_primary_line picks one line per security and session, and every series of a
  security reads that line only.
#}
select
    b.ticker,
    b.date,
    k.security_key,
    k.key_rule,
    -- A when-issued or when-distributed line adds a suffix to the regular ticker, so
    -- the shortest ticker is the regular line. Then the more liquid line wins.
    row_number() over (
        partition by k.security_key, b.date
        order by length(b.ticker), b.close * b.volume desc nulls last, b.ticker
    ) = 1 as is_primary_line
from (
    select ticker, date, close, volume
    from {{ ref('stg_massive__day_aggs') }}
) b
inner join {{ ref('int_tickers_keyed') }} k
    on k.ticker = b.ticker
   and k.date   = b.date
