-- fwd_ret_1 on D must be the total return from the close of D to the close of the
-- next session of the same security. This test finds that session by row number, not
-- by lead(), on the rows of mart_signals over the last 60 sessions.
with sessions as (

    select distinct date
    from {{ ref('mart_signals') }}
    order by date desc
    limit 60

), indexed as (

    select
        s.security_key,
        s.date,
        p.adj_close_total,
        row_number() over (partition by s.security_key order by s.date) as n
    from {{ ref('mart_signals') }} s
    inner join {{ ref('stg_prices_adjusted') }} p
        on p.ticker = s.ticker and p.date = s.date
    where s.date >= (select min(date) from sessions)

), recomputed as (

    select
        cur.security_key,
        cur.date,
        nxt.adj_close_total / nullif(cur.adj_close_total, 0) - 1 as fwd_ret_1
    from indexed cur
    left join indexed nxt
        on nxt.security_key = cur.security_key and nxt.n = cur.n + 1

)

select
    r.security_key,
    r.date,
    f.fwd_ret_1,
    r.fwd_ret_1 as fwd_ret_1_recomputed
from recomputed r
inner join {{ ref('mart_forward_returns') }} f
    on f.security_key = r.security_key and f.date = r.date
where (f.fwd_ret_1 is null) <> (r.fwd_ret_1 is null)
   or abs(f.fwd_ret_1 - r.fwd_ret_1) > 1e-9 * greatest(abs(f.fwd_ret_1), abs(r.fwd_ret_1))
