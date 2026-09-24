-- reversal_5, momentum_12_1 and vol_20 on D must use closes up to D and no later.
-- This test recomputes them with joins on the session index of each security, not
-- with lag() or window frames, and compares over the last 60 sessions.
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

), targets as (

    select *
    from indexed
    where date >= (select min(date) from sessions)

), prices as (

    select
        t.security_key,
        t.date,
        -1 * (t.adj_close_total / nullif(l5.adj_close_total, 0) - 1) as reversal_5,
        l21.adj_close_total / nullif(l252.adj_close_total, 0) - 1    as momentum_12_1
    from targets t
    left join indexed l5
        on l5.security_key = t.security_key and l5.n = t.n - 5
    left join indexed l21
        on l21.security_key = t.security_key and l21.n = t.n - 21
    left join indexed l252
        on l252.security_key = t.security_key and l252.n = t.n - 252

), vol as (

    -- The 20 returns that end at D: offset k takes the return of session n - k.
    select
        t.security_key,
        t.date,
        stddev_samp(cur.adj_close_total / nullif(prv.adj_close_total, 0) - 1) as vol_20
    from targets t
    cross join range(20) as k(k)
    inner join indexed cur
        on cur.security_key = t.security_key and cur.n = t.n - k.k
    left join indexed prv
        on prv.security_key = t.security_key and prv.n = t.n - k.k - 1
    group by t.security_key, t.date

), recomputed as (

    select p.*, v.vol_20
    from prices p
    inner join vol v
        on v.security_key = p.security_key and v.date = p.date

)

select
    s.security_key,
    s.date,
    s.reversal_5,    r.reversal_5    as reversal_5_recomputed,
    s.momentum_12_1, r.momentum_12_1 as momentum_12_1_recomputed,
    s.vol_20,        r.vol_20        as vol_20_recomputed
from recomputed r
inner join {{ ref('mart_signals') }} s
    on s.security_key = r.security_key and s.date = r.date
where false
{%- for c in ['reversal_5', 'momentum_12_1', 'vol_20'] %}
   or (s.{{ c }} is null) <> (r.{{ c }} is null)
   or abs(s.{{ c }} - r.{{ c }}) > 1e-9 * greatest(abs(s.{{ c }}), abs(r.{{ c }}))
{%- endfor %}
