-- The jump and the volatility of each link, recomputed from stg_prices_adjusted, agree
-- with the link table and are within the limit.
with old_bars as (

    select l.new_key, p.date, p.adj_close_split
    from {{ ref('stg_security_links') }} l
    inner join {{ ref('stg_tickers') }} k
        on k.ticker      = l.ticker
       and k.episode_key = l.old_key
       and k.date       <= l.last_old_bar
    inner join {{ ref('stg_prices_adjusted') }} p
        on p.ticker = k.ticker
       and p.date   = k.date

), old_returns as (

    select new_key, date, ret
    from (
        select
            new_key,
            date,
            ln(adj_close_split / lag(adj_close_split) over (partition by new_key order by date)) as ret
        from old_bars
    )
    where ret is not null
    qualify row_number() over (partition by new_key order by date desc)
        <= {{ var('link_vol_window') }}

), recomputed as (

    select
        l.new_key,
        l.jump,
        l.vol,
        ln(pn.adj_close_split / po.adj_close_split)   as jump_2,
        (select stddev_samp(r.ret) from old_returns r where r.new_key = l.new_key) as vol_2
    from {{ ref('stg_security_links') }} l
    inner join {{ ref('stg_prices_adjusted') }} po
        on po.ticker = l.ticker and po.date = l.last_old_bar
    inner join {{ ref('stg_prices_adjusted') }} pn
        on pn.ticker = l.ticker and pn.date = l.first_new_bar

)

select *
from recomputed
where not coalesce(abs(jump - jump_2) < 1e-9, false)
   or not coalesce(abs(vol - vol_2) < 1e-9, false)
   or not coalesce(abs(jump_2) <= {{ var('link_max_jump_sigma') }} * vol_2, false)
