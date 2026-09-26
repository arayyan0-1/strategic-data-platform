-- The model spread of an in-universe name is at least one tick and under 20%, it is
-- known when its volatility is known, almost every name has a volatility, and the median
-- spread of large names is below that of small names on the last session. A units error
-- or a wrong exponent breaks one of these. A new series (a new security key) has no
-- volatility for its first 21 sessions.
with last as (

    select max(date) as d from {{ ref('mart_spreads') }}

), recent as (

    select s.*, u.close, u.market_cap
    from {{ ref('mart_spreads') }} s
    join {{ ref('stg_universe') }} u
        on u.security_key = s.security_key and u.date = s.date and u.is_primary_line
    where s.in_universe
      and s.date > (select d from last) - interval 60 day

), bad as (

    select 'range' as check_name, security_key, date, spread
    from recent
    where (spread is null and sigma is not null)
       or spread < {{ var('tick_size') }} / close - 1e-12
       or spread > 0.2

), coverage as (

    select avg((sigma is null)::int) as no_sigma from recent

), order_by_size as (

    select
        median(spread) filter (where market_cap >= 1e10) as large,
        median(spread) filter (where market_cap < 2e9)   as small
    from recent
    where date = (select d from last)

)

select check_name, security_key, date, spread from bad
union all
select 'coverage', null, null, no_sigma from coverage where no_sigma > 0.01
union all
select 'size order', null, null, large from order_by_size where not (large < small)
