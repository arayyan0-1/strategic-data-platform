{# Spread as a fraction of price, modelled (no quotes on this plan) by Kyle-Obizhaeva invariance:
   half spread = spread_kappa0 (σ/0.02)(σ·dollar volume/8e5)^(-1/3), floored at one tick.
   spread_ar (Abdi-Ranaldo) is a diagnostic only. #}

with s as (

    -- Windows run over the security's full series; the type filter comes last so a brief
    -- vendor type change does not cut a window.
    select
        u.security_key,
        p.date,
        u.in_universe,
        u.type_filled,
        p.close,
        u.adv,
        ln(p.adj_close_split)                                   as c,
        (ln(p.adj_high_split) + ln(p.adj_low_split)) / 2        as eta
    from {{ ref('int_prices_adjusted') }} p
    join {{ ref('int_universe') }} u using (ticker, date)
    where u.is_primary_line
      and p.adj_low_split > 0 and p.adj_high_split > 0 and p.adj_close_split > 0

), terms as (

    select
        s.*,
        (c - eta) * (c - lead(eta) over (partition by security_key order by date)) as term,
        sqrt(avg(g.ret_1 * g.ret_1) over (
            partition by security_key order by date
            rows between 21 preceding and 1 preceding))            as sigma
    from s
    left join {{ ref('signals') }} g using (security_key, date)

), estimated as (

    select
        security_key,
        date,
        in_universe,
        type_filled,
        close,
        adv,
        sigma,
        sqrt(greatest(4 * avg(term) over (
            partition by security_key order by date
            rows between 21 preceding and 1 preceding), 0))         as spread_ar
    from terms

)

select
    security_key,
    date,
    in_universe,
    sigma,
    -- An unknown input gives an unknown spread, not the tick floor.
    case when sigma > 0 and adv > 0 and close > 0 then
        greatest(
            2 * {{ var('spread_kappa0') }} * (sigma / 0.02)
              * pow(sigma * adv / 800000.0, -1.0 / 3),
            {{ var('tick_size') }} / close)
    end                                                             as spread,
    spread_ar
from estimated
where type_filled = 'CS'
