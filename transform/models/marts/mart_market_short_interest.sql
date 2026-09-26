{#
  Short interest of the in-universe names at the last settlement date, beside the
  settlement before. effective_date is the first session the number was public.
  days_to_cover is null where the vendor gave the 999.99 cap, so the computed ratio
  is kept too.
#}

with settlements as (

    select distinct settlement_date
    from {{ ref('stg_short_interest') }}
    order by settlement_date desc
    limit 2

), bounds as (

    select max(settlement_date) as last_s, min(settlement_date) as prev_s from settlements

), live as (

    select u.ticker, u.name, u.close, u.adv
    from {{ ref('stg_universe') }} u
    where u.date = (select max(date) from {{ ref('stg_universe') }}) and u.in_universe

), cur as (

    select * from {{ ref('stg_short_interest') }}
    where settlement_date = (select last_s from bounds)

), prev as (

    select ticker, short_interest from {{ ref('stg_short_interest') }}
    where settlement_date = (select prev_s from bounds)

)

select
    c.settlement_date,
    c.effective_date,
    c.ticker,
    l.name,
    l.close,
    c.short_interest,
    p.short_interest                                                    as prev_short_interest,
    c.short_interest / nullif(p.short_interest, 0) - 1                  as change,
    c.days_to_cover,
    c.days_to_cover_computed,
    c.short_interest * l.close                                          as short_value
from cur c
join live l using (ticker)
left join prev p using (ticker)
