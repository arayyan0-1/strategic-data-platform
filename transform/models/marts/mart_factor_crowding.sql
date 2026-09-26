{#
  Crowding of each style from short interest. At each settlement, from the session it
  was public (effective_date), each in-universe name gets its short interest over its
  shares outstanding. crowding is the mean of that ratio in the bottom quintile of the
  style less the mean in the top quintile: short sellers who lean against the short leg
  are on the same side as the factor, so a high value is a crowded trade. crowding_z is
  crowding against its own history of earlier settlements (at least 12).
#}
{% set styles = ['size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                 'dividend_yield', 'high_52w'] %}

with si as (

    select
        u.security_key,
        s.effective_date                                    as date,
        s.short_interest / nullif(u.market_cap / nullif(u.close, 0), 0) as si_ratio
    from {{ ref('stg_short_interest') }} s
    join {{ ref('stg_universe') }} u
        on u.ticker = s.ticker and u.date = s.effective_date
    where s.effective_date is not null and u.in_universe and u.market_cap > 0

), joined as (

    select e.date, si.si_ratio,
           {% for s in styles %}e.z_{{ s }}{{ ',' if not loop.last }}{% endfor %}
    from si
    join {{ ref('mart_style_exposures') }} e
        on e.security_key = si.security_key and e.date = si.date
    where si.si_ratio between 0 and 1

), long_form as (

    unpivot joined
    on {% for s in styles %}z_{{ s }}{{ ',' if not loop.last }}{% endfor %}
    into name factor value z

), binned as (

    -- A z-score of exactly 0 is a missing characteristic. It is left out.
    select date, replace(factor, 'z_', '') as factor, si_ratio,
           ntile(5) over (partition by date, factor order by z) as quintile
    from long_form
    where z <> 0

), spread as (

    select
        date, factor,
        avg(si_ratio) filter (where quintile = 5)                  as si_top,
        avg(si_ratio) filter (where quintile = 1)                  as si_bottom,
        avg(si_ratio) filter (where quintile = 1)
            - avg(si_ratio) filter (where quintile = 5)            as crowding
    from binned
    group by date, factor

)

select
    *,
    case when count(*) over prev >= 12 then
        (crowding - avg(crowding) over prev) / nullif(stddev_samp(crowding) over prev, 0)
    end                                                            as crowding_z,
    count(*) over prev                                             as n_history
from spread
window prev as (partition by factor order by date rows between unbounded preceding and 1 preceding)
