-- The p-value of each effect is the two-sided tail area of the standard normal density
-- beyond |t|. The check integrates the density from |t| to |t| + 10 by the midpoint rule
-- and allows a relative difference of 1e-4.
with steps as (

    select i, 0.0005 as h from range(0, 20000) as r(i)

), integrals as (

    select
        s.period, s.calendar, s.factor, s.bucket, s.t, s.p,
        2 * sum(exp(-power(abs(s.t) + (g.i + 0.5) * g.h, 2) / 2) / sqrt(2 * pi()) * g.h) as p_integral
    from {{ ref('market_seasonality') }} s
    cross join steps g
    where s.t is not null
    group by s.period, s.calendar, s.factor, s.bucket, s.t, s.p

)

select *
from integrals
where p is null
   or p not between 0 and 1
   or abs(p - p_integral) > 1e-4 * p_integral
