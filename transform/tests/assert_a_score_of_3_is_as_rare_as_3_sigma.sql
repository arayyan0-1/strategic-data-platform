-- On the history of its family, a score of 3 is passed on about 1 day in 370 (0.27%). A
-- family and a horizon fail when the share is under 0.15% or over 0.5%, or when the
-- family has no scores.
with expected(family, h) as (

    values
        ('etf', 1), ('etf', 5), ('etf', 21),
        ('rate', 1), ('rate_step', 1),
        ('relation', 21),
        ('specific', 1),
        ('style', 1), ('style', 21), ('industry', 1), ('industry', 21)

), scores as (

    select family, h, z from {{ ref('market_scores') }}
    union all
    select family, h, z from {{ ref('factor_scores') }}

), shares as (

    select family, h, count(*) as n, avg((abs(z) >= 3)::int) as share
    from scores
    group by family, h

)

select e.family, e.h, s.n, s.share
from expected e
left join shares s using (family, h)
where s.share is null or s.share not between 0.0015 and 0.005
