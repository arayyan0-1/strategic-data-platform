-- The normal quantile of the scores gives 1, 2, 3, 4 and 5 at the two-sided tail shares
-- of a normal distribution.
select *
from (
    select q, z, {{ two_sided_z('q') }} as got
    from (values
        (0.31731050786, 1.0),
        (0.04550026390, 2.0),
        (0.00269979606, 3.0),
        (0.0000633424836, 4.0),
        (0.000000573303, 5.0)
    ) t(q, z)
)
where abs(got - z) > 1e-6
