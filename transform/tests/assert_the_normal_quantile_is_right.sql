{# A returned row is a two-sided tail share whose normal quantile is not 1, 2, 3, 4 or 5 within
   1e-6. #}
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
