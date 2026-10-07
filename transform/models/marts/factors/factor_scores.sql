{# Score of each factor's move over 1 and 21 sessions, per factor, horizon and session: a score
   of 3 is as rare as a normal 3 sigma move. Each family of factor_returns is its own pool. z_raw
   is trailing; only the map to z uses the pool. #}

with live as (

    -- A factor with no defined exposure returns zero within rounding; its history starts at
    -- the first return above monitor_zero_return.
    select
        family, factor, date, ret,
        max(abs(ret)) over (partition by family, factor order by date
                            rows between unbounded preceding and current row)
            > {{ var('monitor_zero_return') }} as started
    from {{ ref('factor_returns') }}
    where ret is not null

), moves as (

    select family, factor as subject, date, ln(1 + ret) as x, 0.0 as min_sd, true as elig
    from live
    where started and ret > -1

), raw as (

    select family, subject, h, date, elig, z_raw from {{ move_z('moves', [1, 21]) }}

)

select family, subject as factor, h, date, z_raw, z
from {{ calibrated('raw') }}
