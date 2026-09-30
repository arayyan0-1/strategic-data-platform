{#
  The score of the move of each factor over 1 and 21 sessions, one row per factor,
  horizon and session, over the whole history. A score is as rare as a move of that many
  standard deviations of a normal distribution: a score of 3 is a move that the family
  passes on about 1 day in 370. Each family of factor_returns is its own pool.

  z_raw is the log return over h sessions divided by the volatility of the daily log
  returns known before the last session of the move, times the root of h
  (macros/scores.sql). z is the score: z_raw mapped to the normal quantile of the share of
  the pool of its family that is as large. Each z_raw uses trailing data only. The map
  uses the whole pool, as a table of rarity.

  A factor with no defined exposure has a return of zero within rounding. Its history
  starts at the first return larger than the var monitor_zero_return.
#}

with live as (

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
