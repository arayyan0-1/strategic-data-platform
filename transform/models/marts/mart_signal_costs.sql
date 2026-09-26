{#
  The cost of each quintile long-short, from the estimated spreads. Each leg is equal
  weight. A name that enters a leg is bought, and a name that leaves is sold, each at
  half its spread, so the cost of a leg on a session is the sum of those half spreads
  over the count of names. The cost is paid at the close of the formation session, the
  session of the gross return in mart_signal_ls_returns. Gross and net are daily.

  capacity is the size of each leg at which the trade in a name of the 10th percentile
  of dollar volume, among the names traded that session, is 5% of that volume. It is a
  rough limit before impact dominates. The spread cost does not include impact.
#}

with legs as (

    select
        signal, date, security_key,
        case when quintile = max(quintile) over w then 'top'
             when quintile = min(quintile) over w then 'bottom' end as leg
    from {{ ref('mart_signal_panel') }}
    window w as (partition by date, signal)
    qualify leg is not null

), sessions as (

    select date, row_number() over (order by date) as rn
    from (select distinct date from legs)

), numbered as (

    select l.*, s.rn from legs l join sessions s using (date)

), sizes as (

    select signal, leg, rn, count(*) as n from numbered group by all

), moves as (

    -- A name in the leg now and not before enters. A name in the leg before and not
    -- now leaves.
    select
        coalesce(c.signal, p.signal)            as signal,
        coalesce(c.leg, p.leg)                  as leg,
        coalesce(c.rn, p.rn + 1)                as rn,
        coalesce(c.security_key, p.security_key) as security_key,
        c.security_key is not null and p.security_key is null as enters,
        c.security_key is null and p.security_key is not null as leaves
    from numbered c
    full join numbered p
        on p.signal = c.signal and p.leg = c.leg and p.rn = c.rn - 1
       and p.security_key = c.security_key

), priced as (

    select m.*, s.date, coalesce(sp.spread, 0.01) as spread, u.adv
    from moves m
    join sessions s on s.rn = m.rn
    left join {{ ref('mart_spreads') }} sp
        on sp.security_key = m.security_key and sp.date = s.date
    left join {{ ref('stg_universe') }} u
        on u.security_key = m.security_key and u.date = s.date and u.is_primary_line
    -- The first session builds the legs from nothing. That is a start, not a trade.
    where (m.enters or m.leaves) and m.rn > 1

), leg_costs as (

    select
        p.signal, p.date, p.leg,
        coalesce(sum(p.spread / 2 / now_.n) filter (where p.enters), 0)
            + coalesce(sum(p.spread / 2 / before.n) filter (where p.leaves), 0) as cost,
        count(*) filter (where p.enters) / any_value(now_.n)                   as turnover,
        0.05 * any_value(now_.n) * quantile_cont(p.adv, 0.1)                   as capacity
    from priced p
    join sizes now_ on now_.signal = p.signal and now_.leg = p.leg and now_.rn = p.rn
    left join sizes before on before.signal = p.signal and before.leg = p.leg
                          and before.rn = p.rn - 1
    group by p.signal, p.date, p.leg

)

select
    r.signal,
    r.date,
    r.ls_ret                                                    as gross,
    coalesce(sum(c.cost), 0)                                    as cost,
    r.ls_ret - coalesce(sum(c.cost), 0)                         as net,
    avg(c.turnover)                                             as turnover,
    min(c.capacity)                                             as capacity
from {{ ref('mart_signal_ls_returns') }} r
left join leg_costs c on c.signal = r.signal and c.date = r.date
where r.ls_ret is not null
group by r.signal, r.date, r.ls_ret
