{# A returned row is an asset and period where the gate is not the history seen a second way:
   n_up + n_down is not the number of formation months with a trend and a next month, or n_up,
   n_down, the means or their difference are more than 1e-9 from a recount. #}
with periods (period) as (

    values ('development'), ('all')

), sample as (

    select h.index_key, p.period, h.state, h.fwd_excess
    from {{ ref('fund_asset_history') }} h
    join periods p
        on p.period = 'all' or h.month <= cast('{{ var("fund_signal_dev_end") }}' as date)
    where h.trend is not null and h.fwd_excess is not null

), recount as (

    select
        index_key,
        period,
        count(*)                                                       as n,
        count(*) filter (where state = 1)                              as n_up,
        count(*) filter (where state = -1)                             as n_down,
        avg(fwd_excess) filter (where state = 1) * 12                  as mu_up,
        avg(fwd_excess) filter (where state = -1) * 12                 as mu_down,
        avg(fwd_excess) * 12                                           as mu_all
    from sample
    group by 1, 2

)

select g.index_key, g.period, g.n_up, g.n_down, r.n, r.n_up as recount_up, r.mu_up as recount_mu_up
from {{ ref('fund_asset_gate') }} g
left join recount r using (index_key, period)
where r.n is null
   or g.n_up + g.n_down <> r.n
   or g.n_up <> r.n_up
   or g.n_down <> r.n_down
   or (g.mu_up is null) <> (r.mu_up is null) or abs(g.mu_up - r.mu_up) > 1e-9
   or (g.mu_down is null) <> (r.mu_down is null) or abs(g.mu_down - r.mu_down) > 1e-9
   or abs(g.mu_all - r.mu_all) > 1e-9
   or abs(g.diff - (r.mu_up - r.mu_down)) > 1e-9
