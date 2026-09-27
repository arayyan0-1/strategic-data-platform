-- The industry returns are the industry coefficients less the market, which is their
-- cap-weighted mean. So their cap-weighted sum is 0 on each session. A different sum
-- means a wrong market factor or a wrong industry map.
{% set industries = ['NoDur', 'Durbl', 'Manuf', 'Enrgy', 'Chems', 'BusEq', 'Telcm',
                     'Utils', 'Shops', 'Hlth', 'Money', 'Other', 'Unknown'] %}
with days as (
    select date, lead(date) over (order by date) as next_date
    from (select distinct date from {{ ref('style_exposures') }})
), shares as (
    select d.next_date as date, e.industry, sum(e.weight_cap) / sum(sum(e.weight_cap)) over (partition by d.next_date) as share
    from {{ ref('style_exposures') }} e
    join days d using (date)
    where e.fwd_ret_1 is not null and e.weight > 0
    group by d.next_date, e.industry
)
select s.date, sum(s.share * case s.industry
    {% for c in industries %}when '{{ c }}' then f.ind_{{ c | lower }}
    {% endfor %}end) as weighted_sum
from shares s
join {{ ref('style_factor_returns') }} f using (date)
group by s.date
having abs(sum(s.share * case s.industry
    {% for c in industries %}when '{{ c }}' then f.ind_{{ c | lower }}
    {% endfor %}end)) > 1e-9
