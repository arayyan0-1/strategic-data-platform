{# A returned row is an index month where fwd_ret is not the return of the next calendar month, or
   fwd_excess is not that return less the T-bill return of that month. A missing next month gives
   null for both. #}
select h.index_key, h.month, h.fwd_ret, n.ret as next_ret, h.fwd_excess, n.ret - n.rf as next_excess
from {{ ref('fund_signal_history') }} h
left join {{ ref('int_index_returns') }} n
    on n.index_key = h.index_key and n.month = last_day(h.month + interval 1 month)
where h.fwd_ret is distinct from n.ret
   or h.fwd_excess is distinct from n.ret - n.rf
