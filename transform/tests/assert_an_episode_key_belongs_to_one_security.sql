{# A returned row is an episode key on two securities: the holdings join on the key would fan a
   line out into two. #}
select episode_key, count(distinct security_key) as n_securities
from {{ ref('int_tickers_keyed') }}
group by 1
having count(distinct security_key) > 1
