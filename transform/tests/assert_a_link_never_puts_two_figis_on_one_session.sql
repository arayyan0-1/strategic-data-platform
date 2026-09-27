-- A link joins the FIGIs of a security one after the other. On each session, the rows of
-- a linked security come from one FIGI only.
select security_key, date, list(distinct episode_key) as episode_keys
from {{ ref('int_tickers_keyed') }}
where security_key in (select root_key from {{ ref('int_security_links') }})
group by security_key, date
having count(distinct episode_key) > 1
