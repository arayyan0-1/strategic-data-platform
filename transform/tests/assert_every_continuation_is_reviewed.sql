{# A returned row is a FIGI key change that passes structure and price, fails identity and has no
   reviewed decision. Warns only: until reviewed, the change starts a new security. #}
{{ config(severity='warn') }}
select ticker, link_date, old_name, name, old_cik, cik, same_cik, same_name, jump, vol
from {{ ref('int_key_changes') }}
where needs_review
