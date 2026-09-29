-- A change of FIGI key that passes the structure and price tests of a link and fails
-- an identity test looks like one security, as when a company moves into a new holding
-- company. It needs a row in the seed security_link_decisions. This test warns and does
-- not stop the build: until the review, the change starts a new security.
{{ config(severity='warn') }}
select ticker, link_date, old_name, name, old_cik, cik, same_cik, same_name, jump, vol
from {{ ref('int_key_changes') }}
where needs_review
