-- A split applies overnight. On the execution date all trading is already
-- adjusted. The as-of join must therefore use a strict comparison. If it uses
-- ">=" then a bar on the execution date selects that same event.
select ticker, date, next_split_date
from "sdp"."main_staging"."stg_prices_adjusted"
where next_split_date is not null
  and next_split_date <= date