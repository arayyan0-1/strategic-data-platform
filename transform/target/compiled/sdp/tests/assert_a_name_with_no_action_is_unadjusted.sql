select ticker, date, close, adj_close_split, adj_close_total
from "sdp"."main_staging"."stg_prices_adjusted"
where next_split_date is null
  and next_dividend_date is null
  and (adj_close_split <> close or adj_close_total is distinct from close)