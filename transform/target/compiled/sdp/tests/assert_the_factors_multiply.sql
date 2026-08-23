-- Splits and dividends are independent factors and they multiply. Two separate
-- as-of joins, then one product.
select ticker, date, split_factor, dividend_factor, total_factor
from "sdp"."main_staging"."stg_prices_adjusted"
where total_factor is distinct from (split_factor * dividend_factor)
   or adj_close_total is distinct from (close * split_factor * dividend_factor)