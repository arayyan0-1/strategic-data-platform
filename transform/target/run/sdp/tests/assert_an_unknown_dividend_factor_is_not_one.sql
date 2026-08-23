
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- A later dividend whose factor is null means the factor is unknown. Unknown is
-- not 1.0. adj_close_total must be null on those rows and not equal to the
-- split-adjusted price.
select ticker, date, next_dividend_date, dividend_factor, adj_close_total
from "sdp"."main_staging"."stg_prices_adjusted"
where next_dividend_date is not null
  and dividend_factor is null
  and adj_close_total is not null
  
  
      
    ) dbt_internal_test