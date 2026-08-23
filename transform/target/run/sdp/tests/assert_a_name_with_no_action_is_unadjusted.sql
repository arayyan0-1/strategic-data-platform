
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select ticker, date, close, adj_close_split, adj_close_total
from "sdp"."main_staging"."stg_prices_adjusted"
where next_split_date is null
  and next_dividend_date is null
  and (adj_close_split <> close or adj_close_total is distinct from close)
  
  
      
    ) dbt_internal_test