
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- A duplicate (ticker, date) means the as-of join made one price into two, which
-- happens when two corporate actions share a ticker and a date. The model must
-- collapse them before the join.
select ticker, date, count(*) as n
from "sdp"."main_staging"."stg_prices_adjusted"
group by 1, 2
having count(*) > 1
  
  
      
    ) dbt_internal_test