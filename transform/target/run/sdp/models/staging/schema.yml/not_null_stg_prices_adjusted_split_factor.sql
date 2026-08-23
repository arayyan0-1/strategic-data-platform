
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select split_factor
from "sdp"."main_staging"."stg_prices_adjusted"
where split_factor is null



  
  
      
    ) dbt_internal_test