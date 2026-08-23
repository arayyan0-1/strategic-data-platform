
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select settlement_date
from "sdp"."main_staging"."stg_short_interest"
where settlement_date is null



  
  
      
    ) dbt_internal_test