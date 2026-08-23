
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ticker
from "sdp"."main_staging"."stg_short_interest"
where ticker is null



  
  
      
    ) dbt_internal_test