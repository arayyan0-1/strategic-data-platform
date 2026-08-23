
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_date
from "sdp"."main_staging"."stg_corporate_actions"
where event_date is null



  
  
      
    ) dbt_internal_test