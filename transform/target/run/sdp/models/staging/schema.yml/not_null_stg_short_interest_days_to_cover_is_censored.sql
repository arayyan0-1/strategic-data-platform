
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select days_to_cover_is_censored
from "sdp"."main_staging"."stg_short_interest"
where days_to_cover_is_censored is null



  
  
      
    ) dbt_internal_test