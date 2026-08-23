
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select kind
from "sdp"."main_staging"."stg_corporate_actions"
where kind is null



  
  
      
    ) dbt_internal_test