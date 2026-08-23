
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select security_key
from "sdp"."main_staging"."stg_universe"
where security_key is null



  
  
      
    ) dbt_internal_test