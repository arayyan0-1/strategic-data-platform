
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        key_rule as value_field,
        count(*) as n_records

    from "sdp"."main_staging"."stg_universe"
    group by key_rule

)

select *
from all_values
where value_field not in (
    'share_class_figi','composite_figi','cik_ticker','ticker_only'
)



  
  
      
    ) dbt_internal_test