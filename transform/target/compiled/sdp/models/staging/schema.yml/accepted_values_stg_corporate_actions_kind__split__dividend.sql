
    
    

with all_values as (

    select
        kind as value_field,
        count(*) as n_records

    from "sdp"."main_staging"."stg_corporate_actions"
    group by kind

)

select *
from all_values
where value_field not in (
    'split','dividend'
)


