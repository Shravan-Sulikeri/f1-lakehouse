select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select season
from "f1"."main_gold"."driver_session_summary"
where season is null



      
    ) dbt_internal_test