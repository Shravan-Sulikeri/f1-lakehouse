select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select driver
from "f1"."main_gold"."driver_session_summary"
where driver is null



      
    ) dbt_internal_test