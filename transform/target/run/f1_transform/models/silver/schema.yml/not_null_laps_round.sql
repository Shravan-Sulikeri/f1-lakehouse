select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select round
from "f1"."main_silver"."laps"
where round is null



      
    ) dbt_internal_test