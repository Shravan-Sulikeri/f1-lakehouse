select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select session_code
from "f1"."main_silver"."results"
where session_code is null



      
    ) dbt_internal_test