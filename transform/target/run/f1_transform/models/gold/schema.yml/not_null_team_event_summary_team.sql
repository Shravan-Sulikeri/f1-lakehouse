select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select team
from "f1"."main_gold"."team_event_summary"
where team is null



      
    ) dbt_internal_test