
  
    
    

    create  table
      "f1"."main_gold"."team_event_summary__dbt_tmp"
  
    as (
      WITH driver AS (
  SELECT * FROM "f1"."main_gold"."driver_session_summary"
),
race_only AS (
  SELECT * FROM driver WHERE session_code IN ('R','Q','S')
),
team_agg AS (
  SELECT
    season, round, grand_prix, session_code,
    team,
    SUM(laps_on_track) AS team_laps_on_track,
    SUM(pitstops)      AS team_pitstops,
    MIN(best_lap_time) AS team_best_lap_time
  FROM race_only
  GROUP BY 1,2,3,4,5
)
SELECT * FROM team_agg
    );
  
  