
  
    
    

    create  table
      "f1"."main_gold"."driver_session_summary__dbt_tmp"
  
    as (
      WITH laps AS (
  SELECT * FROM "f1"."main_silver"."laps" WHERE laptime IS NOT NULL
),
driver_base AS (
  SELECT
    season,
    round,
    grand_prix,
    session_code,
    COALESCE(NULLIF(driver, ''), CAST(drivernumber AS VARCHAR)) AS driver,
    drivernumber AS driver_number,
    team,
    laptime,
    pitintime,
    pitouttime
  FROM laps
),
ranker AS (
  SELECT
    season, round, grand_prix, session_code, driver, driver_number, team, laptime,
    ROW_NUMBER() OVER (
      PARTITION BY season, round, grand_prix, session_code, driver, driver_number, team
      ORDER BY laptime ASC NULLS LAST
    ) AS rn
  FROM driver_base
),
pb AS (
  SELECT
    season, round, grand_prix, session_code, driver, driver_number, team,
    SUM(CASE WHEN rn = 1 THEN 1 ELSE 0 END) AS personal_best_laps
  FROM ranker
  GROUP BY 1,2,3,4,5,6,7
),
agg AS (
  SELECT
    season, round, grand_prix, session_code, driver, driver_number, team,
    COUNT(*) AS laps_total,
    SUM(CASE WHEN pitintime IS NULL AND pitouttime IS NULL THEN 1 ELSE 0 END) AS laps_on_track,
    SUM(CASE WHEN pitintime IS NOT NULL OR  pitouttime IS NOT NULL THEN 1 ELSE 0 END) AS pitstops,
    MIN(laptime) AS best_lap_time
  FROM driver_base
  GROUP BY 1,2,3,4,5,6,7
)
SELECT
  a.season, a.round, a.grand_prix, a.session_code,
  a.driver, a.driver_number, a.team,
  a.laps_total, a.laps_on_track, a.pitstops, a.best_lap_time,
  COALESCE(pb.personal_best_laps, 0) AS personal_best_laps
FROM agg a
LEFT JOIN pb
  ON  a.season       = pb.season
  AND a.round        = pb.round
  AND a.grand_prix   = pb.grand_prix
  AND a.session_code = pb.session_code
  AND a.driver       = pb.driver
  AND a.driver_number= pb.driver_number
  AND a.team         = pb.team
    );
  
  