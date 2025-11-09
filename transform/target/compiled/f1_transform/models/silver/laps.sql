with src as (
  select * from read_parquet('/opt/data/bronze/laps/**/*.parquet', hive_partitioning=1)
)
select
  try_cast(season as integer) as season,
  try_cast(round  as integer) as round,
  cast(grand_prix as varchar) as grand_prix,
  cast(session    as varchar) as session_code,
  * exclude (season, round, grand_prix, session)
from src