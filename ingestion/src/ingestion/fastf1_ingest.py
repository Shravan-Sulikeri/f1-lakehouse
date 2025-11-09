from __future__ import annotations
import os
from loguru import logger
import pandas as pd
from slugify import slugify

import fastf1
from ingestion.utils import (
    get_env, season_list_from_env,
    snake_columns, dir_has_parquet, write_parquet
)

SESSION_CODES = ["FP1", "FP2", "FP3", "Q", "S", "R"]  # Sprint (S) + Race (R)

def partition_dir(root: str, table: str, season: int, round_no: int, gp_slug: str, code: str) -> str:
    return os.path.join(
        root, table,
        f"season={season}",
        f"round={round_no:02d}",
        f"grand_prix={gp_slug}",
        f"session={code}"
    )

def to_pandas_safe(obj) -> pd.DataFrame | None:
    if obj is None:
        return None
    if isinstance(obj, pd.DataFrame):
        return obj
    # Some fastf1 objects can be converted like this:
    try:
        if hasattr(obj, "to_frame"):
            df = obj.to_frame(index=False)
            if isinstance(df, pd.DataFrame):
                return df
    except Exception:
        pass
    try:
        return pd.DataFrame(obj)
    except Exception:
        return None

def main():
    cache_dir = os.getenv("F1_CACHE_DIR", "/opt/data/cache")
    bronze_root = os.getenv("F1_BRONZE", "/opt/data/bronze")
    seasons = season_list_from_env("F1_SEASONS")

    logger.info(f"Cache: {cache_dir}")
    logger.info(f"Bronze root: {bronze_root}")
    logger.info(f"Seasons: {seasons}")

    fastf1.Cache.enable_cache(cache_dir)

    for season in seasons:
        logger.info(f"=== Season {season} ===")
        try:
            schedule = fastf1.get_event_schedule(season, include_testing=False)
        except Exception as e:
            logger.error(f"Failed to get schedule for {season}: {e}")
            continue

        # schedule rows have 'RoundNumber' and 'EventName'
        for _, ev in schedule.iterrows():
            try:
                round_no = int(ev["RoundNumber"])
                gp_name = str(ev["EventName"])
            except Exception:
                # schedule formats can vary across versions
                try:
                    round_no = int(ev.get("RoundNumber") or ev.get("Round", 0))
                    gp_name = str(ev.get("EventName") or ev.get("Event", "unknown"))
                except Exception:
                    logger.warning(f"Skipping schedule row due to missing fields: {ev}")
                    continue

            gp_slug = slugify(gp_name or f"round-{round_no}")
            logger.info(f"Round {round_no:02d} – {gp_name} ({gp_slug})")

            for code in SESSION_CODES:
                try:
                    sess = fastf1.get_session(season, round_no, code)
                    sess.load(laps=True, telemetry=False, weather=True)
                except Exception as e:
                    logger.debug(f"No data for {season} R{round_no:02d} {code}: {e}")
                    continue

                # LAPS
                try:
                    laps = getattr(sess, "laps", None)
                    if laps is not None and not laps.empty:
                        laps = snake_columns(laps)
                        out = partition_dir(bronze_root, "laps", season, round_no, gp_slug, code)
                        if dir_has_parquet(out):
                            logger.info(f"laps exists, skip → {out}")
                        else:
                            write_parquet(laps, out)
                            logger.info(f"wrote laps → {out}")
                except Exception as e:
                    logger.warning(f"laps write failed: {e}")

                # WEATHER
                try:
                    weather = getattr(sess, "weather_data", None)
                    if weather is not None and not weather.empty:
                        weather = snake_columns(weather)
                        out = partition_dir(bronze_root, "weather", season, round_no, gp_slug, code)
                        if dir_has_parquet(out):
                            logger.info(f"weather exists, skip → {out}")
                        else:
                            write_parquet(weather, out)
                            logger.info(f"wrote weather → {out}")
                except Exception as e:
                    logger.warning(f"weather write failed: {e}")

                # RESULTS
                try:
                    results = to_pandas_safe(getattr(sess, "results", None))
                    if results is not None and not results.empty:
                        results = snake_columns(results)
                        out = partition_dir(bronze_root, "results", season, round_no, gp_slug, code)
                        if dir_has_parquet(out):
                            logger.info(f"results exists, skip → {out}")
                        else:
                            write_parquet(results, out)
                            logger.info(f"wrote results → {out}")
                except Exception as e:
                    logger.warning(f"results write failed: {e}")

    logger.info("✅ Bronze ingestion finished")

if __name__ == "__main__":
    main()
