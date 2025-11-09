import os
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px

WAREHOUSE = os.getenv("F1_WAREHOUSE", "/opt/data/warehouse/f1.duckdb")

@st.cache_resource(show_spinner=False)
def get_con():
    if not os.path.exists(WAREHOUSE):
        raise FileNotFoundError(f"Warehouse not found: {WAREHOUSE}")
    # read_only=False so temp objects work; file lives on external SSD mount
    return duckdb.connect(WAREHOUSE, read_only=False)

def resolve_schema(con, base_name: str, table_name: str) -> str | None:
    for schema in (f"main_{base_name}", base_name):
        try:
            con.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ? LIMIT 1",
                [schema, table_name]
            ).fetchone()
            con.execute(f"SELECT 1 FROM {schema}.{table_name} LIMIT 1")
            return schema
        except Exception:
            continue
    return None

def load_filters(con, silver_schema):
    seasons = con.sql(
        f"SELECT DISTINCT season FROM {silver_schema}.laps ORDER BY season"
    ).df()["season"].tolist()
    sessions = con.sql(
        f"SELECT DISTINCT session_code FROM {silver_schema}.laps ORDER BY session_code"
    ).df()["session_code"].tolist()
    return seasons, sessions

def ns_to_pretty_series(ns_series: pd.Series) -> pd.Series:
    """int ns → 'mm:ss.mmm' (string). Leaves non-numeric as-is."""
    s = pd.to_numeric(ns_series, errors="coerce")
    td = pd.to_timedelta(s, unit="ns")
    comp = td.dt.components
    return comp.apply(lambda r: f"{int(r.minutes):02d}:{int(r.seconds):02d}.{int(r.milliseconds):03d}", axis=1)

def ns_to_seconds(ns_series: pd.Series) -> pd.Series:
    s = pd.to_numeric(ns_series, errors="coerce")
    return s / 1e9

def safe_to_datetime(x):
    """Try several parses for date-like columns (string/date/timestamp)."""
    try:
        return pd.to_datetime(x)
    except Exception:
        # Last resort: numeric ns → ts
        xn = pd.to_numeric(x, errors="coerce")
        return pd.to_datetime(xn, unit="ns")

st.set_page_config(page_title="F1 Lakehouse Dashboard", layout="wide")
st.title("🏎️ F1 Lakehouse — Silver/Gold Explorer")
st.caption(f"Warehouse: {WAREHOUSE}")

# Connect + resolve schemas
try:
    con = get_con()
except Exception as e:
    st.error(f"Could not open DuckDB at {WAREHOUSE}: {e}")
    st.stop()

silver = resolve_schema(con, "silver", "laps")
gold   = resolve_schema(con, "gold",   "driver_session_summary")

if not silver or not gold:
    st.error(f"Could not resolve schemas. Found silver={silver}, gold={gold}. Run dbt build first.")
    st.stop()

# Sidebar filters
with st.sidebar:
    st.subheader("Filters")
    seasons, sessions = load_filters(con, silver)
    season = st.selectbox("Season", seasons, index=len(seasons)-1 if seasons else 0)
    session_code = st.selectbox("Session", sessions, index=sessions.index("R") if "R" in sessions else 0)

# Session date (formatted)
date_df = con.sql(
    f"""
    SELECT MIN(lapstartdate) AS session_date
    FROM {silver}.laps
    WHERE season = $season AND session_code = $session
    """,
    params={"season": season, "session": session_code}
).df()
if len(date_df) and pd.notna(date_df.loc[0, "session_date"]):
    session_date = safe_to_datetime(date_df.loc[0, "session_date"])
    st.caption(f"Session date: {session_date.strftime('%Y-%m-%d')}")
else:
    st.caption("Session date: N/A")

# KPIs
kpi = con.sql(
    f"""
    WITH laps AS (
      SELECT * FROM {silver}.laps
      WHERE season = $season AND session_code = $session
    )
    SELECT
      COUNT(*)::BIGINT AS total_laps,
      COUNT(DISTINCT driver)::BIGINT AS unique_drivers,
      COUNT(DISTINCT team)::BIGINT AS unique_teams
    FROM laps
    """,
    params={"season": season, "session": session_code}
).df()

col1, col2, col3 = st.columns(3)
col1.metric("Total Laps", f"{int(kpi['total_laps'][0]):,}")
col2.metric("Drivers", f"{int(kpi['unique_drivers'][0]):,}")
col3.metric("Teams", f"{int(kpi['unique_teams'][0]):,}")

st.markdown("---")
left, right = st.columns(2)

# ---------- Fastest laps (Gold) ----------
fastest = con.sql(
    f"""
    SELECT season, round, grand_prix, session_code, driver, best_lap_time
    FROM {gold}.driver_session_summary
    WHERE season = $season AND session_code = $session
    """,
    params={"season": season, "session": session_code}
).df()

if len(fastest):
    fastest["best_lap_pretty"]  = ns_to_pretty_series(fastest["best_lap_time"])
    fastest["best_lap_seconds"] = ns_to_seconds(fastest["best_lap_time"])

    # Table (clean columns)
    tbl = fastest[["season","round","grand_prix","session_code","driver","best_lap_pretty"]]\
          .sort_values(["best_lap_pretty"])
    left.subheader("Fastest laps (top 50)")
    left.dataframe(tbl.head(50), use_container_width=True)

    # Chart: Top-20 fastest
    top20 = fastest.sort_values("best_lap_seconds").head(20)
    fig_fast = px.bar(top20, x="driver", y="best_lap_seconds", title="Top-20 fastest laps (seconds)")
    left.plotly_chart(fig_fast, use_container_width=True, theme="streamlit")
else:
    left.info("No fastest-lap data for this selection.")

# ---------- Team event summary (Gold) ----------
team = con.sql(
    f"""
    SELECT season, round, grand_prix, session_code, team,
           team_laps_on_track, team_pitstops, team_best_lap_time
    FROM {gold}.team_event_summary
    WHERE season = $season AND session_code = $session
    ORDER BY round, team
    """,
    params={"season": season, "session": session_code}
).df()

if len(team):
    team["team_best_lap_pretty"]  = ns_to_pretty_series(team["team_best_lap_time"])
    team["team_best_lap_seconds"] = ns_to_seconds(team["team_best_lap_time"])

    right.subheader("Team event summary")
    right.dataframe(
        team[["season","round","grand_prix","session_code","team",
              "team_laps_on_track","team_pitstops","team_best_lap_pretty"]],
        use_container_width=True
    )

    # Chart: grouped bars (laps vs pitstops)
    fig_team = px.bar(
        team.sort_values(["round","team"]),
        x="team", y=["team_laps_on_track","team_pitstops"],
        barmode="group", title="Team laps vs pitstops"
    )
    right.plotly_chart(fig_team, use_container_width=True, theme="streamlit")
else:
    right.info("No team summary for this selection.")

st.markdown("---")

# ---------- Pace evolution (Silver) ----------
pace = con.sql(
    f"""
    SELECT lapnumber,
           /* median() works for numeric ns */
           median(laptime) AS median_laptime
    FROM {silver}.laps
    WHERE season = $season AND session_code = $session AND laptime IS NOT NULL
    GROUP BY lapnumber
    ORDER BY lapnumber
    """,
    params={"season": season, "session": session_code}
).df()

if len(pace):
    pace["median_laptime_seconds"] = ns_to_seconds(pace["median_laptime"])
    fig_pace = px.line(pace, x="lapnumber", y="median_laptime_seconds",
                       title="Session pace over laps (median lap time, seconds)")
    st.plotly_chart(fig_pace, use_container_width=True, theme="streamlit")
else:
    st.info("No pace data available for this selection.")

st.markdown("---")
st.caption("Change season/session in the sidebar. Times formatted as mm:ss.mmm; data read directly from the DuckDB on your external SSD.")