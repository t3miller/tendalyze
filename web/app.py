import os
import sys
import tempfile
from datetime import date

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st

# Page config must be first Streamlit call
st.set_page_config(page_title="Tendalyze", layout="wide")

# Make sure Python can find the project root (where the 'etl' package lives)
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from etl.ingest_hudl_csv import load_hudl_csv
from etl.ingest_teams_csv import load_teams_csv


# ---------- DB helpers ----------

def get_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(db_url)


def get_games(conn):
    """Return basic info for each game from the games table."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT
                game_id,
                offense_team_id,
                defense_team_id,
                game_date,
                season,
                week,
                venue,
                source
            FROM games
            ORDER BY
                game_date DESC NULLS LAST,
                game_id DESC;
            """
        )
        return cur.fetchall()


def get_game_summary(conn, game_id: int):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Total plays
        cur.execute(
            "SELECT COUNT(*) AS plays FROM plays WHERE game_id = %s;",
            (game_id,),
        )
        total_plays = cur.fetchone()["plays"]

        # Run / pass breakdown
        cur.execute(
            """
            SELECT play_type, COUNT(*) AS count
            FROM plays
            WHERE game_id = %s
            GROUP BY play_type
            ORDER BY count DESC;
            """,
            (game_id,),
        )
        run_pass = cur.fetchall()

        # Top formations
        cur.execute(
            """
            SELECT formation_norm, COUNT(*) AS count
            FROM plays
            WHERE game_id = %s
            GROUP BY formation_norm
            ORDER BY count DESC
            LIMIT 10;
            """,
            (game_id,),
        )
        top_formations = cur.fetchall()

        # Yards by down
        cur.execute(
            """
            SELECT down, AVG(yards_gained) AS avg_yards
            FROM plays
            WHERE game_id = %s
            GROUP BY down
            ORDER BY down;
            """,
            (game_id,),
        )
        yards_by_down = cur.fetchall()

        # Raw plays for download/table
        cur.execute(
            """
            SELECT *
            FROM plays
            WHERE game_id = %s
            ORDER BY drive_id, play_id;
            """,
            (game_id,),
        )
        plays_rows = cur.fetchall()

    return total_plays, run_pass, top_formations, yards_by_down, plays_rows


def get_team_lookup(conn):
    """Return a dict of team_id -> metadata (name, location, division, etc.)."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT team_id, team_name, city, state, division, region, district
            FROM teams
            ORDER BY team_name;
            """
        )
        rows = cur.fetchall()

    lookup = {}
    for r in rows:
        label = r["team_name"]
        city = r["city"]
        state = r["state"]
        if city and state:
            label += f" ({city}, {state})"
        elif state:
            label += f" ({state})"

        lookup[r["team_id"]] = {
            "label": label,
            "team_name": r["team_name"],
            "city": city,
            "state": state,
            "division": r["division"],
            "region": r["region"],
            "district": r["district"],
        }
    return lookup


# ---------- Main UI ----------

def main():
    # Sidebar
    st.sidebar.title("Tendalyze")
    st.sidebar.caption("Football tendencies for coaches.")
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Status**")

    db_url = os.getenv("DATABASE_URL")
    if db_url:
        st.sidebar.success("Connected to Neon ✅")
    else:
        st.sidebar.error("DATABASE_URL not set ❌")
        st.stop()

    st.sidebar.markdown("---")
    st.sidebar.caption("Made for Hudl play-by-play exports.")

    # Main title
    st.title("Tendalyze")
    st.write("Upload Hudl-style play-by-play and explore tendencies.")

    # Tabs: Upload + Game Explorer + Teams Admin + Tableau
    upload_tab, explore_tab, teams_tab, tableau_tab = st.tabs(
        ["📤 Upload & Ingest", "📊 Game Explorer", "🏫 Teams Admin", "📈 Tableau Dashboards"]
    )

    # ----- Upload tab -----
    with upload_tab:
        st.subheader("Upload a Hudl CSV")

        # Load teams
        try:
            conn = get_connection()
            with conn:
                team_lookup = get_team_lookup(conn)
        except Exception as e:
            st.error(f"Could not load teams: {e}")
            team_lookup = {}

        if not team_lookup:
            st.info("Add teams in the Teams Admin tab before uploading games.")
        else:
            # Build team dropdown choices
            team_choices = [
                (info["label"], team_id) for team_id, info in team_lookup.items()
            ]
            team_choices.sort(key=lambda x: x[0])

            st.markdown("#### Tag this game")
            your_label = st.selectbox(
                "Your team", [label for label,
