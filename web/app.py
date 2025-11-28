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
                venue
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

    # Tabs: Upload + Game Explorer + Teams Admin
    upload_tab, explore_tab, teams_tab = st.tabs(
        ["📤 Upload & Ingest", "📊 Game Explorer", "🏫 Teams Admin"]
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
                "Your team", [label for label, _ in team_choices]
            )
            opp_label = st.selectbox(
                "Opponent team", [label for label, _ in team_choices]
            )

            your_team_id = next(
                tid for label, tid in team_choices if label == your_label
            )
            opponent_team_id = next(
                tid for label, tid in team_choices if label == opp_label
            )

            col_meta1, col_meta2, col_meta3 = st.columns(3)
            with col_meta1:
                game_date = st.date_input("Game date", value=date.today())
            with col_meta2:
                season = st.number_input(
                    "Season (year)",
                    min_value=2000,
                    max_value=2100,
                    value=date.today().year,
                )
            with col_meta3:
                week = st.number_input("Week", min_value=0, max_value=25, value=0)

            venue = st.text_input(
                "Venue (optional)", placeholder="Home stadium, city, etc."
            )

            st.markdown("---")

            uploaded_file = st.file_uploader(
                "Choose a Hudl CSV file",
                type=["csv"],
                help="Export play-by-play from Hudl, then drop it here.",
                key="hudl_csv",
            )

            if uploaded_file is not None:
                st.success(f"File selected: **{uploaded_file.name}**")

                if st.button("Ingest this game into Tendalyze"):
                    with tempfile.NamedTemporaryFile(
                        delete=False, suffix=".csv"
                    ) as tmp:
                        tmp.write(uploaded_file.getvalue())
                        tmp_path = tmp.name

                    try:
                        game_id = load_hudl_csv(
                            tmp_path,
                            your_team_id=your_team_id,
                            opponent_team_id=opponent_team_id,
                            game_date=game_date,
                            season=int(season),
                            week=int(week),
                            venue=venue or None,
                        )
                        st.success(
                            f"Ingestion complete! ✅ Game {game_id} created and plays loaded into Neon."
                        )
                    except Exception as e:
                        st.error(f"Error while ingesting this file: {e}")

    # ----- Game Explorer tab -----
    with explore_tab:
        st.subheader("Explore games already in Tendalyze")

        try:
            conn = get_connection()
        except Exception as e:
            st.error(f"Could not connect to database: {e}")
            return

        with conn:
            games = get_games(conn)
            team_lookup = get_team_lookup(conn)

        if not games:
            st.info(
                "No games found yet. Ingest a CSV in the Upload tab to get started."
            )
            return

        if not team_lookup:
            st.info(
                "No teams found yet. Upload teams in the Teams Admin tab to see team names."
            )

        # Build filters from team metadata
        states = sorted(
            {info["state"] for info in team_lookup.values() if info["state"]}
        )
        divisions = sorted(
            {info["division"] for info in team_lookup.values() if info["division"]}
        )

        col_filters = st.columns(2)
        with col_filters[0]:
            state_filter = st.multiselect("Filter by state", states)
        with col_filters[1]:
            division_filter = st.multiselect("Filter by division", divisions)

        def format_team(team_id):
            info = team_lookup.get(team_id)
            if not info:
                return f"Team {team_id}"
            return info["label"]

        # Apply filters to games
        def game_passes_filters(g):
            your_info = team_lookup.get(g["offense_team_id"])
            opp_info = team_lookup.get(g["defense_team_id"])

            def team_matches(info):
                if not info:
                    return False
                ok_state = not state_filter or info["state"] in state_filter
                ok_div = not division_filter or info["division"] in division_filter
                return ok_state and ok_div

            if not state_filter and not division_filter:
                return True

            return team_matches(your_info) or team_matches(opp_info)

        filtered_games = [g for g in games if game_passes_filters(g)]

        if not filtered_games:
            st.info("No games match the current filters.")
            return

        # Build labels like "Your Team vs Opponent (Season Week, Date)"
        game_labels = []
        for g in filtered_games:
            your_name = format_team(g["offense_team_id"])
            opp_name = format_team(g["defense_team_id"])
            label = f"{your_name} vs {opp_name} (Game {g['game_id']})"
            meta_parts = []
            if g["season"]:
                meta_parts.append(str(g["season"]))
            if g["week"] is not None:
                meta_parts.append(f"Week {g['week']}")
            if g["game_date"]:
                meta_parts.append(g["game_date"].strftime("%Y-%m-%d"))
            if meta_parts:
                label += " - " + ", ".join(meta_parts)
            game_labels.append(label)

        game_id_by_label = {
            label: g["game_id"] for label, g in zip(game_labels, filtered_games)
        }

        choice = st.selectbox("Select a game", game_labels)
        selected_game = game_id_by_label[choice]

        with conn:
            (
                total_plays,
                run_pass,
                top_formations,
                yards_by_down,
                plays_rows,
            ) = get_game_summary(conn, selected_game)

        # Metrics row
        st.markdown("### Game summary")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total plays", total_plays)

        with col2:
            if run_pass:
                run_row = next(
                    (r for r in run_pass if (r["play_type"] or "").lower() == "run"),
                    None,
                )
                pass_row = next(
                    (r for r in run_pass if (r["play_type"] or "").lower() == "pass"),
                    None,
                )
                pieces = []
                if run_row:
                    pieces.append(f"Run: {run_row['count']}")
                if pass_row:
                    pieces.append(f"Pass: {pass_row['count']}")
                st.write(" / ".join(pieces) if pieces else "No run/pass split available.")

        # Two-column layout for tendencies
        st.markdown("### Tendencies")
        tcol1, tcol2 = st.columns(2)

        with tcol1:
            st.markdown("**Top formations (by count)**")
            if top_formations:
                tf_df = pd.DataFrame(top_formations)
                st.table(tf_df)
            else:
                st.write("No formations found for this game.")

        with tcol2:
            st.markdown("**Yards per down**")
            if yards_by_down:
                yd_df = pd.DataFrame(yards_by_down)
                st.table(yd_df)
            else:
                st.write("No yardage data found for this game.")

        # Raw plays + download
        st.markdown("### All plays for this game")
        plays_df = pd.DataFrame(plays_rows)
        st.dataframe(plays_df, use_container_width=True, hide_index=True)

        csv_bytes = plays_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download plays as CSV",
            data=csv_bytes,
            file_name=f"tendalyze_game_{selected_game}.csv",
            mime="text/csv",
        )

    # ----- Teams Admin tab -----
    with teams_tab:
        st.subheader("Upload teams into Tendalyze")

        st.write(
            "Upload a CSV of teams with columns: "
            "`team_name, mascot, city, state, division, region, district`."
        )

        teams_file = st.file_uploader(
            "Choose a Teams CSV file",
            type=["csv"],
            key="teams_csv",
        )

        if teams_file is not None:
            st.success(f"File selected: **{teams_file.name}**")

            if st.button("Upload Teams"):
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                    tmp.write(teams_file.getvalue())
                    tmp_path = tmp.name

                try:
                    inserted, skipped = load_teams_csv(tmp_path)
                    st.success(
                        f"Teams uploaded! ✅ Inserted: {inserted}, "
                        f"Skipped (already existed): {skipped}"
                    )
                except Exception as e:
                    st.error(f"Error while uploading teams: {e}")

        # Show current teams
        try:
            conn = get_connection()
            with conn:
                with conn.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                ) as cur:
                    cur.execute(
                        """
                        SELECT team_name, mascot, city, state, division, region, district
                        FROM teams
                        ORDER BY state, division, team_name;
                        """
                    )
                    teams_rows = cur.fetchall()
            if teams_rows:
                st.markdown("### Teams currently in Tendalyze")
                teams_df = pd.DataFrame(teams_rows)
                st.dataframe(teams_df, use_container_width=True, hide_index=True)
            else:
                st.info("No teams in the database yet.")
        except Exception as e:
            st.error(f"Could not load teams: {e}")


if __name__ == "__main__":
    main()
