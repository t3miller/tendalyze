import os
import sys
import tempfile

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


# ---------- DB helpers ----------

def get_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(db_url)


def get_games(conn):
    """Return basic info for each game that has plays."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT
                game_id,
                MIN(offense_team_id) AS offense_team_id,
                MIN(defense_team_id) AS defense_team_id
            FROM plays
            GROUP BY game_id
            ORDER BY game_id;
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

    # Tabs: Upload + Game Explorer
    upload_tab, explore_tab = st.tabs(["📤 Upload & Ingest", "📊 Game Explorer"])

    # ----- Upload tab -----
    with upload_tab:
        st.subheader("Upload a Hudl CSV")

        uploaded_file = st.file_uploader(
            "Choose a Hudl CSV file",
            type=["csv"],
            help="Export play-by-play from Hudl, then drop it here.",
        )

        if uploaded_file is not None:
            st.success(f"File selected: **{uploaded_file.name}**")

            if st.button("Ingest this game into Tendalyze"):
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                    tmp.write(uploaded_file.getvalue())
                    tmp_path = tmp.name

                try:
                    load_hudl_csv(tmp_path)
                    st.success("Ingestion complete! Plays have been loaded into Neon. ✅")
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

        if not games:
            st.info("No games found yet. Ingest a CSV in the Upload tab to get started.")
            return

        # Build nice labels like "Game 1 (O:10 vs D:20)"
        game_labels = [
            f"Game {g['game_id']} (O:{g['offense_team_id']} vs D:{g['defense_team_id']})"
            for g in games
        ]
        game_id_by_label = {label: g["game_id"] for label, g in zip(game_labels, games)}

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

        # Two-column layout for tables
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


if __name__ == "__main__":
    main()
