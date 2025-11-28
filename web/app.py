import os
import sys
import tempfile

import streamlit as st
import psycopg2
import psycopg2.extras

# Make sure Python can find the project root (where the 'etl' package lives)
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from etl.ingest_hudl_csv import load_hudl_csv


def get_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(db_url)


def main():
    st.title("Tendalyze")

    st.write(
        "Upload a Hudl-style play-by-play CSV and Tendalyze will load it "
        "into the database for use in your Tableau dashboards."
    )

    db_url = os.getenv("DATABASE_URL")
    if db_url:
        st.success("DATABASE_URL is set. Backend can talk to Neon. ✅")
    else:
        st.error("DATABASE_URL is NOT set on this machine. ❌")
        st.stop()

    st.markdown("---")

    # ===== Upload & ingest section =====
    uploaded_file = st.file_uploader(
        "Choose a Hudl CSV file",
        type=["csv"],
        help="Export a play-by-play CSV from Hudl and upload it here.",
    )

    if uploaded_file is not None:
        st.write(f"File selected: **{uploaded_file.name}**")

        if st.button("Ingest this game into Tendalyze"):
            # Save uploaded file to a temporary location
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name

            try:
                load_hudl_csv(tmp_path)
                st.success("Ingestion complete! Plays have been loaded into Neon. ✅")
            except Exception as e:
                st.error(f"Error while ingesting this file: {e}")

    st.markdown("---")
    st.header("Game Explorer")

    # ===== Game explorer section =====
    try:
        conn = get_connection()
    except Exception as e:
        st.error(f"Could not connect to database: {e}")
        return

    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Get list of games that have plays
            cur.execute("SELECT DISTINCT game_id FROM plays ORDER BY game_id;")
            game_rows = cur.fetchall()

    if not game_rows:
        st.info("No games found yet. Ingest a CSV above to get started.")
        return

    game_ids = [row["game_id"] for row in game_rows]
    selected_game = st.selectbox("Select a game_id", game_ids)

    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Basic counts
            cur.execute(
                "SELECT COUNT(*) AS plays FROM plays WHERE game_id = %s;",
                (selected_game,),
            )
            total_plays = cur.fetchone()["plays"]

            cur.execute(
                """
                SELECT play_type, COUNT(*) AS count
                FROM plays
                WHERE game_id = %s
                GROUP BY play_type
                ORDER BY count DESC;
                """,
                (selected_game,),
            )
            run_pass = cur.fetchall()

            cur.execute(
                """
                SELECT formation_norm, COUNT(*) AS count
                FROM plays
                WHERE game_id = %s
                GROUP BY formation_norm
                ORDER BY count DESC
                LIMIT 10;
                """,
                (selected_game,),
            )
            top_formations = cur.fetchall()

            cur.execute(
                """
                SELECT down, AVG(yards_gained) AS avg_yards
                FROM plays
                WHERE game_id = %s
                GROUP BY down
                ORDER BY down;
                """,
                (selected_game,),
            )
            yards_by_down = cur.fetchall()

    st.subheader(f"Game {selected_game} summary")
    col1, col2 = st.columns(2)

    with col1:
        st.metric("Total plays", total_plays)

    with col2:
        if run_pass:
            run_row = next(
                (r for r in run_pass if (r["play_type"] or "").lower() == "run"), None
            )
            pass_row = next(
                (r for r in run_pass if (r["play_type"] or "").lower() == "pass"), None
            )
            rp_text = []
            if run_row:
                rp_text.append(f"Run: {run_row['count']}")
            if pass_row:
                rp_text.append(f"Pass: {pass_row['count']}")
            if rp_text:
                st.write(" / ".join(rp_text))

    st.markdown("#### Top formations (by count)")
    if top_formations:
        st.table(top_formations)
    else:
        st.write("No formations found for this game.")

    st.markdown("#### Yards per down")
    if yards_by_down:
        st.table(yards_by_down)
    else:
        st.write("No yardage data found for this game.")


if __name__ == "__main__":
    main()
