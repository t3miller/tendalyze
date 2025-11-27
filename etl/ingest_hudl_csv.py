import os
import csv
import psycopg2
from psycopg2.extras import execute_batch

DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)


def get_or_create_team(cur, team_name: str):
    cur.execute("SELECT team_id FROM teams WHERE team_name = %s", (team_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        "INSERT INTO teams (team_name) VALUES (%s) RETURNING team_id",
        (team_name,)
    )
    return cur.fetchone()[0]


def get_or_create_game(cur, season: int, week: int, home_team_id: int, away_team_id: int):
    cur.execute(
        """
        SELECT game_id FROM games
        WHERE season = %s AND week = %s
          AND home_team_id = %s AND away_team_id = %s
        """,
        (season, week, home_team_id, away_team_id)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO games (season, week, home_team_id, away_team_id)
        VALUES (%s, %s, %s, %s)
        RETURNING game_id
        """,
        (season, week, home_team_id, away_team_id)
    )
    return cur.fetchone()[0]


def normalize_formation(raw: str) -> str:
    if not raw:
        return None
    f = raw.strip().lower()
    if "trips" in f and "right" in f:
        return "Trips Right"
    if "trips" in f and "left" in f:
        return "Trips Left"
    if "double" in f:
        return "Doubles"
    return raw.strip().title()


def load_hudl_csv(csv_path: str):
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    plays_to_insert = []

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            season = int(row["Season"])
            week = int(row["Week"])

            offense_team_name = row["OffenseTeam"]
            defense_team_name = row["DefenseTeam"]

            offense_team_id = get_or_create_team(cur, offense_team_name)
            defense_team_id = get_or_create_team(cur, defense_team_name)

            game_id = get_or_create_game(cur, season, week, offense_team_id, defense_team_id)

            quarter = int(row["Quarter"])
            clock = row["Clock"]
            down = int(row["Down"])
            distance = int(row["Distance"])
            yard_line = int(row["YardLine"])
            hash_mark = row["Hash"] or None

            formation_raw = row["Formation"] or None
            formation_norm = normalize_formation(formation_raw)
            personnel = row["Personnel"] or None
            play_type = row["PlayType"] or None
            run_direction = row["RunDirection"] or None
            pass_zone = row["PassZone"] or None
            yards_gained = int(row["Yards"])
            result = row["Result"] or None

            cur.execute(
                """
                INSERT INTO drives (game_id, offense_team_id, defense_team_id)
                VALUES (%s, %s, %s)
                RETURNING drive_id
                """,
                (game_id, offense_team_id, defense_team_id)
            )
            drive_id = cur.fetchone()[0]

            plays_to_insert.append((
                drive_id,
                game_id,
                offense_team_id,
                defense_team_id,
                quarter,
                clock,
                down,
                distance,
                yard_line,
                hash_mark,
                formation_raw,
                formation_norm,
                personnel,
                play_type,
                run_direction,
                pass_zone,
                yards_gained,
                result,
            ))

    execute_batch(
        cur,
        """
        INSERT INTO plays (
            drive_id, game_id,
            offense_team_id, defense_team_id,
            quarter, clock, down, distance, yard_line, hash_mark,
            formation_raw, formation_norm, personnel,
            play_type, run_direction, pass_zone,
            yards_gained, result
        )
        VALUES (
            %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s
        )
        """,
        plays_to_insert,
        page_size=100
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(plays_to_insert)} plays from {csv_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "csv_path",
        nargs="?",
        default="etl/sample_data/sample_game.csv",
        help="Path to Hudl-style CSV file"
    )
    args = parser.parse_args()

    load_hudl_csv(args.csv_path)
