import csv
import os
from datetime import date
from typing import Optional

import psycopg2


def _parse_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def load_hudl_csv(
    csv_path: str,
    offense_team_id: int,
    defense_team_id: int,
    game_date: Optional[date] = None,
    season: Optional[int] = None,
    week: Optional[int] = None,
    venue: Optional[str] = None,
    source: str = "Hudl",
) -> int:
    """
    Ingest a Hudl-style CSV into the plays table,
    creating a row in games and returning the new game_id.

    We IGNORE any game_id column in the CSV and use the one we create.
    """

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set!")

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # 1) Create a game record
    cur.execute(
        """
        INSERT INTO games (offense_team_id, defense_team_id, game_date, season, week, venue, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING game_id;
        """,
        (offense_team_id, defense_team_id, game_date, season, week, venue, source),
    )
    game_id = cur.fetchone()[0]

    # 2) Insert plays tied to that game
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            drive_id = _parse_int(row.get("drive_id"))
            quarter = _parse_int(row.get("quarter"))
            clock = row.get("clock")
            down = _parse_int(row.get("down"))
            distance = _parse_int(row.get("distance"))
            yard_line = _parse_int(row.get("yard_line"))
            hash_mark = row.get("hash_mark")
            formation_raw = row.get("formation_raw")
            formation_norm = row.get("formation_norm")
            personnel = row.get("personnel")
            play_type = row.get("play_type")
            run_direction = row.get("run_direction")
            pass_zone = row.get("pass_zone")
            yards_gained = _parse_int(row.get("yards_gained"))
            result = row.get("result")

            cur.execute(
                """
                INSERT INTO plays (
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
                    result
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                );
                """,
                (
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
                ),
            )

    conn.commit()
    cur.close()
    conn.close()

    return game_id
