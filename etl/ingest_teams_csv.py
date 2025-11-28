import csv
import os
import psycopg2


def load_teams_csv(csv_path: str):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set!")

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            team_name = row.get("team_name")
            mascot = row.get("mascot")
            city = row.get("city")
            state = row.get("state")
            division = row.get("division")
            region = row.get("region")
            district = row.get("district")

            cur.execute(
                """
                INSERT INTO teams (team_name, mascot, city, state, division, region, district)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (team_name, mascot, city, state, division, region, district),
            )

    conn.commit()
    cur.close()
    conn.close()
