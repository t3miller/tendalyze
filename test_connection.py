import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")

def main():
    if not DATABASE_URL:
        print("DATABASE_URL is not set")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print("Connected to Postgres:", version)

        cur.execute("SELECT COUNT(*) FROM plays;")
        count = cur.fetchone()[0]
        print("Rows in plays table:", count)

        cur.close()
        conn.close()
        print("Connection test complete.")

    except Exception as e:
        print("Error connecting:", e)

if __name__ == "__main__":
    main()
