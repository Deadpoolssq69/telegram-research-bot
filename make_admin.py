# make_admin.py

import os
import psycopg2

# Read the DATABASE_URL from the environment (exactly what your bot uses)
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("Error: Please set the DATABASE_URL environment variable first.")
    exit(1)

# Replace 123456789 with your actual numeric Telegram ID
MY_TELEGRAM_ID = 5419681514

def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # Insert you as an admin; if you’re already there, ignore the conflict
    cur.execute("""
        INSERT INTO users (telegram_id, role)
        VALUES (%s, 'admin')
        ON CONFLICT (telegram_id) DO NOTHING;
    """, (MY_TELEGRAM_ID,))

    print(f"Checked `users` table: inserted {MY_TELEGRAM_ID} as admin (if not present).")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
