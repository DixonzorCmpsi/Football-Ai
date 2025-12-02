from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

# --- Configuration ---
load_dotenv()
password = os.getenv('POSTGRE_PASSWORD')
if not password:
    print("Error: POSTGRE_PASSWORD not set.")
    exit(1)

encoded_password = quote_plus(password)
DB_CONNECTION_STRING = f"postgresql://postgres:{encoded_password}@localhost:5432/fantasy_data"

def fix_schema():
    engine = create_engine(DB_CONNECTION_STRING)
    print("Connecting to database...")
    
    try:
        with engine.connect() as conn:
            # Drop the old table so it can be recreated with new columns
            conn.execute(text("DROP TABLE IF EXISTS weekly_rankings"))
            conn.commit()
            print("✅ Successfully dropped 'weekly_rankings' table.")
            print("You can now run 'python 06_generate_rankings.py' to recreate it correctly.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fix_schema()