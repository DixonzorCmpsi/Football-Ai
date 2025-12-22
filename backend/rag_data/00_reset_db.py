# rag_data/00_reset_db.py
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Tables that might have old/bad schemas
TABLES_TO_DROP = [
    "weekly_player_stats",
    "weekly_defense_stats", 
    "weekly_offense_stats",
    "weekly_snap_counts",
    "weekly_injuries",
    "weekly_player_odds",
    "weekly_rankings",
    "schedule",             # We want to ensure this is fresh
    "player_profiles",      # Ensure fresh
    "modeling_dataset",     # Ensure fresh
    "featured_dataset"      # Ensure fresh
]

def main():
    print("--- ☢️  NUCLEAR OPTION: RESETTING DATABASE TABLES ---")
    if not DB_CONNECTION_STRING:
        print("❌ Error: DB_CONNECTION_STRING not set.")
        return

    confirm = input("Are you sure you want to DROP all football tables? (y/n): ")
    if confirm.lower() != 'y':
        print("Aborting.")
        return

    engine = create_engine(DB_CONNECTION_STRING)
    
    with engine.connect() as conn:
        for table in TABLES_TO_DROP:
            try:
                print(f"   Dropping {table}...", end=" ")
                conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                print("✅")
            except Exception as e:
                print(f"❌ Error: {e}")
        conn.commit()

    print("\n✅ Database cleaned. Run '05_etl_to_postgres.py' to rebuild everything.")

if __name__ == "__main__":
    main()