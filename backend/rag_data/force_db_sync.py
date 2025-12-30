import polars as pl
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. Setup
load_dotenv()
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
SEASON = 2025 # Adjust if dynamic logic needed
CSV_FILE = f"weekly_injuries_{SEASON}.csv"
TABLE_NAME = "weekly_injuries"

if not DB_CONNECTION_STRING:
    print("❌ DB_CONNECTION_STRING missing.")
    exit()

if not os.path.exists(CSV_FILE):
    print(f"❌ CSV File not found: {CSV_FILE}")
    exit()

print(f"--- ☢️ NUCLEAR OPTION: Forcing DB Sync for {TABLE_NAME} ---")

try:
    # 2. Connect
    engine = create_engine(DB_CONNECTION_STRING)
    
    # 3. Read New CSV
    print(f"   📖 Reading {CSV_FILE}...")
    df = pl.read_csv(CSV_FILE)
    
    if "week" not in df.columns:
        print("❌ ABORTING: CSV is missing 'week' column! Run 08 script first.")
        exit()
        
    print(f"      ✅ Loaded {len(df)} rows. Columns: {df.columns}")

    # 4. DROP Existing Table
    print(f"   💣 Dropping old table '{TABLE_NAME}'...")
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME} CASCADE"))
        conn.commit()

    # 5. UPLOAD New Table
    print(f"   🚀 Uploading new data to Postgres...")
    df.to_pandas().to_sql(TABLE_NAME, engine, index=False, if_exists='replace')
    
    print("   ✅ SUCCESS: Database is now in sync with CSV.")

except Exception as e:
    print(f"❌ Error: {e}")