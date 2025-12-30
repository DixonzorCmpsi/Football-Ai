import polars as pl
import os
from dotenv import load_dotenv

# 1. Load Environment Variables
load_dotenv()
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

if not DB_CONNECTION_STRING:
    print("❌ Error: DB_CONNECTION_STRING not found in environment.")
    exit()

print(f"--- 🕵️‍♂️ INSPECTING DATABASE TABLE: weekly_injuries ---")

try:
    # 2. Check Columns (Schema)
    query_schema = "SELECT * FROM weekly_injuries LIMIT 1"
    df_schema = pl.read_database_uri(query_schema, DB_CONNECTION_STRING)
    
    print(f"✅ Connection Successful. Table Columns: {df_schema.columns}")
    
    if "week" in df_schema.columns:
        print("✅ 'week' column EXISTS in database.")
    else:
        print("❌ CRITICAL: 'week' column is MISSING in database!")

    # 3. Look up Quinshon Judkins
    target_id = "00-0040784" 
    print(f"\n🔎 Querying for Judkins ({target_id})...")
    
    query_player = f"SELECT * FROM weekly_injuries WHERE player_id = '{target_id}'"
    df_player = pl.read_database_uri(query_player, DB_CONNECTION_STRING)
    
    if not df_player.is_empty():
        print("✅ Found Records:")
        print(df_player.sort("week", descending=True))
    else:
        print(f"❌ Judkins ({target_id}) NOT FOUND in database table.")

except Exception as e:
    print(f"❌ Database Error: {e}")