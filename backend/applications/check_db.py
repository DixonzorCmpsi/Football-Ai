import os
import polars as pl
from dotenv import load_dotenv

load_dotenv()
DB_CONN = os.getenv("DB_CONNECTION_STRING")

print("--- Checking Database Stats ---")
try:
    df = pl.read_database_uri("SELECT week, COUNT(*) as count FROM weekly_player_stats GROUP BY week ORDER BY week", DB_CONN)
    print(df)
except Exception as e:
    print(f"Error reading DB: {e}")