
import polars as pl
import os
import sys

# Add backend to path
sys.path.append('/home/football-ai/Football-Ai/backend')

from applications.server import model_data, refresh_db_data, get_team_abbr

async def inspect_data():
    refresh_db_data()
    
    print("\n--- LINES DATA (Week 18) ---")
    if "df_lines" in model_data:
        df = model_data["df_lines"]
        w18 = df.filter(pl.col("week") == 18)
        print(w18.head(5))
        
        # Check specific game LV vs KC
        kc_game = w18.filter((pl.col("home_team") == "Kansas City Chiefs") | (pl.col("away_team") == "Kansas City Chiefs"))
        print("\nKC Game Lines:")
        print(kc_game)
    else:
        print("df_lines not found")

    print("\n--- PROPS DATA (Week 18) ---")
    if "df_props" in model_data:
        df = model_data["df_props"]
        w18 = df.filter(pl.col("week") == 18)
        print(f"Total props for week 18: {len(w18)}")
        print(w18.head(5))
        
        # Check for a specific player, e.g., Travis Kelce
        kelce = w18.filter(pl.col("player_name").str.contains("Kelce"))
        print("\nTravis Kelce Props:")
        print(kelce)
        
        # Check prop types
        print("\nUnique Prop Types:")
        print(w18["prop_type"].unique().to_list())
    else:
        print("df_props not found")

if __name__ == "__main__":
    import asyncio
    asyncio.run(inspect_data())
