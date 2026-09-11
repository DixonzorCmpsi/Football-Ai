"""
Update Bovada Props with Actual Results for ML Training
This script compares betting lines against actual player stats to determine if props hit or missed.
Run this AFTER games complete to populate the actual_result field.
"""
import polars as pl
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:
    raise ValueError("DB_CONNECTION_STRING not set in .env")

ENGINE = create_engine(DB_CONNECTION_STRING)

def get_current_season():
    now = datetime.now()
    if now.month >= 3:
        return now.year
    else:
        return now.year - 1

SEASON = get_current_season()

# Mapping of prop types to stat columns
PROP_STAT_MAP = {
    "Passing Yards": "passing_yards",
    "Passing Touchdowns": "passing_tds",
    "Rushing Yards": "rushing_yards",
    "Rushing & Receiving Yards": ["rushing_yards", "receiving_yards"],  # Combo stat
    "Receiving Yards": "receiving_yards",
    "Receptions": "receptions",
    "Passing Attempts": "attempts",
    "Rush Attempts": "carries",
    "Completions": "completions",
    "Passing And Rushing Yards": ["passing_yards", "rushing_yards"],  # Combo stat
    "Anytime TD": ["passing_tds", "rushing_tds", "receiving_tds"]  # Any TD
}


def update_prop_results(week: int = None):
    """
    Update the actual_result field in bovada_player_props based on actual stats.
    
    Args:
        week: Specific week to update. If None, updates all weeks.
    """
    print(f"\n--- 📊 Updating Bovada Prop Results (Season {SEASON}) ---")
    
    # Load bovada props from database
    props_query = "SELECT * FROM bovada_player_props"
    if week:
        props_query += f" WHERE week = {week}"
    
    try:
        df_props = pl.read_database(props_query, ENGINE)
    except Exception as e:
        print(f"❌ Error loading props: {e}")
        return
    
    if df_props.is_empty():
        print("⚠️ No props found to update")
        return
    
    print(f"   📥 Loaded {len(df_props)} prop bets")
    
    # Load player stats
    stats_table = f"weekly_player_stats_{SEASON}"
    try:
        df_stats = pl.read_database(f"SELECT * FROM {stats_table}", ENGINE)
    except Exception as e:
        print(f"❌ Error loading stats: {e}")
        return
    
    if df_stats.is_empty():
        print(f"⚠️ No stats found in {stats_table}")
        return
    
    print(f"   📥 Loaded {len(df_stats)} player stat records")

    # weekly_player_stats_* is keyed by player_id and carries NO player_name,
    # while bovada_player_props only knows the sportsbook's display name. Join
    # the two through player_profiles, which has both. Without this every prop
    # lookup raised ColumnNotFoundError: "player_name" and no prop ever got
    # graded.
    def _norm_name(col):
        # Sportsbooks write "Jr.", "II", periods and casing inconsistently.
        return (
            col.str.to_lowercase()
               .str.replace_all(r"[.'`]", "")
               .str.replace_all(r"\s+(jr|sr|ii|iii|iv|v)$", "")
               .str.strip_chars()
        )

    try:
        df_profiles = pl.read_database(
            "SELECT player_id, player_name FROM player_profiles", ENGINE
        ).unique(subset=["player_id"])
    except Exception as e:
        print(f"❌ Error loading player_profiles for name join: {e}")
        return

    if "player_name" not in df_stats.columns:
        df_stats = df_stats.join(df_profiles, on="player_id", how="left")

    # Normalised key on both sides so "A.J. Brown" matches "AJ Brown".
    df_stats = df_stats.with_columns(_norm_name(pl.col("player_name")).alias("_name_key"))
    matched_names = df_stats.get_column("_name_key").unique().to_list()
    print(f"   🔗 Joined names onto stats ({len(matched_names)} distinct players)")
    
    # Process each prop
    updated_props = []
    
    for row in df_props.iter_rows(named=True):
        player_name = row['player_name']
        week_num = row['week']
        prop_type = row['prop_type']
        line = float(row['line'])
        side = row['side'].lower()
        
        # Skip if already has result
        if row.get('actual_result') is not None and row.get('actual_result') != '':
            updated_props.append(row)
            continue
        
        # Get stat mapping
        stat_cols = PROP_STAT_MAP.get(prop_type)
        if not stat_cols:
            # Unknown prop type, skip
            updated_props.append(row)
            continue
        
        # Find player's stats for this week
        name_key = (
            str(player_name or "").lower()
            .replace(".", "").replace("'", "").replace("`", "")
            .strip()
        )
        for suffix in (" jr", " sr", " ii", " iii", " iv", " v"):
            if name_key.endswith(suffix):
                name_key = name_key[: -len(suffix)].strip()
                break
        player_stats = df_stats.filter(
            (pl.col("_name_key") == name_key) &
            (pl.col("week") == week_num)
        )
        
        if player_stats.is_empty():
            # Game not played yet or player not in stats
            updated_props.append(row)
            continue
        
        stat_row = player_stats.row(0, named=True)
        
        # Calculate actual value
        if isinstance(stat_cols, list):
            # Combo stat (sum multiple columns)
            actual_value = 0
            for col in stat_cols:
                val = stat_row.get(col)
                if val is not None:
                    actual_value += float(val)
        else:
            # Single stat
            actual_value = stat_row.get(stat_cols)
            if actual_value is None:
                updated_props.append(row)
                continue
            actual_value = float(actual_value)
        
        # Determine if prop hit
        if side in ['over', 'yes']:
            result = 'hit' if actual_value > line else 'miss'
        elif side in ['under', 'no']:
            result = 'hit' if actual_value < line else 'miss'
        else:
            result = None
        
        # Update row
        updated_row = dict(row)
        updated_row['actual_result'] = result
        updated_props.append(updated_row)
    
    # Convert back to DataFrame
    df_updated = pl.DataFrame(updated_props)
    
    # Count how many we updated
    original_nulls = df_props.filter(
        (pl.col("actual_result").is_null()) | (pl.col("actual_result") == "")
    ).height
    new_nulls = df_updated.filter(
        (pl.col("actual_result").is_null()) | (pl.col("actual_result") == "")
    ).height
    updated_count = original_nulls - new_nulls
    
    print(f"\n   ✅ Updated {updated_count} prop results")
    
    # Save back to database
    try:
        with ENGINE.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS bovada_player_props CASCADE"))
            conn.commit()
        
        df_updated.to_pandas().to_sql("bovada_player_props", ENGINE, if_exists='replace', index=False)
        print(f"   💾 Saved updated props to database")
        
        # Print summary stats
        hits = df_updated.filter(pl.col("actual_result") == "hit").height
        misses = df_updated.filter(pl.col("actual_result") == "miss").height
        pending = df_updated.filter(
            (pl.col("actual_result").is_null()) | (pl.col("actual_result") == "")
        ).height
        
        print(f"\n   📊 Results Summary:")
        print(f"      ✅ Hits: {hits}")
        print(f"      ❌ Misses: {misses}")
        print(f"      ⏳ Pending: {pending}")
        
        if hits + misses > 0:
            hit_rate = (hits / (hits + misses)) * 100
            print(f"      🎯 Hit Rate: {hit_rate:.1f}%")
        
    except Exception as e:
        print(f"❌ Error saving to database: {e}")
        return


if __name__ == "__main__":
    import sys
    
    # Allow running for specific week or all weeks
    if len(sys.argv) > 1:
        week = int(sys.argv[1])
        update_prop_results(week)
    else:
        update_prop_results()  # Update all weeks
