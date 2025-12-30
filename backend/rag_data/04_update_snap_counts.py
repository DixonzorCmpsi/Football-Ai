import nflreadpy as nfl
import polars as pl
import pandas as pd
import os
import sys
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer, util

# --- Configuration ---
load_dotenv()

# Database Config (Connection String)
DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')

def get_current_season():
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1

SEASON = get_current_season()
OUTPUT_FILE = f"weekly_snap_counts_{SEASON}.csv"
PROFILES_FILE = "player_profiles.csv" # Fallback local file

OFFENSIVE_POSITIONS = ['QB', 'RB', 'WR', 'TE', 'FB']
MODEL_NAME = 'all-MiniLM-L6-v2' 
AI_MATCH_THRESHOLD = 0.80 

# STRICT Column Definition for safe concatenation
INTERMEDIATE_COLS = [
    "player_id", "player_name", "season", "week", 
    "offense_snaps", "offense_pct", "position"
]

# Robust Map: PFR/Legacy Abbr -> nflverse/GSIS Standard
TEAM_ABBR_MAP = {
    "GNB": "GB",  "KAN": "KC",  "NWE": "NE",  "NOR": "NO",
    "SFO": "SF",  "TAM": "TB",  "LVR": "LV",  "LAR": "LA",
    "JAC": "JAX", "SD":  "LAC", "STL": "LA",  "OAK": "LV"
}

def standardize_teams(df, col_name="team_abbr"):
    """Normalizes team abbreviations to ensure 'SFO' matches 'SF'."""
    if col_name not in df.columns: return df
    def map_team(t): return TEAM_ABBR_MAP.get(t, t)
    return df.with_columns(
        pl.col(col_name).map_elements(map_team, return_dtype=pl.Utf8).alias(col_name)
    )

def ensure_polars(df):
    if isinstance(df, pl.DataFrame): return df
    return pl.from_pandas(pd.DataFrame(df))

def safe_select(df):
    """Ensures dataframe has exactly INTERMEDIATE_COLS, adds missing as null."""
    for col in INTERMEDIATE_COLS:
        if col not in df.columns:
            if col == "season":
                df = df.with_columns(pl.lit(SEASON).alias(col))
            else:
                df = df.with_columns(pl.lit(None).alias(col))
    return df.select(INTERMEDIATE_COLS)

def get_db_connection():
    if not DB_CONNECTION_STRING:
        print("   ⚠️ DB_CONNECTION_STRING not found in environment.")
        return None
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        return conn
    except Exception as e:
        print(f"   ⚠️ DB Connection Failed: {e}")
        return None

def load_profiles_hybrid():
    """Tries DB first, falls back to Local CSV."""
    
    # 1. Try Database
    conn = get_db_connection()
    if conn:
        print("   📡 Connecting to Database for Profiles...")
        try:
            # We select specific columns to match the local CSV structure
            # We explicitly ask for pfr_id in case it exists
            query = """
                SELECT player_id, player_name, position, team_abbr, pfr_id 
                FROM player_profiles
            """
            df_db = pl.read_database(query, conn)
            conn.close()
            
            if not df_db.is_empty():
                print(f"   ✅ Loaded {len(df_db)} profiles from Database.")
                return df_db
            else:
                print("   ⚠️ Database table 'player_profiles' is empty.")
        except Exception as e:
            print(f"   ⚠️ Error reading from DB (Columns might differ): {e}")
            if conn: conn.close()

    # 2. Fallback to Local
    print(f"   📂 Falling back to local file: {PROFILES_FILE}")
    if os.path.exists(PROFILES_FILE):
        return pl.read_csv(PROFILES_FILE)
    else:
        print(f"   ❌ Critical Error: Local file {PROFILES_FILE} also missing.")
        return None

def main():
    print(f"--- Updating Snap Counts for {SEASON} ---")
    
    # --- LOAD PROFILES (DB FIRST) ---
    df_profiles = load_profiles_hybrid()
    if df_profiles is None: return

    # Normalize Profile columns just like the original script
    try:
        # Standardize Teams in Profiles
        df_profiles = standardize_teams(df_profiles, "team_abbr")

        # Select key columns + PFR ID if available
        prof_cols = ["player_id", "player_name", "team_abbr", "position"]
        
        # Check if pfr_id exists (handled dynamically whether from DB or CSV)
        if "pfr_id" in df_profiles.columns: 
            prof_cols.append("pfr_id")
        else:
            # Create null column if missing to prevent Strategy B crash
            df_profiles = df_profiles.with_columns(pl.lit(None).alias("pfr_id"))
            prof_cols.append("pfr_id")

        df_profiles = df_profiles.select(prof_cols)
    except Exception as e:
        print(f"❌ Error processing profiles: {e}")
        return

    print("Loading snap counts from nflreadpy...")
    try:
        raw_snaps = nfl.load_snap_counts(seasons=[SEASON])
        df_snaps = ensure_polars(raw_snaps)
        
        # 1. Drop 'position' from snaps immediately (Trust Profile Position)
        if "position" in df_snaps.columns:
            df_snaps = df_snaps.drop("position")
            
        df_snaps = df_snaps.filter(pl.col("game_type") == "REG")
        df_snaps = df_snaps.rename({
            "player": "player_name",
            "team": "team_abbr",
            "pfr_player_id": "pfr_id" 
        })
        
        # Standardize Teams in Snaps (Fixes SFO/SF mismatches)
        df_snaps = standardize_teams(df_snaps, "team_abbr")
        
    except Exception as e:
        print(f"❌ Error loading snaps: {e}")
        return

    # --- STRATEGY A: Exact Match ---
    print("Strategy A: Exact Match (Name + Team)...")
    joined_a = df_snaps.join(df_profiles, on=["player_name", "team_abbr"], how="left")
    
    matched_a = joined_a.filter(pl.col("player_id").is_not_null())
    unmatched_a = joined_a.filter(pl.col("player_id").is_null())
    
    # Normalize matched_a immediately
    final_a = safe_select(matched_a)
    print(f"  -> Matches: {len(final_a)}")

    # --- STRATEGY B: PFR ID Backup ---
    # Check if pfr_id column actually has data in profiles
    has_pfr_data = df_profiles["pfr_id"].null_count() < len(df_profiles)

    if len(unmatched_a) > 0 and has_pfr_data:
        print(f"Strategy B: PFR ID Backup ({len(unmatched_a)} remaining)...")
        # Prepare for retry (use original snap columns)
        snap_cols = df_snaps.columns
        unmatched_clean = unmatched_a.select(snap_cols)
        
        joined_b = unmatched_clean.join(df_profiles, on=["pfr_id"], how="left", suffix="_prof")
        
        matched_b = joined_b.filter(pl.col("player_id").is_not_null())
        
        if len(matched_b) > 0:
            # Trust Profile Name from the join
            matched_b = matched_b.with_columns(pl.col("player_name_prof").alias("player_name"))
            
            final_b = safe_select(matched_b)
            print(f"  -> Recovered via ID: {len(final_b)}")
            
            # Update unmatched list for next step
            # Note: We filter where player_id IS NULL to pass to Strategy C
            unmatched_b = joined_b.filter(pl.col("player_id").is_null()).select(snap_cols)
        else:
            final_b = None
            unmatched_b = unmatched_clean
    else:
        final_b = None
        unmatched_b = unmatched_a

    # --- STRATEGY C: AI Semantic Match ---
    final_c = None
    if len(unmatched_b) > 0:
        print(f"Strategy C: AI Semantic Matching ({len(unmatched_b)} remaining)...")
        try:
            model = SentenceTransformer(MODEL_NAME)
        except Exception as e: 
            print(f"   ⚠️ Could not load AI model: {e}")
            return

        team_rosters = {}
        for row in df_profiles.iter_rows(named=True):
            tm = row['team_abbr']
            if tm not in team_rosters: team_rosters[tm] = {}
            team_rosters[tm][row['player_name']] = row['player_id']

        unique_missing = unmatched_b.select(["player_name", "team_abbr"]).unique()
        recovered_data = []

        for row in unique_missing.iter_rows(named=True):
            snap_name = row['player_name']
            team = row['team_abbr']
            
            if team in team_rosters:
                candidates = list(team_rosters[team].keys())
                if candidates:
                    emb1 = model.encode(snap_name, convert_to_tensor=True)
                    emb2 = model.encode(candidates, convert_to_tensor=True)
                    scores = util.cos_sim(emb1, emb2)
                    best_idx = int(scores.argmax())
                    best_score = float(scores.max())
                    
                    if best_score >= AI_MATCH_THRESHOLD:
                        best_match = candidates[best_idx]
                        recovered_data.append({
                            "snap_name": snap_name,
                            "team_abbr": team,
                            "real_name": best_match,
                            "player_id": team_rosters[team][best_match]
                        })
                        print(f"  -> AI Match: '{snap_name}' ≈ '{best_match}' ({best_score:.2f})")

        if recovered_data:
            df_rec = pl.DataFrame(recovered_data)
            cols_clean = [c for c in df_snaps.columns]
            unmatched_clean = unmatched_b.select(cols_clean)
            
            joined_c = unmatched_clean.join(
                df_rec, 
                left_on=["player_name", "team_abbr"], 
                right_on=["snap_name", "team_abbr"], 
                how="inner"
            )
            
            # We need 'position' from profiles
            # Join back to profiles to get it
            joined_c = joined_c.join(
                df_profiles.select(["player_id", "position"]), 
                on="player_id", 
                how="left"
            )
            
            # Use Real Name
            joined_c = joined_c.with_columns(pl.col("real_name").alias("player_name"))
            
            final_c = safe_select(joined_c)
            print(f"  -> Recovered via AI: {len(final_c)} rows")

    # 4. CONCAT & FINAL FILTER
    data_frames = [df for df in [final_a, final_b, final_c] if df is not None and len(df) > 0]
    
    if not data_frames:
        print("No matches found.")
        return

    final_df = pl.concat(data_frames)
    
    # Filter by Position (Trusting Profile Position)
    print("Filtering for Offensive Positions...")
    final_clean = final_df.filter(pl.col("position").is_in(OFFENSIVE_POSITIONS))
    
    # Drop position column from output
    output_cols = ["player_id", "player_name", "season", "week", "offense_snaps", "offense_pct"]
    final_clean = final_clean.select(output_cols)
    
    # Type Safety
    final_clean = final_clean.with_columns([
        pl.col("week").cast(pl.Int64),
        pl.col("offense_snaps").cast(pl.Int64),
        pl.col("offense_pct").cast(pl.Float64)
    ])

    print(f"\nSaving {len(final_clean)} rows to {OUTPUT_FILE}...")
    final_clean.write_csv(OUTPUT_FILE)
    print("✅ Snap counts updated successfully.")

if __name__ == "__main__":
    main()