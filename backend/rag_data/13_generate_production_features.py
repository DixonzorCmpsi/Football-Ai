import pandas as pd
import numpy as np
import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from feature_history import (
    LAG_COLUMNS,
    add_player_lags,
    add_season_average,
    add_team_shares,
    attach_snap_counts,
    build_upcoming_rows,
    derive_metrics,
    normalize_weekly_stats,
)

# How many prior seasons to pull in purely so that week-1 lag features are real
# numbers instead of zeros. Only the current season is ever written out.
HISTORY_SEASONS = 2

# --- Configuration ---
load_dotenv()

def get_current_season():
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1

SEASON = get_current_season()
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# We still output a CSV as a backup/artifact for debug scripts
RAG_DATA_DIR = Path("rag_data") if Path("rag_data").exists() else Path(".")
OUTPUT_FILE = RAG_DATA_DIR / f"weekly_feature_set_{SEASON}.csv"

def get_db_engine():
    if not DB_CONNECTION_STRING:
        print("❌ Error: DB_CONNECTION_STRING not found.")
        sys.exit(1)
    return create_engine(DB_CONNECTION_STRING)

def load_table(engine, table_name):
    print(f"   - Fetching {table_name} from DB...")
    query = f"SELECT * FROM {table_name}"
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"     ⚠️ Warning: Could not load {table_name}: {e}")
        return pd.DataFrame()

def load_prior_seasons(seasons, profiles):
    """Prior-season weekly stats, normalized into the current derived schema.

    Read from the cached CSVs in rag_data (the DB only ever holds the current
    season). Returns an empty frame when nothing is cached, in which case the
    generator degrades to its old current-season-only behaviour rather than
    failing the ETL.
    """
    pfr_to_gsis = {}
    if profiles is not None and not profiles.empty and "pfr_id" in profiles.columns:
        valid = profiles.dropna(subset=["pfr_id", "player_id"])
        pfr_to_gsis = dict(zip(valid["pfr_id"].astype(str), valid["player_id"].astype(str)))

    frames = []
    for season in seasons:
        path = RAG_DATA_DIR / f"weekly_player_stats_{season}.csv"
        if not path.exists():
            print(f"   - no cached stats for {season}, skipping")
            continue
        df = pd.read_csv(path, low_memory=False)
        if df.empty:
            continue
        df = normalize_weekly_stats(df, season)

        snap_path = RAG_DATA_DIR / f"weekly_snap_counts_{season}.csv"
        snaps = pd.read_csv(snap_path, low_memory=False) if snap_path.exists() else pd.DataFrame()
        df = attach_snap_counts(df, snaps, pfr_to_gsis)

        df = derive_metrics(df)
        df = add_team_shares(df)
        frames.append(df)
        print(f"   - {season}: {len(df)} rows")

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def unplayed_games(schedule):
    """Scheduled games with no score yet.

    A week is only partly played for most of a game week -- in 2026 week 1, two
    games were final while fourteen had not kicked off. Treating "week 1 exists
    in the stats table" as "week 1 is done" pushed placeholder rows to week 2 and
    left every player in those fourteen games with no feature row at all.
    """
    if schedule is None or schedule.empty:
        return pd.DataFrame()
    out = schedule
    for col in ("home_score", "away_score"):
        if col in out.columns:
            scores = pd.to_numeric(out[col], errors="coerce")
            out = out[scores.isna()]
    return out


def eligible_players(profiles, schedule, week):
    """Skill players whose team has an unplayed game in `week`."""
    if profiles is None or profiles.empty or schedule is None or schedule.empty:
        return pd.DataFrame()

    wk = schedule[schedule["week"] == week] if "week" in schedule.columns else schedule
    if wk.empty:
        return pd.DataFrame()

    opponent = {}
    for _, g in wk.iterrows():
        home, away = g.get("home_team"), g.get("away_team")
        if home and away:
            opponent[home] = away
            opponent[away] = home
    if not opponent:
        return pd.DataFrame()

    team_col = "team_abbr" if "team_abbr" in profiles.columns else "team"
    if team_col not in profiles.columns:
        return pd.DataFrame()

    rows = profiles.copy()
    if "position" in rows.columns:
        rows = rows[rows["position"].isin(["QB", "RB", "WR", "TE"])]
    if "status" in rows.columns:
        rows = rows[rows["status"].isin(["ACT", "DEV"])]
    rows = rows[rows[team_col].isin(opponent.keys())]
    if rows.empty:
        return pd.DataFrame()

    out = pd.DataFrame({
        "player_id": rows["player_id"].astype(str),
        "player_name": rows["player_name"] if "player_name" in rows.columns else rows["player_id"],
        "position": rows["position"],
        "team": rows[team_col],
        "opponent_team": rows[team_col].map(opponent),
    })
    return out.dropna(subset=["opponent_team"]).reset_index(drop=True)


def main():
    print(f"--- 🏭 Generating Production Features for {SEASON} (DB Centric) ---")
    engine = get_db_engine()

    # 1. Load Data from DB
    print("1. Loading Raw Data from Database...")
    df_player = load_table(engine, f"weekly_player_stats_{SEASON}")
    if df_player.empty:
        print("❌ CRITICAL: weekly_player_stats table is empty or missing.")
        return

    df_defense = load_table(engine, f"weekly_defense_stats_{SEASON}")
    df_offense = load_table(engine, f"weekly_offense_stats_{SEASON}")
    df_snaps = load_table(engine, f"weekly_snap_counts_{SEASON}")
    df_profiles = load_table(engine, "player_profiles") 

    print(f"   ✅ Loaded {len(df_player)} player rows.")

    # --- NORMALIZE COLUMN NAMES (THE FIX) ---
    name_map = {
        'receiving_tds': 'receiving_touchdown',
        'passing_tds': 'passing_touchdown',
        'rushing_tds': 'rush_touchdown', 
        'receiving_yards_after_catch': 'yards_after_catch',
        'attempts': 'pass_attempts'  # <--- CRITICAL: This was missing!
    }
    df_player.rename(columns=name_map, inplace=True)

    # 2. Merge Snap Counts
    if not df_snaps.empty:
        # Normalize IDs
        df_snaps['player_id'] = df_snaps['player_id'].astype(str)
        df_player['player_id'] = df_player['player_id'].astype(str)
        
        cols_to_use = ['player_id', 'week', 'offense_snaps', 'offense_pct']
        valid_cols = [c for c in cols_to_use if c in df_snaps.columns]
        
        df_player = pd.merge(df_player, df_snaps[valid_cols], on=['player_id', 'week'], how='left')
        df_player[['offense_snaps', 'offense_pct']] = df_player[['offense_snaps', 'offense_pct']].fillna(0)
    else:
        df_player['offense_snaps'] = 0; df_player['offense_pct'] = 0

    # 3. Merge Profiles
    if not df_profiles.empty:
        if 'draft_number' in df_profiles.columns: df_profiles.rename(columns={'draft_number': 'draft_ovr'}, inplace=True)
        if 'display_name' in df_profiles.columns and 'player_name' not in df_profiles.columns:
             df_profiles.rename(columns={'display_name': 'player_name'}, inplace=True)
        
        if 'years_exp' not in df_profiles.columns and 'draft_year' in df_profiles.columns:
            df_profiles['years_exp'] = (SEASON - df_profiles['draft_year']).clip(lower=0)

        cols = [c for c in ['player_id', 'player_name', 'age', 'years_exp', 'draft_ovr'] if c in df_profiles.columns]
        df_player = pd.merge(df_player, df_profiles[cols], on='player_id', how='left')
        for c in ['age', 'years_exp', 'draft_ovr']: 
            if c in df_player.columns: df_player[c] = df_player[c].fillna(0)
            
    if 'player_name' not in df_player.columns: df_player['player_name'] = df_player['player_id']

    # 4. Engineer Defense vs. Position Features (DvP)
    print("4. Engineering Defense vs. Position stats...")
    dvp = df_player.groupby(['opponent_team', 'week', 'position'])['y_fantasy_points_ppr'].sum().reset_index()
    dvp.sort_values(['opponent_team', 'position', 'week'], inplace=True)
    
    dvp['rolling_avg_points_allowed_to_pos'] = dvp.groupby(['opponent_team', 'position'])['y_fantasy_points_ppr']\
        .transform(lambda x: x.shift(1).rolling(4, min_periods=1).mean()).fillna(0)
    
    dvp_wide = dvp.pivot_table(index=['opponent_team', 'week'], columns='position', values='rolling_avg_points_allowed_to_pos').reset_index()
    dvp_wide.columns = [f"rolling_avg_points_allowed_to_{c}" if c in ['QB', 'RB', 'WR', 'TE'] else c for c in dvp_wide.columns]
    
    df_player = pd.merge(df_player, dvp_wide, on=['opponent_team', 'week'], how='left')
    for pos in ['QB', 'RB', 'WR', 'TE']:
        col = f"rolling_avg_points_allowed_to_{pos}"
        if col in df_player.columns: df_player[col] = df_player[col].fillna(0)

    # 5. Engineer General Opponent Defense Features
    print("5. Engineering Opponent Defense features...")
    if not df_defense.empty:
        df_defense.sort_values(['team_abbr', 'week'], inplace=True)
        if 'opponent_team' in df_defense.columns: df_defense.drop(columns=['opponent_team'], inplace=True)

        metrics = ['points_allowed', 'passing_yards_allowed', 'rushing_yards_allowed', 'def_sacks', 'def_interceptions', 'def_qb_hits']
        for col in metrics:
            if col in df_defense.columns:
                df_defense[f'rolling_avg_{col}_4_weeks'] = df_defense.groupby('team_abbr')[col].shift(1).rolling(4, min_periods=1).mean()
                for lag in [1, 2, 3]:
                    df_defense[f'opp_def_{col}_lag_{lag}'] = df_defense.groupby('team_abbr')[col].shift(lag)

        df_def_merge = df_defense.rename(columns={'team_abbr': 'opponent_team'})
        df_player = pd.merge(df_player, df_def_merge, on=['opponent_team', 'week'], how='left', suffixes=('', '_def'))

    # 6. Engineer Opponent Offense Features
    print("6. Engineering Opponent Offense features...")
    if not df_offense.empty:
        df_offense.sort_values(['team_abbr', 'week'], inplace=True)
        if 'points_scored' in df_offense.columns: df_offense.rename(columns={'points_scored': 'total_off_points'}, inplace=True)
        if 'opponent_team' in df_offense.columns: df_offense.drop(columns=['opponent_team'], inplace=True)

        for col in ['total_off_points', 'total_yards', 'passing_yards', 'rushing_yards']:
            if col in df_offense.columns:
                df_offense[f'opp_off_rolling_{col}_4_weeks'] = df_offense.groupby('team_abbr')[col].shift(1).rolling(4, min_periods=1).mean()
                for lag in [1, 2, 3]:
                    df_offense[f'opp_off_{col}_lag_{lag}'] = df_offense.groupby('team_abbr')[col].shift(lag)

        df_off_merge = df_offense.rename(columns={'team_abbr': 'opponent_team'})
        df_player = pd.merge(df_player, df_off_merge, on=['opponent_team', 'week'], how='left', suffixes=('', '_off'))

    # 7. Engineer Player Lags & Baseline
    print("7. Engineering Player Lags & Baseline...")
    df_player.sort_values(['player_id', 'week'], inplace=True)
    # df_player['rolling_4wk_avg'] = df_player.groupby('player_id')['y_fantasy_points_ppr'] \
    #     .transform(lambda x: x.shift(1).rolling(window=4, min_periods=1).mean()).fillna(0)
    
    
    # --- MISSING FEATURE CALCS ---
    if 'team_receptions_share' not in df_player.columns: df_player['team_receptions_share'] = 0.0
    if 'team_targets_share' not in df_player.columns: df_player['team_targets_share'] = 0.0
    if 'team_rush_attempts_share' not in df_player.columns: df_player['team_rush_attempts_share'] = 0.0
    
    df_player['ayptarget'] = (df_player['receiving_air_yards'] / df_player['targets']).fillna(0)
    df_player['ypr'] = (df_player['receiving_yards'] / df_player['receptions']).fillna(0)
    df_player['ypc'] = (df_player['rushing_yards'] / df_player['rush_attempts']).fillna(0)
    df_player['touches'] = (df_player['rush_attempts'] + df_player['receptions']).fillna(0)
    df_player['adot'] = (df_player['receiving_air_yards'] / df_player['targets']).fillna(0)
    
    if 'passer_rating' not in df_player.columns: df_player['passer_rating'] = 0.0 
    for col in ['receptions_redzone', 'targets_redzone', 'rush_touchdown_redzone']:
        if col not in df_player.columns: df_player[col] = 0.0

    # --- LAG CALCULATION (history-aware) ---
    # Lags are computed over prior seasons + this season + placeholder rows for
    # the upcoming week, then trimmed back to the current season. Computing them
    # over the current season alone left every lag at 0 in week 1, so the models
    # emitted a near-constant deviation for every player.
    df_player['season'] = SEASON
    df_player['player_id'] = df_player['player_id'].astype(str)

    print("7b. Loading prior seasons to seed lag features...")
    df_prior = load_prior_seasons([SEASON - n for n in range(HISTORY_SEASONS, 0, -1)], df_profiles)
    print(f"   -> {len(df_prior)} prior-season rows")

    print("7c. Adding placeholder rows for the upcoming week...")
    df_schedule = load_table(engine, "schedule")
    if not df_schedule.empty and 'season' in df_schedule.columns:
        df_schedule = df_schedule[df_schedule['season'] == SEASON]
    played = set(df_player['week'].dropna().astype(int)) if 'week' in df_player.columns else set()
    pending = unplayed_games(df_schedule)
    if not pending.empty and 'week' in pending.columns:
        target_week = int(pending['week'].dropna().astype(int).min())
    else:
        target_week = (max(played) if played else 0) + 1

    roster = eligible_players(df_profiles, pending, target_week)
    if not roster.empty and played:
        existing = set(zip(df_player['player_id'], df_player['week'].astype(int)))
        keep = [(pid, target_week) not in existing for pid in roster['player_id']]
        roster = roster[keep]
    df_upcoming = build_upcoming_rows(roster, SEASON, target_week)
    print(f"   -> week {target_week}: {len(df_upcoming)} players to project")

    combined = pd.concat(
        [f for f in (df_prior, df_player, df_upcoming) if f is not None and not f.empty],
        ignore_index=True, sort=False,
    )
    combined = add_player_lags(combined, LAG_COLUMNS)
    combined = add_season_average(combined)

    # Only the current season is written out; prior seasons were scaffolding.
    df_player = combined[combined['season'] == SEASON].copy()
    print(f"   -> {len(df_player)} current-season rows after seeding")

    # 8. Rename Columns to match Model Expectations
    rename_map = {
        'rolling_avg_def_sacks_4_weeks': 'rolling_avg_sack_4_weeks',
        'rolling_avg_def_interceptions_4_weeks': 'rolling_avg_interception_4_weeks',
        'rolling_avg_def_qb_hits_4_weeks': 'rolling_avg_qb_hit_4_weeks',
        'opp_def_def_sacks_lag_1': 'opp_def_sack_lag_1',
    }
    df_player.rename(columns=rename_map, inplace=True)
    df_player['opponent'] = df_player['opponent_team']
    # Only numeric columns get zero-filled. Seeding from prior seasons brings in
    # arrow-backed string columns (names, game ids), and fillna(0) across the
    # whole frame raises TypeError on those.
    _num_cols = df_player.select_dtypes(include=['number']).columns
    df_player[_num_cols] = df_player[_num_cols].fillna(0)

    # 9. Write to DB
    table_name = f"weekly_feature_set_{SEASON}"
    print(f"9. Writing {len(df_player)} rows to DB table: {table_name}...")
    
    try:
        df_player.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"   ✅ Successfully uploaded to Database.")
        
        # 10. Backup to CSV
        df_player.to_csv(OUTPUT_FILE, index=False)
        print(f"   ✅ Backup CSV saved to {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"   ❌ Error writing to DB: {e}")

if __name__ == "__main__":
    main()