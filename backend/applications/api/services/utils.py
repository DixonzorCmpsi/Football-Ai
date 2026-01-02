import polars as pl
import numpy as np
from ..config import TEAM_ABBR_MAP
from ..state import model_data

def enforce_types(df: pl.DataFrame) -> pl.DataFrame:
    """Ensures consistent data types for critical columns."""
    if df.is_empty(): return df
    cols = df.columns
    exprs = []
    
    # Force IDs to String
    if "player_id" in cols: exprs.append(pl.col("player_id").cast(pl.Utf8).str.strip_chars())
    
    # Force Week/Season to Int
    if "week" in cols: exprs.append(pl.col("week").fill_null(-1).cast(pl.Int64, strict=False))
    if "season" in cols: exprs.append(pl.col("season").fill_null(-1).cast(pl.Int64, strict=False))
    
    if exprs:
        try: return df.with_columns(exprs)
        except Exception: return df
    return df

def normalize_name(name):
    if not name: return ""
    return str(name).lower().replace(".", "").replace(" ", "").strip()

def get_team_abbr(raw_team):
    clean = str(raw_team).strip()
    return TEAM_ABBR_MAP.get(clean, clean)

def get_headshot_url(player_id: str):
    """Robust Headshot Locator"""
    try:
        if "df_profile" in model_data:
            row = model_data["df_profile"].filter(pl.col("player_id") == player_id)
            if not row.is_empty():
                url = row.row(0, named=True).get("headshot")
                if url and "http" in str(url): return url
    except: pass

    sleeper_id = model_data.get("gsis_to_sleeper", {}).get(player_id)
    if sleeper_id: return f"https://sleepercdn.com/content/nfl/players/{sleeper_id}.jpg"
    
    return "https://sleepercdn.com/images/v2/icons/player_default.webp"

def format_draft_info(year, number):
    if year and number and not np.isnan(number):
        return f"Pick {int(number)} ({int(year)})"
    return "Undrafted"

def calculate_fantasy_points(row):
    try:
        if row.get('y_fantasy_points_ppr') is not None: return float(row['y_fantasy_points_ppr'])
        p_yds = row.get('passing_yards') or 0
        p_tds = row.get('passing_touchdown') or 0
        r_yds = row.get('rushing_yards') or 0
        r_tds = row.get('rush_touchdown') or 0
        rec_yds = row.get('receiving_yards') or 0
        rec_tds = row.get('receiving_touchdown') or 0
        receptions = row.get('receptions') or 0
        ints = row.get('interceptions') or 0
        fumbles = row.get('fumbles_lost') or 0
        return ((p_yds * 0.04) + (p_tds * 4.0) + (r_yds * 0.1) + (r_tds * 6.0) + (rec_yds * 0.1) + (rec_tds * 6.0) + (receptions * 1.0) - (ints * 2.0) - (fumbles * 2.0))
    except: return 0.0
