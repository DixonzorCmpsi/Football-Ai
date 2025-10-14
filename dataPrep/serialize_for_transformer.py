# serialize_labeled_data.py (v7 - Interceptions Feature)
import pandas as pd
from pathlib import Path

# --- Configuration ---
INPUT_PATH = Path("point_range_labeled_dataset.csv")
OUTPUT_PATH = Path("transformer_training_data_v2.csv")

def format_value(value, precision=2, is_percent=False):
    """Safely formats numbers, handling potential missing values (NaN)."""
    if pd.isna(value):
        return "N/A"
    if is_percent:
        return f"{value:.1%}"
    return f"{value:.{precision}f}"

def format_int(value):
    """Safely formats a value as an integer, defaulting to 0 for NaN."""
    if pd.isna(value):
        return 0
    return int(value)

def create_positional_prompt(row):
    """
    Creates a detailed, position-specific prompt using an expert-curated list of features.
    """
    position = row.get('position', 'N/A')

    # --- 1. Base Features ---
    prompt_lines = [
        "Predict the fantasy football performance tier for a player with these stats:",
        "## Player & Game Context",
        f"- Position: {position}",
        f"- Age: {format_int(row.get('age'))}",
        f"- Years Experience: {format_int(row.get('years_exp'))}",
        f"- Opponent: {row.get('opponent', 'N/A')}"
    ]

    # --- 2. Opponent Defensive Matchup ---
    prompt_lines.append("## Opponent Matchup Profile")
    if position == 'QB': pos_allowed_col = 'rolling_avg_points_allowed_to_QB'
    elif position == 'RB': pos_allowed_col = 'rolling_avg_points_allowed_to_RB'
    elif position == 'WR': pos_allowed_col = 'rolling_avg_points_allowed_to_WR'
    elif position == 'TE': pos_allowed_col = 'rolling_avg_points_allowed_to_TE'
    else: pos_allowed_col = None

    if pos_allowed_col and pos_allowed_col in row:
        prompt_lines.append(f"- Avg Fantasy Points Allowed to {position}s (4-wk): {format_value(row.get(pos_allowed_col))}")
    prompt_lines.append(f"- Avg Total Points Allowed (4-wk): {format_value(row.get('rolling_avg_points_allowed_4_weeks'))}")
    
    if position in ['QB', 'WR', 'TE']:
        prompt_lines.append(f"- Avg Sacks per Game (4-wk): {format_value(row.get('rolling_avg_sack_4_weeks'))}")
        prompt_lines.append(f"- Avg Interceptions per Game (4-wk): {format_value(row.get('rolling_avg_interception_4_weeks'))}") # ADDED
        prompt_lines.append(f"- Avg Pass Yards Allowed (4-wk): {format_value(row.get('rolling_avg_passing_yards_allowed_4_weeks'))}")
    if position == 'RB':
        prompt_lines.append(f"- Avg Rush Yards Allowed (4-wk): {format_value(row.get('rolling_avg_rushing_yards_allowed_4_weeks'))}")

    # --- 3. Position-Specific Performance Indicators ---
    if position == 'QB':
        prompt_lines.extend([
            "## QB Performance Indicators",
            f"- Pass Attempts (Last Game): {format_int(row.get('pass_attempts'))}",
            f"- Passing Air Yards (Last Game): {format_int(row.get('passing_air_yards'))}",
            f"- Interceptions (Last Game): {format_int(row.get('interception'))}",
            f"- Passer Rating (Last Game): {format_value(row.get('passer_rating'))}",
            f"- Team Pass Attempt Share (Season): {format_value(row.get('team_pass_attempts_share'), is_percent=True)}",
            f"- Season Avg Pass YPG: {format_value(row.get('season_pass_ypg'))}",
            f"- Career Avg Pass TDs per Game: {format_value(row.get('career_average_pass_touchdown'))}",
            f"- 4-Week Avg Fantasy Points: {format_value(row.get('rolling_avg_fantasy_points_ppr_4_weeks'))}"
        ])

    elif position == 'RB':
        prompt_lines.extend([
            "## RB Performance Indicators",
            f"- Touches (Last Game): {format_int(row.get('touches'))}",
            f"- Rush Attempts (Last Game): {format_int(row.get('rush_attempts'))}",
            f"- Targets (Last Game): {format_int(row.get('targets'))}",
            f"- Team Rush Attempt Share (Season): {format_value(row.get('team_rush_attempts_share'), is_percent=True)}",
            f"- Offense Snap % (Last Game): {format_value(row.get('offense_pct'), is_percent=True)}",
            f"- Red Zone Rush Attempts (Last Game): {format_int(row.get('rush_attempts_redzone'))}",
            f"- Season Avg Rush Attempts per Game: {format_value(row.get('season_average_rush_attempts'))}",
            f"- Career Avg Touches per Game: {format_value(row.get('career_average_touches'))}",
            f"- 4-Week Avg Fantasy Points: {format_value(row.get('rolling_avg_fantasy_points_ppr_4_weeks'))}"
        ])

    elif position in ['WR', 'TE']:
        prompt_lines.extend([
            f"## {position} Performance Indicators",
            f"- Targets (Last Game): {format_int(row.get('targets'))}",
            f"- Receptions (Last Game): {format_int(row.get('receptions'))}",
            f"- Receiving Yards (Last Game): {format_int(row.get('receiving_yards'))}",
            f"- Team Target Share (Season): {format_value(row.get('team_targets_share'), is_percent=True)}",
            f"- Team Receiving Yards Share (Season): {format_value(row.get('team_receiving_yards_share'), is_percent=True)}",
            f"- Offense Snap % (Last Game): {format_value(row.get('offense_pct'), is_percent=True)}",
            f"- Average Depth of Target (Last Game): {format_value(row.get('adot'))}",
            f"- Career Avg Receptions per Game: {format_value(row.get('career_average_receptions'))}",
            f"- 4-Week Avg Fantasy Points: {format_value(row.get('rolling_avg_fantasy_points_ppr_4_weeks'))}"
        ])

    prompt_lines.append("\nWhat is their performance tier?")
    return "\n".join(prompt_lines)

# --- Main Execution ---
print(f"Step 1: Loading labeled dataset from '{INPUT_PATH}'...")
df = pd.read_csv(INPUT_PATH)

print("Step 2: Creating expanded, position-specific prompts...")
records = []
for index, row in df.iterrows():
    prompt = create_positional_prompt(row)
    completion = row['point_range_label']
    records.append({'prompt': prompt, 'completion': completion})

text_df = pd.DataFrame(records)

# --- Step 3: Save the Final Dataset ---
text_df.to_csv(OUTPUT_PATH, index=False)
print(f"\nSuccessfully created transformer-ready dataset: '{OUTPUT_PATH}' ✅")
print(f"This file contains {len(text_df)} highly detailed, position-specific prompts.")