import json
import re
import os
import polars as pl
from datetime import datetime
from difflib import get_close_matches

# --- CONFIGURATION ---
DATA_DIR = "bovada_data"

def get_current_season():
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1

SEASON = get_current_season()
OUTPUT_GAME_CSV = f"weekly_bovada_game_lines_{SEASON}.csv"
OUTPUT_PLAYER_CSV = f"weekly_bovada_player_props_{SEASON}.csv"
SCHEDULE_FILE = f"schedule_{SEASON}.csv"
PROFILES_FILE = f"player_profiles_{SEASON}.csv"

# --- TEAM MAPPER ---
BOVADA_TEAM_MAP = {
    "arizona-cardinals": "ARI", "atlanta-falcons": "ATL", "baltimore-ravens": "BAL",
    "buffalo-bills": "BUF", "carolina-panthers": "CAR", "chicago-bears": "CHI",
    "cincinnati-bengals": "CIN", "cleveland-browns": "CLE", "dallas-cowboys": "DAL",
    "denver-broncos": "DEN", "detroit-lions": "DET", "green-bay-packers": "GB",
    "houston-texans": "HOU", "indianapolis-colts": "IND", "jacksonville-jaguars": "JAX",
    "kansas-city-chiefs": "KC", "las-vegas-raiders": "LV", "los-angeles-chargers": "LAC",
    "los-angeles-rams": "LA", "miami-dolphins": "MIA", "minnesota-vikings": "MIN",
    "new-england-patriots": "NE", "new-orleans-saints": "NO", "new-york-giants": "NYG",
    "new-york-jets": "NYJ", "philadelphia-eagles": "PHI", "pittsburgh-steelers": "PIT",
    "san-francisco-49ers": "SF", "seattle-seahawks": "SEA", "tampa-bay-buccaneers": "TB",
    "tennessee-titans": "TEN", "washington-commanders": "WAS"
}

TEAM_NAME_LOOKUP = {k.replace("-", " ").title(): v for k, v in BOVADA_TEAM_MAP.items()}
TEAM_NAME_LOOKUP["San Francisco 49Ers"] = "SF" 

def load_player_positions():
    if not os.path.exists(PROFILES_FILE): return {}
    try:
        df = pl.read_csv(PROFILES_FILE)
        return dict(zip(df['player_name'], df['position']))
    except: return {}

def american_to_implied_prob(moneyline):
    if not moneyline: return None
    s_line = str(moneyline).strip().upper()
    if s_line == "EVEN": return 50.0
    s_line = re.sub(r'[^\d\-+]', '', s_line)
    try:
        odds = int(s_line)
        if odds < 0: return round((-odds) / (-odds + 100) * 100, 2)
        else: return round(100 / (odds + 100) * 100, 2)
    except: return None

def clean_player_name(raw_name):
    return re.sub(r'\s*\([A-Z]+\)$', '', raw_name).strip()

def get_game_context(url, df_schedule):
    if df_schedule.is_empty(): return None
    found_teams = []
    for slug, abbr in BOVADA_TEAM_MAP.items():
        if slug in url: found_teams.append(abbr)
    if len(found_teams) < 2: return None 
    
    url_date_str = None
    date_match = re.search(r'(202\d{5})', url)
    if date_match: url_date_str = date_match.group(1)

    for row in df_schedule.iter_rows(named=True):
        game_teams = {row['home_team'], row['away_team']}
        if game_teams.issubset(set(found_teams)) or set(found_teams).issubset(game_teams):
            if url_date_str and row.get('gameday'):
                sched_date = str(row['gameday']).replace("-", "")
                if sched_date != url_date_str: continue 
            return {
                "week": row['week'], "game_id": row['game_id'],
                "home_team": row['home_team'], "away_team": row['away_team']
            }
    return None

def extract_game_lines(lines, context):
    gl = {
        "game_id": context['game_id'], "week": context['week'], "season": SEASON,
        "home_team": context['home_team'], "away_team": context['away_team'],
        
        # SPREAD
        "away_spread": None, "away_spread_odds": None, "away_spread_prob": None,
        "home_spread": None, "home_spread_odds": None, "home_spread_prob": None,
        
        # MONEYLINE
        "away_ml": None, "away_ml_prob": None,
        "home_ml": None, "home_ml_prob": None,
        
        # TOTAL
        "total_over": None, "total_over_odds": None, "total_over_prob": None,
        "total_under": None, "total_under_odds": None, "total_under_prob": None
    }
    
    spread_pattern = re.compile(r'^([+-]?\d+\.?\d*)\s*\(([+-]?\d+|EVEN)\)$')
    ml_pattern = re.compile(r'^([+-]?\d+|EVEN)$')
    total_pattern = re.compile(r'^[OU]\s*(\d+\.?\d*)\s*\(([+-]?\d+|EVEN)\)$')

    for i in range(min(len(lines), 30)):
        if total_pattern.match(lines[i]):
            try:
                # 1. Totals
                o_match = total_pattern.match(lines[i])
                u_match = total_pattern.match(lines[i+1])
                
                if o_match: 
                    gl["total_over"], odds = o_match.group(1), o_match.group(2)
                    gl["total_over_odds"] = odds
                    gl["total_over_prob"] = american_to_implied_prob(odds)
                
                if u_match: 
                    gl["total_under"], odds = u_match.group(1), u_match.group(2)
                    gl["total_under_odds"] = odds
                    gl["total_under_prob"] = american_to_implied_prob(odds)
                
                # 2. Moneyline
                if ml_pattern.match(lines[i-1]) and ml_pattern.match(lines[i-2]):
                    gl["home_ml"] = lines[i-1]
                    gl["home_ml_prob"] = american_to_implied_prob(lines[i-1])
                    gl["away_ml"] = lines[i-2]
                    gl["away_ml_prob"] = american_to_implied_prob(lines[i-2])

                # 3. Spread
                if spread_pattern.match(lines[i-3]) and spread_pattern.match(lines[i-4]):
                    hm_spread = spread_pattern.match(lines[i-3])
                    aw_spread = spread_pattern.match(lines[i-4])
                    
                    gl["home_spread"] = hm_spread.group(1)
                    gl["home_spread_odds"] = hm_spread.group(2)
                    gl["home_spread_prob"] = american_to_implied_prob(hm_spread.group(2))
                    
                    gl["away_spread"] = aw_spread.group(1)
                    gl["away_spread_odds"] = aw_spread.group(2)
                    gl["away_spread_prob"] = american_to_implied_prob(aw_spread.group(2))
                
                return gl
            except: continue
    return None

def is_valid_player_prop(player_name, prop_type):
    time_ban_words = [
        "1st Quarter", "2nd Quarter", "3rd Quarter", "4th Quarter", 
        "1st Half", "2nd Half", "1H", "2H", "Quarter", "Half", "First Half", "Second Half"
    ]
    if any(t.lower() in player_name.lower() for t in time_ban_words): return False
    if any(t.lower() in prop_type.lower() for t in time_ban_words): return False

    entity_ban_words = ["Def/ST", "Defense", "Special Teams", "Team", "Total"]
    if any(b in player_name for b in entity_ban_words): return False

    if any(player_name.startswith(t) for t in TEAM_NAME_LOOKUP.keys()): return False

    return True

def process_menu_json(filepath, df_schedule, position_map):
    player_props = []
    game_lines = None

    try:
        with open(filepath, 'r', encoding='utf-8') as f: data = json.load(f)
        lines, url = data.get("raw_lines", []), data.get("url", "")
        
        context = get_game_context(url, df_schedule)
        if not context: return [], None
        week, game_id = context['week'], context['game_id']

        game_lines = extract_game_lines(lines, context)

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # --- Type A: "Total [Prop] - [Player]" ---
            if line.startswith("Total ") and " - " in line:
                try:
                    parts = line.split(" - ")
                    prop_type = parts[0].replace("Total ", "").strip()
                    player_name = clean_player_name(parts[1])
                    
                    if not is_valid_player_prop(player_name, prop_type):
                        i += 1; continue

                    pos = position_map.get(player_name, "FLEX")
                    if pos == "FLEX":
                        if "Passing" in prop_type: pos = "QB"
                        elif "Rushing" in prop_type: pos = "RB"
                        elif "Receiving" in prop_type: pos = "WR/TE"

                    if i + 4 < len(lines) and lines[i+1] == "Over":
                        line_val, odds_val = lines[i+3], lines[i+4]
                        player_props.append({
                            "player_name": player_name, "position": pos, "prop_type": prop_type,
                            "line": line_val, "odds": odds_val, "side": "Over",
                            "implied_prob": american_to_implied_prob(odds_val),
                            "week": week, "game_id": game_id, "season": SEASON
                        })
                except: pass

            # --- Type B: "Anytime Touchdown" ---
            if "Anytime Touchdown" in line:
                curr = i + 1
                while curr < len(lines):
                    if "Props" in lines[curr] or "Total" in lines[curr]: break
                    if curr + 1 < len(lines):
                        p_name, p_odds = clean_player_name(lines[curr]), lines[curr+1]
                        
                        if p_odds.startswith('+') or p_odds.startswith('-') or p_odds == "EVEN":
                            if is_valid_player_prop(p_name, "Anytime Touchdown"):
                                p_pos = position_map.get(p_name, "FLEX")
                                player_props.append({
                                    "player_name": p_name, "position": p_pos, "prop_type": "Anytime TD",
                                    "line": "1.0", "odds": p_odds, "side": "Yes",
                                    "implied_prob": american_to_implied_prob(p_odds),
                                    "week": week, "game_id": game_id, "season": SEASON
                                })
                            curr += 2
                        else: curr += 1
                    else: break
                i = curr; continue

            i += 1
            
    except Exception as e: print(f"Error reading {filepath}: {e}")
    return player_props, game_lines

def main():
    print(f"--- 🧠 Processing Bovada Data -> Separate CSVs ---")
    
    if os.path.exists(SCHEDULE_FILE):
        print(f"    Loading Schedule: {SCHEDULE_FILE}")
        df_schedule = pl.read_csv(SCHEDULE_FILE).with_columns(pl.col("gameday").cast(pl.Utf8))
    else:
        print(f"❌ Schedule file not found."); return

    print(f"    Loading Profiles: {PROFILES_FILE}")
    position_map = load_player_positions()

    all_player_props, all_game_lines = [], []
    
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file == "Menu.json":
                full_path = os.path.join(root, file)
                p_props, g_lines = process_menu_json(full_path, df_schedule, position_map)
                if p_props: all_player_props.extend(p_props)
                if g_lines: all_game_lines.append(g_lines)

    if all_player_props:
        df_p = pl.DataFrame(all_player_props)
        df_p = df_p.with_columns(pl.lit(datetime.now().isoformat()).alias("processed_at"))
        df_p.write_csv(OUTPUT_PLAYER_CSV)
        print(f"✅ Saved {len(df_p)} Player Props to {OUTPUT_PLAYER_CSV}")
    else: print("⚠️ No Player Props found.")

    if all_game_lines:
        df_g = pl.DataFrame(all_game_lines).unique(subset=["game_id"])
        df_g = df_g.with_columns(pl.lit(datetime.now().isoformat()).alias("processed_at"))
        df_g.write_csv(OUTPUT_GAME_CSV)
        print(f"✅ Saved {len(df_g)} Game Lines to {OUTPUT_GAME_CSV}")
    else: print("⚠️ No Game Lines found.")

if __name__ == "__main__":
    main()