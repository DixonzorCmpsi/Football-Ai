import json
import re
import os
import math
import polars as pl
from datetime import datetime
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer, util

# --- CONFIGURATION ---
load_dotenv()
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
print(f"DEBUG: DB_CONNECTION_STRING is '{DB_CONNECTION_STRING}'")

current_dir = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(current_dir, "bovada_data")

# AI CONFIGURATION
MODEL_NAME = 'all-MiniLM-L6-v2'
AI_MATCH_THRESHOLD = 0.80

def get_current_season():
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1

SEASON = get_current_season()
OUTPUT_GAME_CSV = os.path.join(current_dir, f"weekly_bovada_game_lines_{SEASON}.csv")
OUTPUT_PLAYER_CSV = os.path.join(current_dir, f"weekly_bovada_player_props_{SEASON}.csv")
CSV_SCHEDULE = os.path.join(current_dir, f"schedule_{SEASON}.csv")
CSV_PROFILES = os.path.join(current_dir, f"player_profiles_{SEASON}.csv")

# --- TEAM MAPPERS ---
# 1. SLUG Map (for URL matching)
BOVADA_SLUG_MAP = {
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

# 2. FULL NAME Map (for Content Fallback Scanning)
BOVADA_NAME_MAP = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE", "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC", "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LA", "Miami Dolphins": "MIA", "Minnesota Vikings": "MIN",
    "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF", "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN", "Washington Commanders": "WAS"
}

BANNED_NAMES = set(k.replace("-", " ").title() for k in BOVADA_SLUG_MAP.keys())
BANNED_NAMES.add("San Francisco 49Ers")
BANNED_NAMES.add("San Francisco 49ers")

# --- SMART MATCHER CLASS ---
class SmartPlayerMatcher:
    def __init__(self, profiles_df):
        print(f"   🧠 Initializing AI Model ({MODEL_NAME})...")
        self.model = SentenceTransformer(MODEL_NAME)
        self.cache = {} 
        self.team_rosters = {}
        
        team_col = 'team' if 'team' in profiles_df.columns else 'team_abbr'
        for row in profiles_df.iter_rows(named=True):
            tm = row.get(team_col)
            nm = row['player_name']
            pos = row['position']
            if tm and nm:
                if tm not in self.team_rosters: self.team_rosters[tm] = {}
                clean_nm = nm.replace(".", "").lower()
                self.team_rosters[tm][clean_nm] = {"real_name": nm, "position": pos}
        print(f"   ✅ Indexed {len(profiles_df)} players across {len(self.team_rosters)} teams.")

    def match(self, raw_name, context_teams):
        raw_clean = raw_name.replace(".", "").lower()
        cache_key = (raw_clean, tuple(sorted(list(context_teams))))
        if cache_key in self.cache: return self.cache[cache_key]

        candidates = []
        candidate_map = {} 
        for team in context_teams:
            if team in self.team_rosters:
                for clean_nm, info in self.team_rosters[team].items():
                    candidates.append(info['real_name'])
                    candidate_map[info['real_name']] = info['position']
                    if clean_nm == raw_clean:
                        self.cache[cache_key] = (info['real_name'], info['position'])
                        return info['real_name'], info['position']
        
        if not candidates: return raw_name, "FLEX"

        for cand in candidates:
            if cand.lower() == raw_name.lower():
                self.cache[cache_key] = (cand, candidate_map[cand])
                return cand, candidate_map[cand]

        # --- OPTIMIZATION: Pre-compute embeddings for all candidates ---
        # Instead of encoding candidates every time, we can encode them once per team or globally.
        # However, since the candidate list changes per context (teams), we'll do a simpler optimization:
        # Use rapidfuzz for string similarity first (much faster), then fallback to BERT only if needed.
        from rapidfuzz import process, fuzz
        
        match = process.extractOne(raw_name, candidates, scorer=fuzz.token_sort_ratio)
        if match:
            best_match, score, _ = match
            if score >= 85: # High confidence string match
                self.cache[cache_key] = (best_match, candidate_map[best_match])
                return best_match, candidate_map[best_match]

        # Fallback to BERT (Heavy) only if string match fails
        # Also, batch encode is faster but here we do single query vs list.
        # To speed this up further, we could cache embeddings of all players in __init__.

        
        self.cache[cache_key] = (raw_name, "FLEX")
        return raw_name, "FLEX"

# --- DATA LOADERS ---
def load_data_source(query: str, csv_path: str, context_name: str) -> pl.DataFrame:
    if DB_CONNECTION_STRING:
        try:
            print(f"   Attempting DB load for {context_name}...")
            df = pl.read_database_uri(query, DB_CONNECTION_STRING)
            if not df.is_empty(): 
                print(f"   ✅ Loaded {len(df)} rows from DB for {context_name}")
                return df
        except Exception as e: 
            print(f"   ❌ DB Load Error for {context_name}: {e}")
            pass
    if os.path.exists(csv_path):
        try: return pl.read_csv(csv_path, ignore_errors=True)
        except: pass
    return pl.DataFrame()

def load_schedule():
    df = load_data_source("SELECT * FROM schedule", CSV_SCHEDULE, "Schedule")
    if not df.is_empty() and "gameday" in df.columns:
        return df.with_columns(pl.col("gameday").cast(pl.Utf8))
    return df

def load_player_profiles():
    df = load_data_source("SELECT player_name, position, team_abbr FROM player_profiles", CSV_PROFILES, "Profiles")
    return df

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
    name = re.sub(r'\s*\([A-Z]+\)$', '', raw_name).strip()
    return name

# --- IMPROVED CONTEXT FINDER ---
def get_game_context(url, raw_lines, df_schedule):
    """
    Identifies the NFL game associated with a file.
    Strategy 1: Check URL for team slugs (e.g. 'baltimore-ravens').
    Strategy 2: Scan file header text for Full Team Names (e.g. 'Baltimore Ravens').
    """
    if df_schedule.is_empty(): return None
    found_teams = set()

    # Strategy 1: URL Scan
    for slug, abbr in BOVADA_SLUG_MAP.items():
        if slug in url: found_teams.add(abbr)
    
    # Strategy 2: Content Scan (Fallback if URL fails)
    # We check the first 100 lines for team names
    if len(found_teams) < 2:
        header_text = " ".join(raw_lines[:100]) # Scan top of file
        for name, abbr in BOVADA_NAME_MAP.items():
            if name in header_text:
                found_teams.add(abbr)

    if len(found_teams) < 2: return None 

    # Determine Game ID from Schedule
    url_date_str = None
    date_match = re.search(r'(202\d{5})', url)
    if date_match: url_date_str = date_match.group(1)

    for row in df_schedule.iter_rows(named=True):
        game_teams = {row['home_team'], row['away_team']}
        
        # Check if the found teams match the schedule
        # found_teams can be > 2 if multiples mentioned, so we check subsets
        if game_teams.issubset(found_teams) or found_teams.issubset(game_teams):
            if url_date_str and row.get('gameday'):
                sched_date = str(row['gameday']).replace("-", "")
                if sched_date != url_date_str: continue 
            
            return {"week": row['week'], "game_id": row['game_id'], "home_team": row['home_team'], "away_team": row['away_team']}
    return None

def extract_game_lines(lines, context):
    gl = {
        "game_id": context['game_id'], "week": context['week'], "season": SEASON,
        "home_team": context['home_team'], "away_team": context['away_team'],
        "total_over": None, "total_over_odds": None, "total_over_prob": None,
        "away_ml": None, "away_ml_prob": None, "home_ml": None, "home_ml_prob": None,
        "total_under": None, "total_under_odds": None, "total_under_prob": None,
        "away_spread": None, "away_spread_odds": None, "away_spread_prob": None,
        "home_spread": None, "home_spread_odds": None, "home_spread_prob": None
    }
    
    over_pattern = re.compile(r'^O\s*(\d+\.?\d*)\s*\(([+-]?\d+|EVEN)\)$')
    ml_pattern = re.compile(r'^([+-]\d+|EVEN)$')
    spread_pattern = re.compile(r'^([+-]?\d+\.?\d*)\s*\(([+-]?\d+|EVEN)\)$')

    mls_found = []
    spreads_found = []
    
    limit = min(len(lines), 60)
    for i in range(limit):
        line = lines[i].strip()
        if line in ["First Half", "1st Quarter", "2nd Quarter"]: break 
        
        o_match = over_pattern.match(line)
        if o_match:
            gl["total_over"] = float(o_match.group(1))
            gl["total_over_odds"] = o_match.group(2)
            gl["total_over_prob"] = american_to_implied_prob(o_match.group(2))
            continue

        m_match = ml_pattern.match(line)
        if m_match and "Bets" not in line and "days" not in line:
            mls_found.append({"line": m_match.group(1), "prob": american_to_implied_prob(m_match.group(1))})
            continue

        s_match = spread_pattern.match(line)
        if s_match:
            spreads_found.append({
                "line": s_match.group(1),
                "odds": s_match.group(2),
                "prob": american_to_implied_prob(s_match.group(2))
            })

    if len(mls_found) >= 2:
        gl["away_ml"] = mls_found[0]["line"]
        gl["away_ml_prob"] = mls_found[0]["prob"]
        gl["home_ml"] = mls_found[1]["line"]
        gl["home_ml_prob"] = mls_found[1]["prob"]

    if len(spreads_found) >= 2:
        gl["away_spread"] = spreads_found[0]["line"]
        gl["away_spread_odds"] = spreads_found[0]["odds"]
        gl["away_spread_prob"] = spreads_found[0]["prob"]
        gl["home_spread"] = spreads_found[1]["line"]
        gl["home_spread_odds"] = spreads_found[1]["odds"]
        gl["home_spread_prob"] = spreads_found[1]["prob"]

    if gl["total_over"] is not None or gl["away_ml"] is not None:
        return gl
    return None

def is_valid_player_prop(player_name, prop_type):
    if player_name in BANNED_NAMES: return False
    ban_words = ["1st Quarter", "1st Half", "2nd Quarter", "Half", "Def/ST", "Defense", "Team", "Total"]
    if any(b in player_name for b in ban_words): return False
    if any(b in prop_type for b in ban_words): return False
    return True

def process_menu_json(filepath, df_schedule, smart_matcher):
    player_props = []
    game_lines = None
    try:
        with open(filepath, 'r', encoding='utf-8') as f: data = json.load(f)
        lines, url = data.get("raw_lines", []), data.get("url", "")
        
        # --- NEW: Pass lines to get_game_context for text scanning ---
        context = get_game_context(url, lines, df_schedule)
        
        if not context: return [], None
        week, game_id = context['week'], context['game_id']
        game_lines = extract_game_lines(lines, context)
        
        match_teams = {context['home_team'], context['away_team']}

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Type A: "Total [Prop] - [Player]"
            if line.startswith("Total ") and " - " in line:
                try:
                    parts = line.split(" - ")
                    prop_type = parts[0].replace("Total ", "")
                    p_name_raw = clean_player_name(parts[1])
                    
                    if is_valid_player_prop(p_name_raw, prop_type):
                        if i + 1 < len(lines):
                            odds_line = lines[i+1].strip()
                            
                            # Sub-Type A1: Single line format (e.g. "O 25.5 (-115)")
                            if odds_line.startswith('+') or odds_line.startswith('-') or odds_line == "EVEN":
                                p_name, p_pos = smart_matcher.match(p_name_raw, match_teams)
                                
                                # Extract line value (e.g. O 25.5)
                                val_match = re.search(r'O\s*(\d+\.?\d*)', odds_line)
                                line_val = val_match.group(1) if val_match else "0"
                                
                                player_props.append({
                                    "player_name": p_name, 
                                    "position": p_pos, 
                                    "prop_type": prop_type,
                                    "line": line_val, 
                                    "odds": odds_line, 
                                    "side": "Over", 
                                    "implied_prob": american_to_implied_prob(odds_line),
                                    "week": week, 
                                    "game_id": game_id, 
                                    "season": SEASON
                                })
                                i += 2
                                continue

                            # Sub-Type A2: Block format (Over / Under / Line / Odds)
                            elif odds_line == "Over" and i + 4 < len(lines):
                                # i+1: Over, i+2: Under, i+3: Line, i+4: Over Odds
                                line_val = lines[i+3].strip()
                                over_odds = lines[i+4].strip()
                                
                                # Basic validation
                                if re.match(r'^\d+\.?\d*$', line_val) and (over_odds.startswith('+') or over_odds.startswith('-') or over_odds == "EVEN"):
                                    p_name, p_pos = smart_matcher.match(p_name_raw, match_teams)
                                    
                                    player_props.append({
                                        "player_name": p_name, 
                                        "position": p_pos, 
                                        "prop_type": prop_type,
                                        "line": line_val, 
                                        "odds": over_odds, 
                                        "side": "Over", 
                                        "implied_prob": american_to_implied_prob(over_odds),
                                        "week": week, 
                                        "game_id": game_id, 
                                        "season": SEASON
                                    })
                                    i += 6
                                    continue
                except Exception as e: 
                    pass

            # Type B: "Anytime Touchdown"
            if "Anytime Touchdown" in line:
                curr = i + 1
                while curr < len(lines):
                    l_curr = lines[curr].strip()
                    if "Props" in l_curr or "Total" in l_curr: break
                    if curr + 1 < len(lines):
                        p_name_raw = clean_player_name(l_curr)
                        p_odds = lines[curr+1].strip()
                        
                        if p_odds.startswith('+') or p_odds.startswith('-') or p_odds == "EVEN":
                            if is_valid_player_prop(p_name_raw, "Anytime Touchdown"):
                                p_name, p_pos = smart_matcher.match(p_name_raw, match_teams)
                                player_props.append({
                                    "player_name": p_name, 
                                    "position": p_pos, 
                                    "prop_type": "Anytime TD",
                                    "line": "1.0", 
                                    "odds": p_odds, 
                                    "side": "Yes",
                                    "implied_prob": american_to_implied_prob(p_odds),
                                    "week": week, 
                                    "game_id": game_id, 
                                    "season": SEASON
                                })
                            curr += 2
                        else: curr += 1
                    else: break
                i = curr; continue
            i += 1
    except Exception as e: print(f"Error reading {filepath}: {e}")
    return player_props, game_lines

def main():
    print(f"--- 🧠 Processing Bovada Data (Season {SEASON}) ---")
    df_schedule = load_schedule()
    if df_schedule.is_empty(): print("❌ Schedule missing."); return
    
    df_profiles = load_player_profiles()
    if df_profiles.is_empty(): print("❌ Player Profiles missing."); return
    
    matcher = SmartPlayerMatcher(df_profiles)
    
    all_player_props, all_game_lines = [], []
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file == "Menu.json":
                p, g = process_menu_json(os.path.join(root, file), df_schedule, matcher)
                if p: all_player_props.extend(p)
                if g: all_game_lines.append(g)
    
    if all_player_props:
        df_p = pl.DataFrame(all_player_props).unique().with_columns(pl.lit(datetime.now().isoformat()).alias("processed_at"))
        df_p.write_csv(OUTPUT_PLAYER_CSV)
        print(f"✅ Saved {len(df_p)} Player Props to {OUTPUT_PLAYER_CSV}")
    else: print("⚠️ No Player Props found.")
    
    if all_game_lines:
        df_g = pl.DataFrame(all_game_lines).unique(subset=["game_id"]).with_columns(pl.lit(datetime.now().isoformat()).alias("processed_at"))
        df_g.write_csv(OUTPUT_GAME_CSV)
        print(f"✅ Saved {len(df_g)} Game Lines to {OUTPUT_GAME_CSV}")

if __name__ == "__main__":
    main()
