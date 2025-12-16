import requests
import polars as pl
import os
from datetime import datetime
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer, util

# Load Environment Variables
load_dotenv()
API_KEY = os.getenv("ODDS_API_KEY") # You need to add this to your .env file
# --- Dynamic Season Logic ---
def get_current_season():
    now = datetime.now()
    if now.month >= 3: 
        return now.year
    else: 
        return now.year - 1

SEASON = get_current_season()

# Output File
OUTPUT_FILE = f"weekly_player_odds_{SEASON}.csv"
PROFILES_FILE = "player_profiles.csv"

# Configuration
SPORT = "americanfootball_nfl"
REGIONS = "us" # us | uk | eu | au
MARKETS = "player_pass_yds,player_reception_yds,player_rush_yds,player_receptions,player_anytime_td" # Add more as needed
ODDS_FORMAT = "decimal" # decimal | american
DATE_FORMAT = "iso"

def main():
    if not API_KEY:
        print("❌ Error: ODDS_API_KEY not found in .env")
        return

    print(f"--- Fetching Player Props from The Odds API ---")
    
    # 1. Get Active Games first (to get event IDs)
    events_url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events"
    params = {
        "apiKey": API_KEY,
        "regions": REGIONS,
        "dateFormat": DATE_FORMAT
    }
    
    print("Fetching events...")
    response = requests.get(events_url, params=params)
    if response.status_code != 200:
        print(f"❌ Error fetching events: {response.text}")
        return
        
    events = response.json()
    if not events:
        print("No upcoming games found.")
        return

    # 2. Fetch Props for each event
    all_props = []
    
    print(f"Found {len(events)} upcoming games. Fetching props (this consumes quota)...")
    
    for event in events:
        event_id = event['id']
        home_team = event['home_team']
        away_team = event['away_team']
        commence_time = event['commence_time']
        
        # Determine Week (Simple logic or API provided)
        # Note: The Odds API doesn't explicitly give "Week X", you might need to map it based on date
        # For now, we will just store the date.
        
        props_url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{event_id}/odds"
        prop_params = {
            "apiKey": API_KEY,
            "regions": REGIONS,
            "markets": MARKETS,
            "oddsFormat": ODDS_FORMAT
        }
        
        r = requests.get(props_url, params=prop_params)
        if r.status_code != 200:
            print(f"  ⚠️ Failed to fetch props for {away_team} @ {home_team}")
            continue
            
        data = r.json()
        
        # Process Bookmakers (e.g., DraftKings only to keep it clean)
        bookmakers = data.get('bookmakers', [])
        dk = next((b for b in bookmakers if b['key'] == 'draftkings'), None)
        
        # Fallback to FanDuel or first available if DK missing
        if not dk and bookmakers:
            dk = bookmakers[0]
            
        if not dk: continue
        
        for market in dk.get('markets', []):
            market_name = market['key'] # e.g. player_pass_yds
            
            for outcome in market['outcomes']:
                player_name = outcome['description']
                line = outcome.get('point') # The Over/Under number
                odds = outcome.get('price') # The payout
                side = outcome.get('name') # Over or Under
                
                if line is not None:
                    all_props.append({
                        "player_name": player_name,
                        "team_abbr": None, # API doesn't explicitly link player to team, we must infer
                        "market": market_name,
                        "line": line,
                        "side": side,
                        "odds": odds,
                        "game_date": commence_time
                    })

    if not all_props:
        print("No props found.")
        return

    df_props = pl.DataFrame(all_props)
    print(f"Fetched {len(df_props)} prop lines.")

    # 3. Join with Player Profiles (Using AI Matcher from before!)
    # We reuse the matching logic to link "P. Mahomes" to your DB ID
    # ... (Insert your matching logic here or simple merge) ...
    
    # Save
    df_props.write_csv(OUTPUT_FILE)
    print(f"✅ Odds saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()