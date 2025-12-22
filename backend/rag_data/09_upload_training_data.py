import sys
from pathlib import Path

# Path to your local data folder (relative to this script)
DATA_DIR = Path(__file__).parent.parent / "dataPrep" / "data"

REQUIRED_FILES = [
    "yearly_player_stats_offense.csv",
    "weekly_player_stats_offense.csv",
    "weekly_team_stats_offense.csv",
    "weekly_team_stats_defense.csv"
]

def main():
    print("--- 🔍 Verifying Historical Training Data Files ---")
    
    if not DATA_DIR.exists():
        print(f"❌ Error: Data directory not found at {DATA_DIR}")
        sys.exit(1)

    all_good = True
    for filename in REQUIRED_FILES:
        file_path = DATA_DIR / filename
        if not file_path.exists():
            print(f"   ❌ Missing: {filename}")
            all_good = False
        else:
            print(f"   ✅ Found: {filename}")
    
    if not all_good:
        print("Some historical files are missing. Please check backend/dataPrep/data/")
        sys.exit(1)
        
    print("Files ready for ETL upload (if not already in DB).")

if __name__ == "__main__":
    main()