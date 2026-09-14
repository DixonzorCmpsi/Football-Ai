import polars as pl
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
import nflreadpy as nfl

# --- Configuration ---
load_dotenv()

current_dir = os.path.dirname(os.path.abspath(__file__))

def get_current_season():
    now = datetime.now()
    return now.year if now.month >= 3 else now.year - 1

SEASON = get_current_season()
OUTPUT_FILE = os.path.join(current_dir, f"weekly_injuries_{SEASON}.csv")
PROFILES_FILE = os.path.join(current_dir, f"player_profiles_{SEASON}.csv")

DEBUG_PLAYERS = ["judkins", "jeanty", "burden", "mccaffrey", "nacua"]

# Sleeper team abbreviations that differ from ours.
SLEEPER_TEAM_ALIASES = {"LAR": "LA", "JAC": "JAX", "WSH": "WAS"}


def normalize(name):
    return str(name).lower().replace(".", "").replace("'", "").strip()


def status_of(info: dict) -> str:
    status = info.get('status')
    inj_status = info.get('injury_status')
    if status == "Injured Reserve" or status == "PUP": return "IR"
    if inj_status: return inj_status
    if status == "Inactive": return "Inactive"
    if status == "Suspended": return "Suspended"
    return "Active"


# Sleeper positions -> the coarser groups our profiles use.
POSITION_GROUPS = {
    "CB": "DB", "S": "DB", "SS": "DB", "FS": "DB", "DB": "DB",
    "DE": "DL", "DT": "DL", "NT": "DL", "DL": "DL", "EDGE": "DL",
    "LB": "LB", "ILB": "LB", "OLB": "LB",
    "T": "OL", "OT": "OL", "G": "OL", "OG": "OL", "C": "OL", "OL": "OL",
    "RB": "RB", "FB": "RB", "WR": "WR", "TE": "TE", "QB": "QB", "K": "K", "P": "P", "LS": "LS",
}


def position_group(position) -> str | None:
    return POSITION_GROUPS.get(str(position or "").upper()) or None


def _team_of(info: dict) -> str | None:
    team = info.get('team')
    return SLEEPER_TEAM_ALIASES.get(team, team) if team else None


def _same_position(info: dict, our: dict) -> bool:
    theirs, ours = position_group(info.get("position")), position_group(our.get("position"))
    return theirs is None or ours is None or theirs == ours


def pick_record(candidates: list[dict], our: dict) -> dict | None:
    """The one Sleeper record that is our player, or None when it can't be told apart."""
    if not candidates:
        return None
    our_team = (our.get("team_abbr") or "").upper() or None
    if len(candidates) == 1:
        theirs = candidates[0]
        # Many real players, injured ones included, have no team in Sleeper, so only
        # a *different* team rules a record out.
        if our_team and _team_of(theirs) and _team_of(theirs) != our_team:
            return None
        return theirs
    same_team = [r for r in candidates if our_team and _team_of(r) == our_team]
    if len(same_team) == 1:
        return same_team[0]
    if same_team:
        return None
    elsewhere = [r for r in candidates if _team_of(r) and _team_of(r) != our_team]
    active = [r for r in candidates if r.get('active') and r not in elsewhere]
    if len(active) == 1:
        return active[0]  # retired namesakes are active=False in Sleeper
    if len({status_of(r) for r in candidates}) == 1:
        return candidates[0]  # every candidate reports the same thing
    return None


def match_statuses(sleeper_players: dict, profiles: list[dict]) -> list[dict]:
    """Our player id -> Sleeper status, without borrowing a namesake's status.

    Sleeper often has no gsis_id for a player (every 2025 rookie, for one), so a
    name fallback is needed. Names are not unique, though: Sleeper lists three
    Kyle Williams, two of them long retired and "Inactive". Matching by name alone
    mapped all three onto the Patriots receiver and kept whichever came last, so
    an active starter showed as Inactive (DJ Moore and eight others too).

    * A Sleeper record carrying our gsis id is authoritative for that player.
    * Otherwise candidates are the same-named records in the same position group
      (which separates our two Devin Neals, a running back and a defensive back),
      narrowed by team, then Sleeper's active flag, then agreement (pick_record).
    """
    ours_by_name: dict[str, list[dict]] = {}
    for p in profiles:
        ours_by_name.setdefault(normalize(p.get("player_name")), []).append(p)

    direct: dict[str, str] = {}
    sleeper_by_name: dict[str, list[dict]] = {}
    for info in sleeper_players.values():
        gsis = str(info.get('gsis_id') or "").strip()
        if gsis:
            direct[gsis] = status_of(info)
        else:
            sleeper_by_name.setdefault(normalize(info.get('full_name', '')), []).append(info)

    records = [{"player_id": pid, "sleeper_status": s} for pid, s in direct.items()]
    for name, all_records in sleeper_by_name.items():
        ours = ours_by_name.get(name, [])
        for our in ours:
            if str(our["player_id"]) in direct:
                continue
            candidates = all_records
            if len(ours) > 1:
                # Two of ours share the name: only a record in our player's position
                # group can be his. Sleeper's positions are finer and sometimes
                # differ from ours (a DL listed as LB), so this is never applied to
                # a name only one of our players has.
                if not position_group(our.get("position")):
                    continue
                candidates = [r for r in all_records
                              if position_group(r.get("position")) == position_group(our.get("position"))]
            theirs = pick_record(candidates, our)
            if theirs is not None:
                records.append({"player_id": str(our["player_id"]), "sleeper_status": status_of(theirs)})
    return records


def fetch_sleeper_map(profiles: list[dict]):
    print("   ☁️  Fetching Sleeper API (Real-time Injuries)...")
    try:
        resp = requests.get("https://api.sleeper.app/v1/players/nfl")
        if resp.status_code != 200: return None
        data = resp.json()
        print(f"      - Scanned {len(data)} players from Sleeper.")
        records = match_statuses(data, profiles)
        for r in records:
            name = next((p["player_name"] for p in profiles if str(p["player_id"]) == r["player_id"]), "")
            if any(d in normalize(name) for d in DEBUG_PLAYERS):
                print(f"      🔎 FOUND {name}: {r['sleeper_status']}")
        if not records: return None
        return pl.DataFrame(records).unique(subset=["player_id"])
    except Exception as e:
        print(f"      ❌ Sleeper Fetch Error: {e}")
        return None

def main():
    print(f"--- 🏥 Updating Injury History ({SEASON}) ---")

    # 1. Load Profiles
    if not os.path.exists(PROFILES_FILE):
        print("❌ Profiles file missing.")
        return
    cols = ["player_id", "player_name", "team_abbr", "position"]
    df_profiles = pl.read_csv(PROFILES_FILE, infer_schema_length=0)
    df_profiles = df_profiles.select([c for c in cols if c in df_profiles.columns]).unique(subset=["player_id"])

    # 2. Fetch Current Data
    df_sleeper = fetch_sleeper_map(df_profiles.to_dicts())

    if df_sleeper is not None:
        # Join & Fill
        current_data = df_profiles.join(df_sleeper, on="player_id", how="left")
        current_data = current_data.with_columns(pl.col("sleeper_status").fill_null("Active").alias("injury_status"))

        # 3. TAG WITH CURRENT WEEK
        try:
            target_week = nfl.get_current_week()
            if target_week == 0: target_week = 18 # Offseason fallback
        except: target_week = 18

        print(f"   📅 Tagging snapshot as: Week {target_week}")
        current_data = current_data.with_columns(pl.lit(target_week).alias("week"))
        current_data = current_data.select(["player_id", "player_name", "injury_status", "week"])

        # 4. APPEND TO HISTORY (The Time Machine Logic)
        final_df = current_data

        if os.path.exists(OUTPUT_FILE):
            print("   🔄 Reading existing history...")
            try:
                # Same dtype on both sides: the fresh week is Int32 from pl.lit, and a
                # mismatch used to throw here and silently overwrite all history.
                history_df = pl.read_csv(OUTPUT_FILE).with_columns(pl.col("week").cast(pl.Int64))
                current_data = current_data.with_columns(pl.col("week").cast(pl.Int64))

                # Check if we already have data for this week
                # If so, DELETE IT (replace it with the fresh fetch)
                # This allows you to re-run the script 5 times on Tuesday without creating 5 duplicates
                history_df = history_df.filter(pl.col("week") != target_week)

                # Stack History + New Data
                final_df = pl.concat([history_df, current_data])
                print(f"   📚 Merged with history. Total Records: {len(final_df)}")

            except Exception as e:
                print(f"   ⚠️ Could not read history ({e}). Overwriting.")

        # 5. Save
        final_df.write_csv(OUTPUT_FILE)
        print(f"   💾 Saved to {OUTPUT_FILE}")

    else:
        print("   ❌ Failed to fetch data.")

if __name__ == "__main__":
    main()
