from fastapi import APIRouter, HTTPException
import polars as pl
from ..state import model_data
from ..services.prediction import get_team_roster_cards, get_team_injury_report
from ..services.utils import get_team_abbr
from ..config import logger

router = APIRouter()

@router.get("/schedule/{week}")
async def get_schedule(week: int):
    try:
        if model_data["df_schedule"].is_empty(): return []
        
        max_week = model_data["df_schedule"]["week"].max()
        
        # Allow querying future weeks (playoffs) even if empty, don't fallback to max_week if week > max_week
        # This allows the frontend to receive an empty list for Week 19+ instead of Week 18 data
        target_week = week
        
        sched_df = model_data["df_schedule"].filter(pl.col("week") == int(target_week))
        
        # Sort by gameday and gametime (Earliest first)
        if "gameday" in sched_df.columns and "gametime" in sched_df.columns:
            sched_df = sched_df.sort(["gameday", "gametime"])
            logger.info(f"Sorted schedule for Week {week}. First game: {sched_df['home_team'][0]} vs {sched_df['away_team'][0]} at {sched_df['gameday'][0]} {sched_df['gametime'][0]}")
            
        games = sched_df.to_dicts()
        
        matched_count = 0
        if "df_lines" in model_data and not model_data["df_lines"].is_empty():
            lines_df = model_data["df_lines"].filter(pl.col("week") == int(target_week))
            
            # Create a robust lookup map
            odds_map = {}
            for row in lines_df.iter_rows(named=True):
                h_abbr = get_team_abbr(row['home_team'])
                a_abbr = get_team_abbr(row['away_team'])
                odds_map[(h_abbr, a_abbr)] = row
                odds_map[(a_abbr, h_abbr)] = row
            
            for game in games:
                key = (game['home_team'], game['away_team'])
                match = odds_map.get(key)
                
                if match:
                    game['moneyline_home'] = match.get('home_ml')
                    game['moneyline_away'] = match.get('away_ml')
                    game['game_total'] = match.get('total_over')
                    matched_count += 1
                else:
                    game['moneyline_home'] = None
                    game['moneyline_away'] = None
                    game['game_total'] = None
        
        logger.info(f"Schedule (Wk {week}): Odds attached for {matched_count}/{len(games)} games.")
        return games

    except Exception as e:
        logger.exception(f"Schedule endpoint error: {e}")
        return []

@router.get("/matchup/{week}/{home_team}/{away_team}")
async def get_matchup_rosters(week: int, home_team: str, away_team: str):
    try:
        home_cards = await get_team_roster_cards(home_team, week)
        away_cards = await get_team_roster_cards(away_team, week)
        
        home_injuries = get_team_injury_report(home_team, week)
        away_injuries = get_team_injury_report(away_team, week)
        
        over_under, home_win, away_win, spread = None, None, None, None
        gametime, gameday = None, None

        # Fetch Game Time from Schedule
        if "df_schedule" in model_data and not model_data["df_schedule"].is_empty():
            # Try to match by team abbreviations first
            sched_game = model_data["df_schedule"].filter(
                (pl.col("week") == int(week)) & 
                (pl.col("home_team") == home_team) & 
                (pl.col("away_team") == away_team)
            )
            
            if sched_game.is_empty():
                # Fallback: Try to match by checking if the abbreviation is contained in the full name (if schedule uses full names)
                # This is a simple heuristic; ideally we should have a mapping.
                # But first, let's check if we can find it by just one team if the other mismatches
                # Also strip whitespace just in case
                sched_game = model_data["df_schedule"].filter(
                    (pl.col("week") == int(week)) & 
                    ((pl.col("home_team").str.strip_chars() == home_team) | (pl.col("away_team").str.strip_chars() == away_team))
                )

            if not sched_game.is_empty():
                row = sched_game.row(0, named=True)
                gametime = row.get("gametime")
                gameday = row.get("gameday")
                # logger.info(f"Found gametime for {away_team}@{home_team}: {gameday} {gametime}")
            else:
                logger.warning(f"Could not find schedule entry for {away_team}@{home_team} Week {week}. Available games: {model_data['df_schedule'].filter(pl.col('week')==int(week)).select(['home_team', 'away_team']).to_dicts()}")
        
        # Use the same approach as the schedule endpoint to fetch game odds
        if "df_lines" in model_data and not model_data["df_lines"].is_empty():
            lines_df = model_data["df_lines"].filter(pl.col("week") == int(week))
            
            # Extract spread directly from DataFrame without iteration
            if not lines_df.is_empty():
                # Try filtering by both teams
                home_lines = lines_df.filter(
                    (pl.col("home_team") == home_team) & (pl.col("away_team") == away_team)
                )
                if not home_lines.is_empty():
                    row_dict = home_lines.row(0, named=True)
                    over_under = row_dict.get('total_over')
                    home_win = row_dict.get('home_ml_prob')
                    away_win = row_dict.get('away_ml_prob')
                    spread_val = row_dict.get('home_spread')
                    if spread_val is not None:
                        try:
                            spread = float(spread_val)
                        except (ValueError, TypeError):
                            spread = None

        return {
            "matchup": f"{away_team} @ {home_team}",
            "week": week,
            "gametime": gametime,
            "gameday": gameday,
            "over_under": over_under,
            "spread": spread,
            "home_win_prob": home_win,
            "away_win_prob": away_win,
            "home_roster": home_cards,
            "away_roster": away_cards,
            "home_injuries": home_injuries,
            "away_injuries": away_injuries
        }
    except Exception as e: 
        logger.exception(f"Matchup endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load matchup: {str(e)}")
