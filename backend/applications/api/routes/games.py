from fastapi import APIRouter, HTTPException, Query
import polars as pl
from typing import Optional
from ..state import model_data
from ..services.prediction import get_team_roster_cards, get_team_injury_report
from ..services.utils import get_team_abbr
from ..services.betting_insights import get_game_insights
from ..services.parlay_recommender import get_parlay_recommendations, ParlayRecommender
from ..services.weather import get_game_weather
from .tier_list import _get_last_season_team_rankings
from ..config import CURRENT_SEASON, logger

router = APIRouter()


def _derive_game_script(over_under, spread, home_off, home_def, away_off, away_def) -> dict | None:
    """Read on how the game is likely to play out, built only from real inputs
    already on the matchup (Vegas total/spread) and last-season team-strength
    ranks — no fabricated grade, just transparent thresholds.
    """
    if over_under is None:
        return None

    abs_spread = abs(spread) if spread is not None else None
    home_implied = round((over_under - spread) / 2, 1) if spread is not None else None
    away_implied = round((over_under + spread) / 2, 1) if spread is not None else None

    if abs_spread is not None and abs_spread >= 9.5:
        tag, label = "BLOWOUT_RISK", "Blowout risk"
        summary = f"Spread of {abs_spread:g} points is the widest signal here — expect a lopsided game script late."
    elif over_under >= 47:
        tag, label = "SHOOTOUT", "Shootout"
        summary = f"O/U {over_under:g} is a high total — both offenses are expected to move the ball."
    elif over_under <= 40:
        tag, label = "GRIND_IT_OUT", "Grind it out"
        summary = f"O/U {over_under:g} is a low total — expect a run-heavy, clock-controlled game."
    else:
        tag, label = "BALANCED", "Balanced"
        summary = f"O/U {over_under:g} is middle-of-the-road — no strong lean either way."

    def side_strength(off, deff):
        bits = []
        if off and off.get("overall"):
            bits.append(f"offense ranks #{off['overall']['rank']}/{off['overall']['rank_out_of']}")
        if deff and deff.get("overall"):
            bits.append(f"defense ranks #{deff['overall']['rank']}/{deff['overall']['rank_out_of']}")
        return ", ".join(bits)

    return {
        "tag": tag,
        "label": label,
        "summary": summary,
        "home_implied_total": home_implied,
        "away_implied_total": away_implied,
        "home_strength_note": side_strength(home_off, home_def) or None,
        "away_strength_note": side_strength(away_off, away_def) or None,
    }

@router.get("/schedule/{week}")
async def get_schedule(week: int):
    try:
        if model_data["df_schedule"].is_empty(): return []
        
        # Allow querying future weeks (playoffs) even if empty, don't fallback to max_week if week > max_week
        # This allows the frontend to receive an empty list for Week 19+ instead of Week 18 data
        target_week = week
        
        sched_df = model_data["df_schedule"].filter(pl.col("week") == int(target_week))
        if "season" in sched_df.columns:
            sched_df = sched_df.filter(pl.col("season").cast(pl.Int64, strict=False) == int(CURRENT_SEASON))
        
        # Sort by gameday and gametime (Earliest first)
        if not sched_df.is_empty() and "gameday" in sched_df.columns and "gametime" in sched_df.columns:
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

        weather = None
        try:
            weather = get_game_weather(home_team, gameday)
        except Exception as e:
            logger.warning(f"Weather lookup failed for {away_team}@{home_team} week {week}: {e}")

        home_rankings, away_rankings = None, None
        game_script = None
        try:
            home_rankings = _get_last_season_team_rankings(home_team)
            away_rankings = _get_last_season_team_rankings(away_team)
            game_script = _derive_game_script(
                over_under,
                spread,
                (home_rankings or {}).get("offense"),
                (home_rankings or {}).get("defense"),
                (away_rankings or {}).get("offense"),
                (away_rankings or {}).get("defense"),
            )
        except Exception as e:
            logger.warning(f"Game script derivation failed for {away_team}@{home_team} week {week}: {e}")

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
            "away_injuries": away_injuries,
            "weather": weather,
            "home_rankings": home_rankings,
            "away_rankings": away_rankings,
            "game_script": game_script,
        }
    except Exception as e: 
        logger.exception(f"Matchup endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load matchup: {str(e)}")

@router.get("/matchup/{week}/{home_team}/{away_team}/insights")
async def get_matchup_insights(week: int, home_team: str, away_team: str):
    """
    Get betting insights for a specific matchup.
    Returns over/under script analysis with recommended parlays.
    """
    try:
        insights = get_game_insights(week, home_team, away_team)
        return insights
    except Exception as e:
        logger.exception(f"Insights endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load insights: {str(e)}")


@router.get("/parlays/{week}")
async def get_week_parlay_recommendations(
    week: int,
    game_id: Optional[str] = Query(None, description="Optional game_id to filter (e.g., 2025_21_LA_SEA)"),
    min_probability: float = Query(45.0, description="Minimum probability threshold (0-100)")
):
    """
    Get parlay recommendations based on game scripts for a given week.
    
    Returns two sets of recommendations:
    - over_script: Props likely to hit if the game goes OVER (high-scoring)
    - under_script: Props likely to hit if the game goes UNDER (low-scoring)
    
    Each recommendation includes:
    - Player name and position
    - Prop type (passing yards, receiving yards, etc.)
    - Line and recommended side (OVER/UNDER)
    - Probability of hitting given the game script
    - Player's season average vs the line
    - Confidence score
    """
    try:
        recommendations = get_parlay_recommendations(model_data, week, game_id)
        recommendations["week"] = week
        recommendations["game_filter"] = game_id
        return recommendations
    except Exception as e:
        logger.exception(f"Parlay recommendations error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate parlays: {str(e)}")


@router.get("/parlays/{week}/{home_team}/{away_team}")
async def get_matchup_parlay_recommendations(
    week: int,
    home_team: str,
    away_team: str,
    min_probability: float = Query(45.0, description="Minimum probability threshold")
):
    """
    Get parlay recommendations for a specific matchup.
    
    Automatically constructs game_id from week, home_team, and away_team.
    """
    try:
        # Construct game_id (format: 2025_21_AWAY_HOME)
        from ..config import CURRENT_SEASON
        game_id = f"{CURRENT_SEASON}_{week}_{away_team}_{home_team}"
        
        recommendations = get_parlay_recommendations(model_data, week, game_id)
        recommendations["week"] = week
        recommendations["game_id"] = game_id
        recommendations["home_team"] = home_team
        recommendations["away_team"] = away_team
        
        # Also try with reversed order in case game_id format varies
        if (recommendations["over_script"]["total_recommendations"] == 0 and 
            recommendations["under_script"]["total_recommendations"] == 0):
            game_id_alt = f"{CURRENT_SEASON}_{week}_{home_team}_{away_team}"
            recommendations = get_parlay_recommendations(model_data, week, game_id_alt)
            recommendations["game_id"] = game_id_alt
            
        return recommendations
    except Exception as e:
        logger.exception(f"Matchup parlay recommendations error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate parlays: {str(e)}")


@router.get("/parlays/{week}/correlated/{player_name}")
async def get_correlated_props(
    week: int,
    player_name: str,
    prop_type: str = Query(..., description="Anchor prop type (e.g., 'Passing Yards')"),
    script: str = Query("over", description="Game script: 'over' or 'under'")
):
    """
    Get props that correlate with an anchor player's prop.
    
    Example: If you bet Josh Allen Passing Yards OVER, what other props correlate?
    - WRs on his team (Receiving Yards OVER)
    - TEs on his team (Receptions OVER)
    """
    try:
        from ..services.parlay_recommender import ParlayRecommender, GameScript
        
        recommender = ParlayRecommender(model_data)
        script_type = GameScript.OVER if script.lower() == "over" else GameScript.UNDER
        
        # Find the game_id for this player
        if not model_data["df_props"].is_empty():
            player_props = model_data["df_props"].filter(
                (pl.col("week") == week) & 
                (pl.col("player_name").str.to_lowercase().str.contains(player_name.lower()))
            )
            
            if not player_props.is_empty():
                game_id = player_props["game_id"][0]
                
                correlations = recommender.get_correlated_props(
                    week, game_id, player_name, prop_type, script_type
                )
                
                return {
                    "anchor_player": player_name,
                    "anchor_prop": prop_type,
                    "script": script,
                    "game_id": game_id,
                    "correlated_props": correlations
                }
        
        return {
            "anchor_player": player_name,
            "anchor_prop": prop_type,
            "script": script,
            "correlated_props": [],
            "message": "No props found for player"
        }
    except Exception as e:
        logger.exception(f"Correlated props error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

