"""
Betting Insights Service
Analyzes game lines and player props to generate over/under script betting recommendations
"""
import polars as pl
from typing import List, Dict, Any, Optional, Tuple
from ..config import logger
from ..state import model_data

def calculate_correlation_score(prop_type: str, game_script: str, position: str) -> float:
    """
    Calculate how correlated a prop is with a game script (over/under).
    Returns a score from 0.0 to 1.0.
    
    Over Script (High-scoring game):
    - QB passing yards/TDs highly correlated
    - WR receiving yards highly correlated
    - More passing volume expected
    
    Under Script (Low-scoring game):
    - QB passing yards UNDER highly correlated
    - Rushing yards OVER highly correlated
    - Fewer passing attempts expected
    """
    
    # Over script correlations (high-scoring) - focus on passing game
    over_correlations = {
        'Passing Yards': 0.95,           # Primary indicator
        'Passing Touchdowns': 0.95,       # Primary indicator
        'Receiving Yards': 0.90,          # WRs benefit from pass-heavy
        'Passing Attempts': 0.85,
        'Completions': 0.85,
        'Passing And Rushing Yards': 0.75,
        'Receptions': 0.75,
        'Rushing Yards': 0.40,            # Less relevant in high-scoring
        'Rush Attempts': 0.30,
    }
    
    # Under script correlations (low-scoring) - focus on ground game
    under_correlations = {
        'Rushing Yards': 0.95,            # Primary indicator
        'Rush Attempts': 0.90,
        'Passing Yards': 0.85,            # For UNDER bets
        'Passing Touchdowns': 0.80,       # For UNDER bets
        'Receiving Yards': 0.75,          # For UNDER bets
        'Rushing & Receiving Yards': 0.70,
        'Passing Attempts': 0.60,
        'Completions': 0.60,
        'Passing And Rushing Yards': 0.50,
    }
    
    if game_script == 'over':
        return over_correlations.get(prop_type, 0.3)
    else:  # under
        return under_correlations.get(prop_type, 0.3)


def get_game_insights(week: int, home_team: str, away_team: str) -> Dict[str, Any]:
    """
    Generate betting insights for a specific game.
    Returns top prop combinations for over/under scenarios.
    """
    
    try:
        # Check if lines are available
        if "df_lines" not in model_data or model_data["df_lines"].is_empty():
            return {"status": "no_lines", "message": "No betting lines available yet"}
        
        if "df_props" not in model_data or model_data["df_props"].is_empty():
            return {"status": "no_lines", "message": "No player props available yet"}
        
        # Get game lines
        game_lines = model_data["df_lines"].filter(
            (pl.col("week") == week) &
            (pl.col("home_team") == home_team) &
            (pl.col("away_team") == away_team)
        )
        
        if game_lines.is_empty():
            return {"status": "no_lines", "message": "No lines available for this game yet"}
        
        game_info = game_lines.row(0, named=True)
        over_under = game_info.get('total_over')
        
        if over_under is None:
            return {"status": "no_lines", "message": "Over/under line not set yet"}
        
        # Get player props for both teams
        # We need to find the game_id that matches
        game_id = game_info.get('game_id')
        
        player_props = model_data["df_props"].filter(
            (pl.col("week") == week) &
            (pl.col("game_id") == game_id)
        )
        
        if player_props.is_empty():
            return {"status": "no_lines", "message": "No player props available for this game yet"}
        
        # Generate insights for both scripts
        over_script = generate_script_insights(player_props, 'over', home_team, away_team, over_under, game_info)
        under_script = generate_script_insights(player_props, 'under', home_team, away_team, over_under, game_info)
        
        return {
            "status": "available",
            "game": {
                "home_team": home_team,
                "away_team": away_team,
                "week": week,
                "total": over_under,
                "spread": game_info.get('home_spread'),
            },
            "over_script": over_script,
            "under_script": under_script,
        }
        
    except Exception as e:
        logger.exception(f"Error generating insights: {e}")
        return {"status": "error", "message": f"Error generating insights: {str(e)}"}


def generate_script_insights(
    props_df: pl.DataFrame, 
    script: str, 
    home_team: str, 
    away_team: str,
    total: float,
    game_info: Dict
) -> Dict[str, Any]:
    """
    Generate betting recommendations for a specific game script (over or under).
    Focuses on QB passing yards, rushing yards, passing TDs, and WR receiving yards.
    """
    
    # Filter to only relevant prop types for game script analysis
    relevant_props = [
        'Passing Yards',
        'Passing Touchdowns', 
        'Rushing Yards',
        'Receiving Yards',
        'Passing Attempts',
        'Rush Attempts',
        'Passing And Rushing Yards',
        'Rushing & Receiving Yards',
        'Completions',
        'Receptions'
    ]
    
    # Add correlation scores to all props
    props_with_scores = []
    
    for row in props_df.iter_rows(named=True):
        prop_type = row.get('prop_type')
        position = row.get('position', '')
        player_name = row.get('player_name')
        line = row.get('line')
        odds = row.get('odds')
        side = row.get('side', 'over')
        implied_prob = row.get('implied_prob')
        
        if not all([prop_type, player_name, line]):
            continue
        
        # Filter to only relevant prop types
        if prop_type not in relevant_props:
            continue
        
        # Calculate correlation score
        correlation = calculate_correlation_score(prop_type, script, position)
        
        # Adjust for side (over vs under on the prop)
        if script == 'over':
            # High-scoring game: favor OVER on passing/receiving props
            if side == 'over' and prop_type in ['Passing Yards', 'Receiving Yards', 'Passing Touchdowns', 'Passing Attempts']:
                score = correlation * 1.3  # Strong boost for over on passing
                # Extra boost for QB passing yards and WR receiving yards
                if prop_type == 'Passing Yards' and position == 'QB':
                    score *= 1.2
                elif prop_type == 'Receiving Yards' and position == 'WR':
                    score *= 1.15
                # Boost for favorable odds (higher implied probability = more likely)
                if implied_prob and implied_prob > 0.55:
                    score *= 1.1
            elif side == 'under' and prop_type in ['Rushing Yards', 'Rush Attempts']:
                score = correlation * 0.5  # Penalize rushing unders in high-scoring
            else:
                score = correlation
        else:  # under script
            # Low-scoring game: favor OVER on rushing, UNDER on passing
            if side == 'over' and prop_type in ['Rushing Yards', 'Rush Attempts']:
                score = correlation * 1.3  # Boost rushing overs
            elif side == 'under' and prop_type in ['Passing Yards', 'Receiving Yards', 'Passing Touchdowns']:
                score = correlation * 1.3  # Boost passing/receiving unders
                # Extra boost for QB passing yards under
                if prop_type == 'Passing Yards' and position == 'QB':
                    score *= 1.2
                # Boost for favorable odds
                if implied_prob and implied_prob > 0.55:
                    score *= 1.1
            elif side == 'over' and prop_type in ['Passing Yards', 'Receiving Yards']:
                score = correlation * 0.5  # Penalize passing overs in low-scoring
            else:
                score = correlation
        
        props_with_scores.append({
            'player_name': player_name,
            'position': position,
            'prop_type': prop_type,
            'line': line,
            'odds': odds,
            'side': side,
            'implied_prob': implied_prob,
            'correlation_score': score,
        })
    
    # Sort by correlation score
    props_with_scores.sort(key=lambda x: x['correlation_score'], reverse=True)
    
    # Build 3-4 leg parlays
    parlays = []
    
    # Parlay 1: Top 4 most correlated props (avoid same player duplicates)
    parlay_1 = build_parlay(props_with_scores[:20], max_legs=4, name=f"High Confidence {script.title()}")
    if parlay_1:
        parlays.append(parlay_1)
    
    # Parlay 2: QB/WR focused parlay
    qb_wr_parlay = build_qb_wr_stack(props_with_scores, script)
    if qb_wr_parlay:
        parlays.append(qb_wr_parlay)
    
    # Parlay 3: Balanced approach
    diverse_parlay = build_diverse_parlay(props_with_scores, script)
    if diverse_parlay:
        parlays.append(diverse_parlay)
    
    return {
        "script_type": script,
        "description": get_script_description(script, total),
        "parlays": parlays[:3],  # Return top 3 parlays
        "top_individual_props": props_with_scores[:10],  # Top 10 individual props
    }


def build_parlay(props: List[Dict], max_legs: int, name: str) -> Optional[Dict]:
    """Build a parlay avoiding duplicate players"""
    legs = []
    used_players = set()
    
    for prop in props:
        if len(legs) >= max_legs:
            break
        if prop['player_name'] not in used_players:
            legs.append(prop)
            used_players.add(prop['player_name'])
    
    if len(legs) < 2:
        return None
    
    # Calculate combined odds (American to decimal conversion)
    combined_decimal_odds = 1.0
    for leg in legs:
        odds_str = leg.get('odds', '+100')
        if isinstance(odds_str, str):
            odds_str = odds_str.replace('+', '')
        try:
            american_odds = float(odds_str)
            if american_odds >= 0:
                decimal = (american_odds / 100) + 1
            else:
                decimal = (100 / abs(american_odds)) + 1
            combined_decimal_odds *= decimal
        except:
            combined_decimal_odds *= 1.5  # Default multiplier if can't parse
    
    # Convert back to American odds
    if combined_decimal_odds >= 2.0:
        american_odds = int((combined_decimal_odds - 1) * 100)
        odds_display = f"+{american_odds}"
    else:
        american_odds = int(-100 / (combined_decimal_odds - 1))
        odds_display = str(american_odds)
    
    return {
        "name": name,
        "legs": legs,
        "leg_count": len(legs),
        "combined_odds": odds_display,
        "confidence": sum(l['correlation_score'] for l in legs) / len(legs)
    }


def build_qb_wr_stack(props: List[Dict], script: str) -> Optional[Dict]:
    """
    Build a QB + WR stack parlay focused on passing game.
    For OVER: QB passing yards/TDs + WR receiving yards
    For UNDER: QB passing yards under + WR receiving yards under
    """
    # Focus on passing and receiving props for stack
    if script == 'over':
        qb_props = [p for p in props if p['position'] == 'QB' 
                   and p['prop_type'] in ['Passing Yards', 'Passing Touchdowns']
                   and p['side'] == 'over'
                   and p['correlation_score'] > 0.8]
        wr_props = [p for p in props if p['position'] == 'WR' 
                   and p['prop_type'] == 'Receiving Yards'
                   and p['side'] == 'over'
                   and p['correlation_score'] > 0.8]
    else:  # under
        qb_props = [p for p in props if p['position'] == 'QB' 
                   and p['prop_type'] in ['Passing Yards', 'Passing Touchdowns']
                   and p['side'] == 'under'
                   and p['correlation_score'] > 0.8]
        wr_props = [p for p in props if p['position'] == 'WR' 
                   and p['prop_type'] == 'Receiving Yards'
                   and p['side'] == 'under'
                   and p['correlation_score'] > 0.8]
    
    if not qb_props or not wr_props:
        return None
    
    legs = []
    used_players = set()
    
    # Add top QB (prioritize passing yards over TDs)
    for qb in qb_props:
        if qb['player_name'] not in used_players:
            legs.append(qb)
            used_players.add(qb['player_name'])
            break
    
    # Add 2-3 WRs (these would be WR1/WR2 based on correlation scores)
    for wr in wr_props[:3]:
        if wr['player_name'] not in used_players:
            legs.append(wr)
            used_players.add(wr['player_name'])
        if len(legs) >= 4:
            break
    
    if len(legs) < 2:
        return None
    
    name = f"Pass Heavy {script.title()}" if script == 'over' else f"Pass Under {script.title()}"
    return build_parlay(legs, max_legs=4, name=name)


def build_diverse_parlay(props: List[Dict], script: str) -> Optional[Dict]:
    """Build a parlay with diverse positions"""
    position_priority = ['QB', 'RB', 'WR', 'TE']
    legs = []
    used_players = set()
    used_positions = set()
    
    for pos in position_priority:
        pos_props = [p for p in props if p['position'] == pos and p['player_name'] not in used_players]
        if pos_props:
            legs.append(pos_props[0])
            used_players.add(pos_props[0]['player_name'])
            used_positions.add(pos)
        if len(legs) >= 4:
            break
    
    if len(legs) < 3:
        return None
    
    return build_parlay(legs, max_legs=4, name=f"Balanced {script.title()}")


def get_script_description(script: str, total: float) -> str:
    """Generate description for the game script"""
    if script == 'over':
        return f"High-scoring scenario (Over {total}): QBs will throw more, expect passing yards and receiving yards to go OVER. Focus on QB passing yards/TDs and WR1/WR2 receiving yards."
    else:
        return f"Low-scoring scenario (Under {total}): Run-heavy game flow, expect passing yards and receiving yards to stay UNDER. Focus on QB passing unders and rushing yards overs."
