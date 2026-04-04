"""
Parlay Recommendation System

This service analyzes player props and recommends parlays based on game scripts:
- OVER Script: High-scoring game scenarios (positive game script, more passing, more plays)
- UNDER Script: Low-scoring game scenarios (run-heavy, clock management, fewer plays)

Uses player averages, historical correlations, and model predictions to calculate
conditional probabilities for props hitting under each game script.
"""

import polars as pl
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger("football-ai")


class GameScript(Enum):
    OVER = "over"  # High-scoring, pass-heavy game
    UNDER = "under"  # Low-scoring, run-heavy game


@dataclass
class PropRecommendation:
    player_name: str
    player_id: Optional[str]
    position: str
    team: str
    prop_type: str
    line: float
    odds: str
    implied_prob: float
    side: str  # "over" or "under" the line
    script_type: GameScript
    script_probability: float  # Probability this hits given the game script
    reasoning: str
    confidence_score: float  # 0-100 confidence in recommendation
    player_avg: Optional[float]  # Player's rolling 4-week average (primary)
    season_avg: Optional[float]  # Player's full season average (backup)
    edge_vs_line: Optional[float]  # Difference between rolling avg and line


@dataclass
class ParlayRecommendation:
    script_type: GameScript
    props: List[PropRecommendation]
    combined_probability: float
    expected_odds: str
    reasoning: str


class ParlayRecommender:
    """
    Recommends player prop parlays based on game script analysis.
    
    Game Script Logic:
    - OVER SCRIPT: Game total goes over → more plays, more yards, more TDs
      - Favors: High receiving yards, high passing yards, pass catchers
      - Looks for: Players who excel in positive game script
      
    - UNDER SCRIPT: Game total goes under → fewer plays, run-heavy, clock management
      - Priority: 1) Passing Yards UNDER, 2) Passing TDs UNDER, 3) Alt Receptions,
                  4) Receiving Yards, 5) Rushing Yards
      - Uses rolling 4-week average to compare vs line
    """
    
    # Stat correlations with game script (positive = benefits from OVER script)
    SCRIPT_CORRELATIONS = {
        "Passing Yards": 0.85,      # Strong positive - more plays = more passing
        "Passing TDs": 0.75,        # Positive - more scoring = more TDs
        "Passing Touchdowns": 0.75,
        "Completions": 0.70,        # Positive - more plays = more completions
        "Passing Attempts": 0.65,   # Positive - but limited by game flow
        "Receiving Yards": 0.80,    # Strong positive - pass-heavy benefits receivers
        "Receptions": 0.75,         # Positive - more targets in pass-heavy
        "Longest Reception": 0.60,  # Moderate - explosive plays in OVER
        "Rushing Yards": -0.30,     # Slight negative - but volume helps
        "Rush Attempts": -0.45,     # Negative - UNDER favors rushing volume
        "Rushing & Receiving Yards": 0.40,  # Mixed - depends on player role
        "Anytime TD": 0.55,         # Moderate positive - more scoring chances
        "Interceptions Thrown": 0.40,  # More passes = more INT chances
    }
    
    # UNDER script priority ranking (lower = higher priority)
    UNDER_SCRIPT_PRIORITY = {
        "Passing Yards": 1,         # #1 most predictable in UNDER
        "Passing TDs": 2,           # #2 most predictable
        "Passing Touchdowns": 2,
        "Receptions": 3,            # #3 alt receptions
        "Receiving Yards": 4,       # #4 receiving yards
        "Rushing Yards": 5,         # #5 rushing yards
        "Rush Attempts": 6,
        "Completions": 7,
        "Passing Attempts": 8,
    }
    
    # Position-based adjustments for game scripts
    POSITION_SCRIPT_BOOST = {
        "QB": {"over": 1.2, "under": 0.8},   # QBs benefit more from OVER
        "WR": {"over": 1.25, "under": 0.7},  # WRs strongly benefit from OVER
        "TE": {"over": 1.15, "under": 0.85}, # TEs moderately benefit
        "RB": {"over": 0.9, "under": 1.15},  # RBs benefit from UNDER
    }
    
    def __init__(self, model_data: Dict):
        """Initialize with model data containing player stats and props."""
        self.model_data = model_data
        # Map to actual keys used in data_loader.py
        self.df_props = model_data.get("df_props", pl.DataFrame())
        self.df_stats = model_data.get("df_player_stats", pl.DataFrame())  # Changed from df_weekly_stats
        self.df_profiles = model_data.get("df_profile", pl.DataFrame())    # Changed from df_profiles
        self.df_game_lines = model_data.get("df_lines", pl.DataFrame())    # Changed from df_game_lines
        
    def get_player_averages(self, player_name: str, stat_type: str, rolling_weeks: int = 4) -> Tuple[Optional[float], Optional[float]]:
        """
        Get player's rolling average (last N weeks) and season average for a stat.
        
        Returns:
            Tuple of (rolling_avg, season_avg) - rolling_avg is primary for probability
        """
        if self.df_stats.is_empty():
            return None, None
            
        # Map prop types to stat column names
        stat_map = {
            "Passing Yards": "passing_yards",
            "Passing TDs": "passing_touchdown",
            "Passing Touchdowns": "passing_touchdown",
            "Completions": "completions",
            "Passing Attempts": "attempts",
            "Receiving Yards": "receiving_yards",
            "Receptions": "receptions",
            "Rushing Yards": "rushing_yards",
            "Rush Attempts": "rush_attempts",
            "Rushing & Receiving Yards": ["rushing_yards", "receiving_yards"],
            "Passing And Rushing Yards": ["passing_yards", "rushing_yards"],
            "Interceptions Thrown": "interception",
            "Anytime TD": ["receiving_touchdown", "rush_touchdown"],
        }
        
        stat_col = stat_map.get(stat_type)
        if not stat_col:
            return None, None
            
        try:
            # First try to find player_id from profiles
            player_id = None
            if not self.df_profiles.is_empty():
                # Normalize player name for matching
                player_match = self.df_profiles.filter(
                    pl.col("player_name").str.to_lowercase() == player_name.strip().lower()
                )
                if not player_match.is_empty():
                    player_id = player_match["player_id"][0]
            
            if player_id is None:
                # Try fuzzy matching on stats table if it has player_name
                if "player_name" in self.df_stats.columns:
                    player_stats = self.df_stats.filter(
                        pl.col("player_name").str.to_lowercase() == player_name.strip().lower()
                    )
                else:
                    return None, None
            else:
                # Filter by player_id
                player_stats = self.df_stats.filter(pl.col("player_id") == player_id)
            
            if player_stats.is_empty():
                return None, None
            
            # Sort by week descending to get most recent games
            if "week" in player_stats.columns:
                player_stats = player_stats.sort("week", descending=True)
            
            # Calculate rolling average (last N weeks)
            rolling_stats = player_stats.head(rolling_weeks)
            
            def calc_stat_value(df, cols):
                """Calculate stat value, handling single or combined columns."""
                if isinstance(cols, list):
                    total = 0
                    for col in cols:
                        if col in df.columns:
                            col_mean = df[col].mean()
                            if col_mean is not None:
                                total += col_mean
                    return total if total else None
                else:
                    if cols in df.columns:
                        return df[cols].mean()
                return None
            
            rolling_avg = calc_stat_value(rolling_stats, stat_col)
            season_avg = calc_stat_value(player_stats, stat_col)
            
            rolling_avg = round(rolling_avg, 1) if rolling_avg is not None else None
            season_avg = round(season_avg, 1) if season_avg is not None else None
            
            return rolling_avg, season_avg
            
        except Exception as e:
            logger.debug(f"Error getting player average for {player_name}: {e}")
            
        return None, None
    
    def calculate_probability_vs_line(
        self, 
        rolling_avg: Optional[float], 
        season_avg: Optional[float],
        line: float,
        side: str
    ) -> float:
        """
        Calculate probability of hitting a prop based on player average vs line.
        
        Uses rolling 4-week average as primary, season avg as secondary.
        """
        # Use rolling avg if available, otherwise season avg
        player_avg = rolling_avg if rolling_avg is not None else season_avg
        
        if player_avg is None or line <= 0:
            return 50.0  # Default to 50% if no data
        
        # Calculate how far the line is from player's average
        diff = player_avg - line
        diff_pct = diff / line
        
        # Base probability calculation
        # If player averages OVER the line, OVER has higher probability
        # If player averages UNDER the line, UNDER has higher probability
        if side.lower() == "over":
            # For OVER: positive diff = higher probability
            if diff_pct > 0.30:
                prob = 75.0 + min(diff_pct * 30, 15)  # 75-90%
            elif diff_pct > 0.15:
                prob = 65.0 + (diff_pct - 0.15) * 66.67  # 65-75%
            elif diff_pct > 0:
                prob = 55.0 + diff_pct * 66.67  # 55-65%
            elif diff_pct > -0.15:
                prob = 45.0 + diff_pct * 66.67  # 35-55%
            else:
                prob = 35.0 + max(diff_pct + 0.15, -0.20) * 50  # 25-35%
        else:  # UNDER
            # For UNDER: negative diff = higher probability
            if diff_pct < -0.30:
                prob = 75.0 + min(abs(diff_pct) * 30, 15)  # 75-90%
            elif diff_pct < -0.15:
                prob = 65.0 + (abs(diff_pct) - 0.15) * 66.67  # 65-75%
            elif diff_pct < 0:
                prob = 55.0 + abs(diff_pct) * 66.67  # 55-65%
            elif diff_pct < 0.15:
                prob = 45.0 - diff_pct * 66.67  # 35-55%
            else:
                prob = 35.0 - min(diff_pct - 0.15, 0.20) * 50  # 25-35%
        
        return round(max(20.0, min(90.0, prob)), 1)
    
    def calculate_script_probability(
        self, 
        prop_type: str, 
        line: float, 
        player_avg: Optional[float],
        position: str,
        script: GameScript,
        side: str = "over"
    ) -> Tuple[float, str]:
        """
        Calculate probability of prop hitting given a game script.
        
        Returns (probability, reasoning)
        """
        base_correlation = self.SCRIPT_CORRELATIONS.get(prop_type, 0)
        position_boost = self.POSITION_SCRIPT_BOOST.get(position, {"over": 1.0, "under": 1.0})
        
        # Base probability from implied odds (assume -110 = 52.4% if unknown)
        base_prob = 0.524
        
        # Adjust for game script correlation
        if script == GameScript.OVER:
            script_modifier = 1 + (base_correlation * 0.15)  # Up to 15% boost
            script_modifier *= position_boost["over"]
        else:  # UNDER
            script_modifier = 1 - (base_correlation * 0.15)  # Inverse for UNDER
            script_modifier *= position_boost["under"]
        
        # Adjust based on player average vs line
        avg_modifier = 1.0
        reasoning_parts = []
        
        if player_avg is not None and line > 0:
            edge = player_avg - line
            edge_pct = edge / line if line > 0 else 0
            
            if side == "over":
                if edge > 0:
                    avg_modifier = 1 + min(edge_pct * 0.5, 0.25)  # Up to 25% boost
                    reasoning_parts.append(f"Avg ({player_avg}) > Line ({line})")
                else:
                    avg_modifier = 1 + max(edge_pct * 0.3, -0.15)  # Up to 15% penalty
                    reasoning_parts.append(f"Avg ({player_avg}) < Line ({line})")
            else:  # under
                if edge < 0:
                    avg_modifier = 1 + min(abs(edge_pct) * 0.5, 0.25)
                    reasoning_parts.append(f"Avg ({player_avg}) < Line ({line})")
                else:
                    avg_modifier = 1 - min(edge_pct * 0.3, 0.15)
                    reasoning_parts.append(f"Avg ({player_avg}) > Line ({line})")
        
        # Calculate final probability
        final_prob = base_prob * script_modifier * avg_modifier
        final_prob = max(0.15, min(0.90, final_prob))  # Clamp between 15-90%
        
        # Build reasoning
        script_name = "OVER" if script == GameScript.OVER else "UNDER"
        if base_correlation > 0.5:
            reasoning_parts.append(f"Strong {script_name} correlation")
        elif base_correlation > 0:
            reasoning_parts.append(f"Positive {script_name} correlation")
        elif base_correlation < -0.3:
            reasoning_parts.append(f"Benefits from {script_name} script")
        
        reasoning = "; ".join(reasoning_parts) if reasoning_parts else f"Neutral {script_name} play"
        
        return round(final_prob * 100, 1), reasoning
    
    def get_game_total(self, game_id: str) -> Optional[float]:
        """Get the over/under total for a game."""
        if self.df_game_lines.is_empty():
            return None
            
        try:
            game = self.df_game_lines.filter(pl.col("game_id") == game_id)
            if not game.is_empty() and "total_over" in game.columns:
                return game["total_over"][0]
        except:
            pass
        return None
    
    def analyze_props_for_script(
        self, 
        week: int, 
        script: GameScript,
        game_id: Optional[str] = None,
        min_probability: float = 40.0
    ) -> List[PropRecommendation]:
        """
        Analyze all props for a given week and game script.
        
        For UNDER script, prioritizes:
        1. Passing Yards UNDER
        2. Passing TDs UNDER
        3. Alt Receptions (WR/TE)
        4. Receiving Yards
        5. Rushing Yards
        
        Uses rolling 4-week average to calculate probability vs line.
        
        Returns list of prop recommendations sorted by priority then probability.
        """
        if self.df_props.is_empty():
            return []
            
        recommendations = []
        
        # Filter props by week and optionally game
        week_props = self.df_props.filter(pl.col("week") == week)
        if game_id:
            week_props = week_props.filter(pl.col("game_id") == game_id)
        
        if week_props.is_empty():
            return []
        
        # Process each prop
        for row in week_props.iter_rows(named=True):
            player_name = row.get("player_name", "")
            prop_type = row.get("prop_type", "")
            line = row.get("line", 0)
            odds = row.get("odds", "-110")
            implied_prob = row.get("implied_prob", 52.38)
            position = row.get("position", "FLEX")
            game_id_prop = row.get("game_id", "")
            
            # Skip invalid props
            if not player_name or line <= 0:
                continue
                
            # Skip half/quarter props
            if "1H" in player_name or "1Q" in player_name or "First Half" in player_name:
                continue
            
            # Get player rolling average (4 weeks) and season average
            rolling_avg, season_avg = self.get_player_averages(player_name, prop_type)
            
            # Use rolling avg as primary, season avg as backup
            player_avg = rolling_avg if rolling_avg is not None else season_avg
            
            # Determine optimal side based on script and prop type
            correlation = self.SCRIPT_CORRELATIONS.get(prop_type, 0)
            
            if script == GameScript.UNDER:
                # UNDER SCRIPT LOGIC - ALL bets should be UNDER
                # In low-scoring games, passing/receiving go DOWN
                # Priority: 1) Passing Yards, 2) Passing TDs, 3) Receptions, 4) Receiving Yards, 5) Rushing Yards
                
                if prop_type in ("Passing Yards", "Passing TDs", "Passing Touchdowns"):
                    # #1 and #2 priority: QB passing props UNDER
                    if position == "QB":
                        side = "under"  # Bet UNDER on passing in low-scoring games
                    else:
                        side = "skip"
                elif prop_type == "Receptions":
                    # #3 priority: Receptions UNDER for WR/TE
                    if position in ("WR", "TE"):
                        side = "under"  # Always UNDER for receptions in low-scoring games
                    else:
                        side = "skip"
                elif prop_type == "Receiving Yards":
                    # #4 priority: Receiving yards UNDER
                    if position in ("WR", "TE"):
                        side = "under"  # Always UNDER for receiving yards in low-scoring games
                    else:
                        side = "skip"
                elif prop_type == "Rushing Yards":
                    # #5 priority: Rushing yards UNDER for RBs
                    if position == "RB":
                        side = "under"  # UNDER on rushing in low-scoring games (fewer total plays)
                    else:
                        side = "skip"
                elif prop_type == "Rush Attempts":
                    # Also include rush attempts UNDER
                    if position == "RB":
                        side = "under"  # Fewer total plays in UNDER games
                    else:
                        side = "skip"
                elif "TD" in prop_type or "Touchdown" in prop_type:
                    # TD props UNDER in low-scoring games
                    if prop_type in ("Passing TDs", "Passing Touchdowns"):
                        side = "under"
                    else:
                        side = "skip"  # Skip anytime TD props
                else:
                    side = "skip"  # Skip other prop types in UNDER script
                    
            else:  # OVER SCRIPT
                # Special handling for TD props (binary outcomes)
                if "TD" in prop_type or "Touchdown" in prop_type:
                    if position in ("WR", "TE", "QB"):
                        side = "over"  # Pass catchers score more TDs in high-scoring games
                    else:
                        side = "skip"  # Skip RB TDs in pass-heavy scripts
                else:
                    # For yardage/counting stats: use correlation logic
                    side = "over" if correlation >= 0 else "under"
                    
                    # Override based on rolling average edge
                    if player_avg is not None and line > 0:
                        edge_pct = (player_avg - line) / line
                        if edge_pct > 0.15:  # Player averages 15%+ over line
                            side = "over"
                        elif edge_pct < -0.15:  # Player averages 15%+ under line
                            side = "under"
            
            # Skip if marked for skip
            if side == "skip":
                continue
            
            # Calculate probability based on rolling average vs line
            prob_vs_line = self.calculate_probability_vs_line(rolling_avg, season_avg, line, side)
            
            # Adjust probability based on game script correlation
            script_boost = 1.0
            if script == GameScript.UNDER and prop_type in ("Passing Yards", "Passing TDs", "Passing Touchdowns"):
                script_boost = 1.15  # Strong UNDER correlation for passing in low-scoring games
            elif script == GameScript.UNDER and prop_type == "Receptions":
                script_boost = 1.10  # Good UNDER correlation for receptions
            elif script == GameScript.UNDER and prop_type == "Receiving Yards":
                script_boost = 1.08
            elif script == GameScript.UNDER and prop_type in ("Rushing Yards", "Rush Attempts"):
                script_boost = 1.05  # RBs benefit slightly in UNDER
            elif script == GameScript.OVER and correlation > 0.5:
                script_boost = 1.12  # Strong OVER correlation props
            
            script_prob = round(min(90.0, prob_vs_line * script_boost), 1)
            
            # Build reasoning
            reasoning_parts = []
            if rolling_avg is not None:
                reasoning_parts.append(f"L4 avg: {rolling_avg}")
            if season_avg is not None and season_avg != rolling_avg:
                reasoning_parts.append(f"Season: {season_avg}")
            if player_avg is not None:
                edge = player_avg - line
                if abs(edge) > line * 0.1:
                    direction = "above" if edge > 0 else "below"
                    reasoning_parts.append(f"Avg {abs(edge):.1f} {direction} line")
            reasoning = "; ".join(reasoning_parts) if reasoning_parts else f"Game script: {script.value}"
            
            # Only include if meets minimum probability
            if script_prob < min_probability:
                continue
            
            # Calculate confidence score
            confidence = self._calculate_confidence(
                script_prob, player_avg, line, implied_prob
            )
            
            # Get team from game_id
            team = self._extract_team_from_game_id(game_id_prop, player_name)
            
            # Get player_id
            player_id = self._get_player_id(player_name)
            
            edge = round(rolling_avg - line, 1) if rolling_avg else (round(season_avg - line, 1) if season_avg else None)
            
            # Get priority for UNDER script sorting
            priority = self.UNDER_SCRIPT_PRIORITY.get(prop_type, 99) if script == GameScript.UNDER else 0
            
            recommendations.append(PropRecommendation(
                player_name=player_name,
                player_id=player_id,
                position=position,
                team=team,
                prop_type=prop_type,
                line=line,
                odds=odds,
                implied_prob=implied_prob,
                side=side,
                script_type=script,
                script_probability=script_prob,
                reasoning=reasoning,
                confidence_score=confidence,
                player_avg=rolling_avg,  # Primary: rolling 4-week average
                season_avg=season_avg,   # Secondary: full season average
                edge_vs_line=edge
            ))
        
        # Sort: For UNDER script, sort by priority first, then by probability
        # For OVER script, sort by probability
        if script == GameScript.UNDER:
            recommendations.sort(key=lambda x: (
                self.UNDER_SCRIPT_PRIORITY.get(x.prop_type, 99),
                -x.script_probability
            ))
        else:
            recommendations.sort(key=lambda x: x.script_probability, reverse=True)
        
        return recommendations
    
    def _calculate_confidence(
        self, 
        script_prob: float, 
        player_avg: Optional[float], 
        line: float,
        implied_prob: float
    ) -> float:
        """Calculate confidence score based on multiple factors."""
        confidence = script_prob * 0.5  # Base from script probability
        
        # Bonus for having player average data
        if player_avg is not None:
            confidence += 10
            
            # Bonus for significant edge
            edge_pct = abs(player_avg - line) / line if line > 0 else 0
            if edge_pct > 0.15:
                confidence += 15
            elif edge_pct > 0.1:
                confidence += 10
            elif edge_pct > 0.05:
                confidence += 5
        
        # Adjust for implied probability edge
        if implied_prob < 50:  # Plus odds
            confidence += 5  # Bonus for underdog value
        
        return min(100, round(confidence, 1))
    
    def _extract_team_from_game_id(self, game_id: str, player_name: str) -> str:
        """Extract team abbreviation from player profile or game_id."""
        # First try to get team from player profiles
        if not self.df_profiles.is_empty():
            try:
                player = self.df_profiles.filter(
                    pl.col("player_name").str.to_lowercase() == player_name.strip().lower()
                )
                if not player.is_empty() and "team_abbr" in player.columns:
                    team = player["team_abbr"][0]
                    if team:
                        return team
            except:
                pass
        
        # Fallback to game_id
        if not game_id:
            return "UNK"
        # game_id format: 2025_21_AWAY_HOME
        parts = game_id.split("_")
        if len(parts) >= 4:
            # Return both teams since we don't know which one
            return f"{parts[2]}/{parts[3]}"
        return "UNK"
    
    def _get_player_id(self, player_name: str) -> Optional[str]:
        """Look up player_id from profiles."""
        if self.df_profiles.is_empty():
            return None
        try:
            player = self.df_profiles.filter(
                pl.col("player_name").str.to_lowercase() == player_name.lower()
            )
            if not player.is_empty():
                return player["player_id"][0]
        except:
            pass
        return None
    
    def generate_parlay_recommendations(
        self,
        week: int,
        game_id: Optional[str] = None,
        max_props_per_script: int = 100,
        min_probability: float = 40.0
    ) -> Dict[str, List[Dict]]:
        """
        Generate parlay recommendations for both game scripts.
        
        Returns dict with 'over_script' and 'under_script' recommendations.
        """
        results = {
            "over_script": {
                "description": "Parlays for HIGH-SCORING game (total goes OVER)",
                "game_flow": "Pass-heavy, more plays, positive game scripts",
                "favors": ["Passing yards", "Receiving yards", "Receptions", "TDs"],
                "strategy": "In OVER games, teams are passing more to score or catch up. Target WR/TE receiving props and QB passing props.",
                "methodology": "Uses rolling 4-week average to compare against betting line",
                "recommendations": []
            },
            "under_script": {
                "description": "Parlays for LOW-SCORING game (total goes UNDER)",
                "game_flow": "Run-heavy, clock management, fewer plays",
                "priority_order": ["1. Passing Yards UNDER", "2. Passing TDs UNDER", "3. Alt Receptions", "4. Receiving Yards", "5. Rushing Yards"],
                "strategy": "In UNDER games, QBs throw less, receivers get fewer targets. Target QB passing UNDER and RB rushing props.",
                "methodology": "Uses rolling 4-week average to calculate probability vs line",
                "recommendations": []
            }
        }
        
        # Get OVER script recommendations
        over_recs = self.analyze_props_for_script(
            week, GameScript.OVER, game_id, min_probability
        )
        
        for rec in over_recs[:max_props_per_script]:
            results["over_script"]["recommendations"].append({
                "player_name": rec.player_name,
                "player_id": rec.player_id,
                "position": rec.position,
                "team": rec.team,
                "prop_type": rec.prop_type,
                "line": rec.line,
                "side": rec.side.upper(),
                "odds": rec.odds,
                "probability": rec.script_probability,
                "confidence": rec.confidence_score,
                "rolling_avg": rec.player_avg,  # L4 avg (primary)
                "season_avg": rec.season_avg,   # Full season avg
                "edge": rec.edge_vs_line,
                "reasoning": rec.reasoning
            })
        
        # Get UNDER script recommendations
        under_recs = self.analyze_props_for_script(
            week, GameScript.UNDER, game_id, min_probability
        )
        
        for rec in under_recs[:max_props_per_script]:
            results["under_script"]["recommendations"].append({
                "player_name": rec.player_name,
                "player_id": rec.player_id,
                "position": rec.position,
                "team": rec.team,
                "prop_type": rec.prop_type,
                "line": rec.line,
                "side": rec.side.upper(),
                "odds": rec.odds,
                "probability": rec.script_probability,
                "confidence": rec.confidence_score,
                "rolling_avg": rec.player_avg,  # L4 avg (primary)
                "season_avg": rec.season_avg,   # Full season avg
                "edge": rec.edge_vs_line,
                "reasoning": rec.reasoning
            })
        
        # Add summary stats
        results["over_script"]["total_recommendations"] = len(results["over_script"]["recommendations"])
        results["under_script"]["total_recommendations"] = len(results["under_script"]["recommendations"])
        
        return results
    
    def get_correlated_props(
        self,
        week: int,
        game_id: str,
        anchor_player: str,
        anchor_prop_type: str,
        script: GameScript
    ) -> List[Dict]:
        """
        Find props that correlate well with an anchor prop.
        
        For example: If QB passing yards goes over, what WR props also benefit?
        """
        correlations = []
        
        props = self.analyze_props_for_script(week, script, game_id, 40.0)
        
        # Define correlation rules
        prop_correlations = {
            "Passing Yards": ["Receiving Yards", "Receptions", "Passing TDs"],
            "Receiving Yards": ["Passing Yards", "Receptions"],
            "Rushing Yards": ["Rush Attempts"],
            "Receptions": ["Receiving Yards"],
        }
        
        correlated_types = prop_correlations.get(anchor_prop_type, [])
        
        for prop in props:
            # Skip the anchor player
            if prop.player_name.lower() == anchor_player.lower():
                continue
                
            # Check if prop type correlates
            if prop.prop_type in correlated_types or prop.prop_type == anchor_prop_type:
                correlations.append({
                    "player_name": prop.player_name,
                    "prop_type": prop.prop_type,
                    "line": prop.line,
                    "side": prop.side.upper(),
                    "correlation_reason": f"Correlates with {anchor_player} {anchor_prop_type}",
                    "probability": prop.script_probability
                })
        
        # Sort by probability
        correlations.sort(key=lambda x: x["probability"], reverse=True)
        
        return correlations[:20]


def get_parlay_recommendations(model_data: Dict, week: int, game_id: Optional[str] = None) -> Dict:
    """
    Main entry point for getting parlay recommendations.
    
    Args:
        model_data: Dict containing df_props, df_weekly_stats, df_profiles, df_game_lines
        week: NFL week number
        game_id: Optional specific game (e.g., "2025_21_LA_SEA")
        
    Returns:
        Dict with over_script and under_script recommendations
    """
    recommender = ParlayRecommender(model_data)
    return recommender.generate_parlay_recommendations(week, game_id)
