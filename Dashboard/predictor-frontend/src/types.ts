export interface PlayerProp {
  prop_type: string;
  line: number | null;
  odds: string | number | null;
  implied_prob: number | null;
  /** "over" / "under" for lines; "Yes" for scorer markets. */
  side?: string | null;
}

export interface PlayerData {
  player_name: string;
  player_id: string;
  position: 'QB' | 'RB' | 'WR' | 'TE' | 'FLEX';
  team: string;
  opponent: string;
  image: string;
  week: number;
  prediction: number;
  floor_prediction: number;
  average_points: number;
  /** Full-PPR points scored that week, once the stat line exists (0 for a final game with none). */
  actual_points?: number | null;
  /** The player's game for this week is over. */
  game_final?: boolean;
  snap_percentage?: number;
  is_injury_boosted?: boolean; // <-- Add this line
  // Depth-chart starter (pos_rank == 1). Drives roster ordering so the
  // starting QB/RB/WR/TE lead their position groups.
  is_starter?: boolean;
  // Game Context
  overunder: number | null;
  spread: number | null;
  implied_total?: number | null;
  /** This team's moneyline, e.g. "-135". */
  moneyline?: string | null;
  /** Where the game line came from: Bovada, or the schedule when Bovada has none. */
  lines_source?: 'bovada' | 'schedule' | null;
  /** Every Bovada market for the player this week, both sides. */
  props?: PlayerProp[];
  // Props
  prop_line: number | null; 
  prop_prob: number | null;
  pass_td_line: number | null;
  pass_td_prob: number | null;
  anytime_td_prob: number | null;
  // New Props
  pass_att_line?: number | null;
  pass_att_prob?: number | null;
  rec_line?: number | null;
  rec_prob?: number | null;
  rush_att_line?: number | null;
  rush_att_prob?: number | null;
  
  injury_status?: string;

}

export interface MatchupData {
  matchup: string;
  week: number;
  /** Final score once the game is played; null before. */
  home_score?: number | null;
  away_score?: number | null;
  over_under: number | null;
  home_win_prob: number | null;
  away_win_prob: number | null;
  home_roster: PlayerData[];
  away_roster: PlayerData[];
}

export interface HistoryItem {
  week: number;
  opponent: string;
  points: number;
  passing_yds: number;
  rushing_yds: number;
  receiving_yds: number;
  touchdowns: number;
  snap_percentage: number;
  // Added to fix build error:
  receptions?: number;
  targets?: number;
  carries?: number;
  snap_count?: number;
  team_total_snaps?: number;
}