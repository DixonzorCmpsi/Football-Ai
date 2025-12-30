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
  is_injury_boosted?: boolean; // <-- Add this line
  // Game Context
  overunder: number | null;
  spread: number | null;
  // Props
  prop_line: number | null; 
  prop_prob: number | null;
  pass_td_line: number | null;
  pass_td_prob: number | null;
  anytime_td_prob: number | null;
  
  injury_status?: string;

}

export interface MatchupData {
  matchup: string;
  week: number;
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
}