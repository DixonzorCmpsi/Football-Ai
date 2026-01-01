export interface PropBet {
  prop_type: string;
  line: number;
  odds: string;
  implied_prob: number;
}

export interface PlayerData {
  player_id: string;
  player_name: string;
  position: string;
  team: string;
  opponent?: string;
  average_points?: number;
  prediction?: number;
  floor_prediction?: number;
  rolling_4wk_avg?: number;
  image?: string;
  week?: number;
  snap_count?: number;
  snap_percentage?: number;
  overunder?: number | null;
  spread?: number | null;
  implied_total?: number | null;
  props?: PropBet[];
  prop_line?: number | null;
  prop_prob?: number | null;
  pass_td_line?: number | null;
  pass_td_prob?: number | null;
  anytime_td_prob?: number | null;
  injury_status?: string;
  is_injury_boosted?: boolean;
  draft_position?: string;
  [key: string]: any;
}

export interface HistoryEntry {
  week: number;
  opponent: string;
  points: number;
  passing_yds: number;
  rushing_yds: number;
  receiving_yds: number;
  touchdowns: number;
  snap_count: number;
  snap_percentage: number;
  receptions: number;
  targets: number;
  carries: number;
}

export interface HistoryItem extends HistoryEntry {
  passing_tds?: number;
  receiving_tds?: number;
  rushing_tds?: number;
}
