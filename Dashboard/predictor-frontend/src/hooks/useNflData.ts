import { useState, useEffect } from 'react';
import axios from 'axios';

// Runtime-configurable API base. Defaults to relative `/api` so the frontend can be
// served anywhere and proxy requests to the backend (nginx will proxy /api -> backend).
// Use the typed `window.__env` declaration (see `src/types/env.d.ts`) to avoid `any`.
const API_BASE_URL = (typeof window !== 'undefined' && window.__env && window.__env.API_BASE_URL) ? window.__env.API_BASE_URL : '/api'; 

export interface PropBet {
  prop_type: string;
  line: number;
  odds: string;
  implied_prob: number;
}

export interface Player {
  player_name: string;
  player_id: string;
  position: string;
  team: string;
  projected_points?: number;
  actual_points?: number;
  injury_status?: string; 
  trending_count?: number;
  trending_pct?: number;
  image?: string;
}

export interface BroadcastCardData {
  id: string;
  name: string;
  position: string;
  team: string;
  opponent: string; // Added
  spread: number | null;
  overunder?: number | null;
  implied_total?: number | null;
  image: string;
  draft: string;
  injury_status: string;
  is_injury_boosted: boolean; // Added
  prop_line?: number | null;
  prop_prob?: number | null;
  stats: {
    projected: number;
    floor: number;
    average: number;
    over_under: string; 
    snap_count: number;
    snap_percentage: number;
  };
  props?: PropBet[]; 
  // Raw fields to assist with mapping
  pass_td_line?: number | null;
  pass_td_prob?: number | null;
  anytime_td_prob?: number | null;
  // New Props
  pass_att_line?: number | null;
  pass_att_prob?: number | null;
  rec_line?: number | null;
  rec_prob?: number | null;
  rush_att_line?: number | null;
  rush_att_prob?: number | null;
}

export interface HistoryEntry {
  season: number;
  week: number;
  opponent: string;
  points: number;
  passing_yds: number;
  passing_tds: number;
  pass_attempts: number;
  rushing_yds: number;
  receiving_yds: number;
  touchdowns: number;
  snap_count: number;
  snap_percentage: number;
  team_total_snaps?: number;
  receptions: number;
  targets: number;
  carries: number;
  interceptions?: number;
  fumbles_lost?: number;
}

export interface InjuryData {
  player_id: string;
  name: string;
  position: string;
  team: string;
  status: string;
  avg_snaps: number;
  avg_pct?: number;
  headshot?: string;
}

export interface GameWeather {
  is_dome: boolean;
  roof: string;
  temp_f: number | null;
  wind_mph: number | null;
  condition: string;
  precip_chance: number | null;
}

export interface TeamRankMetric {
  label: string;
  value: number;
  rank: number;
  rank_out_of: number;
  lower_is_better: boolean;
}

export interface TeamSideRankings {
  metrics: Record<string, TeamRankMetric>;
  average_rank: number;
  overall?: TeamRankMetric;
}

export interface TeamRankings {
  season: number;
  source: string;
  offense: TeamSideRankings | null;
  defense: TeamSideRankings | null;
}

export interface GameScript {
  tag: 'SHOOTOUT' | 'GRIND_IT_OUT' | 'BLOWOUT_RISK' | 'BALANCED';
  label: string;
  summary: string;
  home_implied_total: number | null;
  away_implied_total: number | null;
  home_strength_note: string | null;
  away_strength_note: string | null;
}

export interface MatchupData {
    matchup: string;
    week: number;
    gametime?: string;
    gameday?: string;
    home_roster: any[];
    away_roster: any[];
    home_injuries?: InjuryData[];
    away_injuries?: InjuryData[];
    over_under?: number | null;
    spread?: number | null;
    home_win_prob?: number | null;
    away_win_prob?: number | null;
    weather?: GameWeather | null;
    home_rankings?: TeamRankings | null;
    away_rankings?: TeamRankings | null;
    game_script?: GameScript | null;
}

export interface ScheduleGame {
  home_team: string;
  away_team: string;
  home_score?: number | null;
  away_score?: number | null;
  gameday?: string;
  gametime?: string;
  game_total?: number;
  moneyline_home?: string;
  moneyline_away?: string;
  [key: string]: unknown;
}

interface RawPlayerData {
    player_id?: string;
    player_name?: string;
    position?: string;
    team?: string;
    opponent?: string;
    image?: string;
    draft_position?: string;
    injury_status?: string;
    is_injury_boosted?: boolean;
    overunder?: number | null;
    implied_total?: number | null;
    spread?: number | null;
    props?: unknown[];
    prop_line?: number | null;
    prop_prob?: number | null;
    pass_td_line?: number | null;
    pass_td_prob?: number | null;
    anytime_td_prob?: number | null;
    // New Props
    pass_att_line?: number | null;
    pass_att_prob?: number | null;
    rec_line?: number | null;
    rec_prob?: number | null;
    rush_att_line?: number | null;
    rush_att_prob?: number | null;
    prediction?: number;
    floor_prediction?: number;
    average_points?: number;
    snap_count?: number;
    snap_percentage?: number;
    odds?: {
        prop_passing_yards?: number;
        prop_rushing_yards?: number;
        prop_receiving_yards?: number;
    };
}

const transformToCardData = (d: RawPlayerData): BroadcastCardData => {
    let oddsDisplay = "No Lines";
    if (d.odds) {
        const lines: string[] = [];
        if (d.odds.prop_passing_yards) lines.push(`${d.odds.prop_passing_yards} Pass`);
        if (d.odds.prop_rushing_yards) lines.push(`${d.odds.prop_rushing_yards} Rush`);
        if (d.odds.prop_receiving_yards) lines.push(`${d.odds.prop_receiving_yards} Rec`);
        if (lines.length > 0) oddsDisplay = lines.join(" | ");
    }

    // Prefer backend-selected prop line/prob, but fall back to the best matching prop in the payload
    let mainPropLine: number | null = d.prop_line ?? null;
    let mainPropProb: number | null = d.prop_prob ?? null;
    if (mainPropLine === null && Array.isArray(d.props) && d.props.length > 0) {
      const targets = d.position === 'QB'
        ? ['passing yards', 'pass yards', 'pass yds']
        : d.position === 'RB'
          ? ['rushing yards', 'rush yards', 'rush yds', 'rushing & receiving yards']
          : ['receiving yards', 'rec yards', 'rec yds'];
      const match = (d.props as PropBet[]).find(p => targets.some(t => p.prop_type?.toLowerCase().includes(t)));
      if (match) {
        mainPropLine = match.line ?? null;
        mainPropProb = match.implied_prob ?? null;
      }
    }

    return {
        id: d.player_id || "",
        name: d.player_name || "",
        position: d.position || "",
        team: d.team || "",
        opponent: d.opponent || "BYE", // Map opponent
      overunder: d.overunder ?? null,
      implied_total: d.implied_total ?? null,
        image: d.image || "",
        draft: d.draft_position || "",
        injury_status: d.injury_status || "Active",
        is_injury_boosted: d.is_injury_boosted || false, // Map boost flag
        spread: d.spread !== undefined ? d.spread : null,
      prop_line: mainPropLine,
      prop_prob: mainPropProb,
        props: (d.props as PropBet[]) || [], 
        // Pass through raw props for PlayerCard mapping
        pass_td_line: d.pass_td_line,
        pass_td_prob: d.pass_td_prob,
        anytime_td_prob: d.anytime_td_prob,
        // New Props
        pass_att_line: d.pass_att_line,
        pass_att_prob: d.pass_att_prob,
        rec_line: d.rec_line,
        rec_prob: d.rec_prob,
        rush_att_line: d.rush_att_line,
        rush_att_prob: d.rush_att_prob,
        stats: {
            projected: d.prediction || 0,
            floor: d.floor_prediction || 0,
            average: d.average_points || 0,
            over_under: oddsDisplay,
            snap_count: d.snap_count || 0,
            snap_percentage: d.snap_percentage || 0
        }
    };
};

export const useCurrentWeek = () => {
  const [week, setWeek] = useState<number | null>(null); 
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    axios.get(`${API_BASE_URL}/current_week`)
      .then(res => {
        setWeek(res.data.week);
        setLoading(false);
      })
      .catch(err => {
        console.error("Failed to fetch week:", err);
        setWeek(null); 
        setLoading(false);
      });
  }, []);

  return { currentWeek: week, loadingWeek: loading };
};

export const usePastRankings = (week: number) => {
  const [data, setData] = useState<Player[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!week || week < 1) {
        Promise.resolve().then(() => setLoading(false));
        return;
    }
    Promise.resolve().then(() => setLoading(true));
    axios.get(`${API_BASE_URL}/rankings/past/${week}`)
      .then(res => setData(res.data))
      .catch(err => console.error("Error fetching trending down:", err))
      .finally(() => Promise.resolve().then(() => setLoading(false)));
  }, [week]);

  return { pastRankings: data, loadingPast: loading };
};

export const useFutureRankings = (week: number) => {
  const [data, setData] = useState<Player[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!week || week < 1) {
        Promise.resolve().then(() => setLoading(false));
        return;
    }
    Promise.resolve().then(() => setLoading(true));
    axios.get(`${API_BASE_URL}/rankings/future/${week}`)
      .then(res => setData(res.data))
      .catch(err => console.error("Error fetching trending up:", err))
      .finally(() => Promise.resolve().then(() => setLoading(false)));
  }, [week]);

  return { futureRankings: data, loadingFuture: loading };
};

export const useSchedule = (week: number) => {
  const [games, setGames] = useState<ScheduleGame[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    // `active` guards every state update below so a StrictMode double-invoke
    // (mount -> cleanup -> mount) can't let the first, discarded run's stale
    // promises clobber state the second, real run already set — without this,
    // `games` bounces empty/populated/empty/populated a few times before
    // settling, which was forcing GameCard to unmount/remount on each bounce.
    let active = true;
    if (!week || week < 1) {
        Promise.resolve().then(() => { if (active) setGames([]); });
        return () => { active = false; };
    }

    Promise.resolve().then(() => { if (active) setLoading(true); });
    Promise.resolve().then(() => { if (active) setGames([]); }); // Clear old games immediately

    axios.get(`${API_BASE_URL}/schedule/${week}`)
      .then(res => { if (active) setGames(res.data); })
      .catch(err => {
          console.error("Error fetching schedule:", err);
          if (active) setGames([]); // Ensure empty array on error
      })
      .finally(() => { if (active) setLoading(false); });

    return () => { active = false; };
  }, [week]);

  return { games, loadingSchedule: loading };
};

export interface SeasonStats {
  season: number;
  games_played: number;
  season_total_pts: number;
  season_avg_pts: number;
  recent_avg_pts: number;
  boom_games: number;
  bust_games: number;
  total_yds: number;
  total_tds: number;
  total_receptions: number;
  total_targets: number;
  total_carries: number;
  def_tackles_total?: number;
  def_tackles_solo?: number;
  def_tackle_assists?: number;
  def_tackles_for_loss?: number;
  def_sacks?: number;
  def_qb_hits?: number;
  def_interceptions?: number;
  def_pass_defended?: number;
  def_fumbles_forced?: number;
  def_fumble_recoveries?: number;
  snaps_total: number;
  snap_pct_avg: number;
  team_sacks_taken?: number | null;
  team_sacks_taken_per_game?: number | null;
  team_sacks_taken_rank?: number | null;
  team_sacks_taken_rank_out_of?: number | null;
  team_rush_tds?: number | null;
  team_rush_tds_per_game?: number | null;
  team_rush_tds_rank?: number | null;
  team_rush_tds_rank_out_of?: number | null;
  team_sacks_taken_team?: string | null;
}

export interface PlayerSeasonStatsResponse {
  player_id: string;
  position_group?: 'qb' | 'rb' | 'wr' | 'te' | 'ol' | 'dl' | 'lb' | 'db';
  seasons: SeasonStats[];
}

export const usePlayerSeasonStats = (playerId: string | null) => {
  const [data, setData] = useState<PlayerSeasonStatsResponse | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!playerId) {
      setData(null);
      return;
    }
    let active = true;
    setLoading(true);
    axios
      .get(`${API_BASE_URL}/player/${playerId}/season_stats`)
      .then((res) => {
        if (active) setData(res.data);
      })
      .catch((err) => console.error('Season stats error:', err))
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [playerId]);

  return { seasonData: data, loadingSeasons: loading };
};

export const usePlayerHistory = (playerId: string | null) => {
    const [history, setHistory] = useState<HistoryEntry[]>([]);
    const [loading, setLoading] = useState(false);
  
    useEffect(() => {
      if (!playerId) return;
      Promise.resolve().then(() => setLoading(true));
      axios.get(`${API_BASE_URL}/player/history/${playerId}`)
        .then(res => setHistory(res.data))
        .catch(err => console.error("History error:", err))
        .finally(() => Promise.resolve().then(() => setLoading(false)));
    }, [playerId]);
  
    return { history, loadingHistory: loading };
  };

export const useBroadcastCard = (playerName: string | null, week?: number) => {
  const [cardData, setCardData] = useState<BroadcastCardData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!playerName) return;
    Promise.resolve().then(() => setLoading(true));
    
    axios.post(`${API_BASE_URL}/predict`, { player_name: playerName, week: week })
      .then(res => {
        setCardData(transformToCardData(res.data));
      })
      .catch(err => {
          console.error("Broadcast card error:", err);
          setCardData(null);
      })
      .finally(() => Promise.resolve().then(() => setLoading(false)));
  }, [playerName, week]);

  return { cardData, loadingCard: loading };
};

export const usePlayerProfileById = (playerId: string | null) => {
    const [cardData, setCardData] = useState<BroadcastCardData | null>(null);
    const [loading, setLoading] = useState(false);
  
    useEffect(() => {
      if (!playerId) return;
      Promise.resolve().then(() => setLoading(true));
      
      axios.get(`${API_BASE_URL}/player/${playerId}`)
        .then(res => {
          setCardData(transformToCardData(res.data));
        })
        .catch(err => {
            console.error("Profile by ID error:", err);
            setCardData(null);
        })
        .finally(() => Promise.resolve().then(() => setLoading(false)));
    }, [playerId]);
  
    return { cardData, loadingProfile: loading };
  };

export const useMatchupDeepDive = (week: number, home: string, away: string | null) => {
    const [matchupData, setMatchupData] = useState<MatchupData | null>(null);
    const [loading, setLoading] = useState(false);
  
    useEffect(() => {
      if (!home || !away) return;
      
      Promise.resolve().then(() => setLoading(true));
      axios.get(`${API_BASE_URL}/matchup/${week}/${home}/${away}`)
        .then(res => {
            const raw = res.data;
            const cleanData: MatchupData = {
                matchup: raw.matchup,
                week: raw.week,
                home_roster: Array.isArray(raw.home_roster) ? raw.home_roster.map(transformToCardData) : [],
                away_roster: Array.isArray(raw.away_roster) ? raw.away_roster.map(transformToCardData) : [],
                over_under: raw.over_under,
                spread: raw.spread,
                home_win_prob: raw.home_win_prob,
                away_win_prob: raw.away_win_prob
            };
            setMatchupData(cleanData);
        })
        .catch(err => console.error("Failed to fetch matchup deep dive:", err))
        .finally(() => Promise.resolve().then(() => setLoading(false)));
    }, [week, home, away]);
  
    return { matchupData, loadingMatchup: loading };
  };

// ---------------------------------------------------------- Tier List

export interface PoolPlayer {
  player_id: string;
  player_name: string;
  position: 'QB' | 'RB' | 'WR' | 'TE';
  team: string;
  image: string;
  injury_status?: string;
  is_rookie: boolean;
  draft_year?: number | null;
  draft_number?: number | null;
  adp?: number | null;
  age?: number | null;
  height?: number | null;
  weight?: number | null;
  season: number;
  stats: {
    games_played: number;
    season_total_pts: number;
    season_avg_pts: number;
    recent_avg_pts: number;
    boom_games: number;
    bust_games: number;
    total_yds: number;
    total_tds: number;
    total_receptions: number;
    total_targets: number;
    total_carries: number;
    snaps_total: number;
    snap_pct_avg: number;
  };
}

export const TIERS = ['UNRANKED', 'S', 'A', 'B', 'C', 'D', 'F'] as const;
export type Tier = typeof TIERS[number];

export const usePositionPool = (position: 'QB' | 'RB' | 'WR' | 'TE' | 'ALL' | null) => {
  const [pool, setPool] = useState<PoolPlayer[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!position) return;
    setLoading(true);
    setError(null);
    axios
      .get(`${API_BASE_URL}/tier_list/pool/${position}`)
      .then((res) => {
        setPool(res.data);
        setError(null);
      })
      .catch((err) => {
        console.error('Pool fetch error:', err);
        setPool([]);
        setError(
          err?.response?.status
            ? `Backend returned ${err.response.status}`
            : 'Backend is not reachable',
        );
      })
      .finally(() => setLoading(false));
  }, [position]);

  return { pool, loadingPool: loading, poolError: error };
};

export interface TierAssignment {
  player_id: string;
  tier: Tier;
}

export interface SavedTierList {
  name: string;
  position: string;
  season: number;
  assignments: TierAssignment[];
  updated_at?: string;
}

export const fetchTierList = async (
  position: string,
  name: string,
): Promise<SavedTierList | null> => {
  try {
    const res = await axios.get(`${API_BASE_URL}/tier_list/${position}/${encodeURIComponent(name)}`);
    return res.data;
  } catch {
    return null;
  }
};

export const saveTierList = async (payload: SavedTierList): Promise<SavedTierList | null> => {
  try {
    const res = await axios.post(`${API_BASE_URL}/tier_list`, payload);
    return res.data;
  } catch (e) {
    console.error('saveTierList error:', e);
    return null;
  }
};

export const refreshRookies = async (): Promise<{ status: string; message?: string }> => {
  try {
    const res = await axios.post(`${API_BASE_URL}/refresh/rookies`);
    return res.data;
  } catch (e) {
    console.error('refreshRookies error:', e);
    return { status: 'error' };
  }
};

export const usePlayerSearch = (query: string) => {
    const [results, setResults] = useState<Player[]>([]);
    
    useEffect(() => {
        if (query.length < 2) { Promise.resolve().then(() => setResults([])); return; }
        const delayDebounce = setTimeout(() => {
            axios.get(`${API_BASE_URL}/players/search?q=${query}`)
                .then(res => setResults(res.data))
                .catch(err => console.error(err));
        }, 300); 
        return () => clearTimeout(delayDebounce);
    }, [query]);

    return results;
};
