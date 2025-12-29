import { useState, useEffect } from 'react';
import axios from 'axios';

const API_BASE_URL = 'http://127.0.0.1:8000'; 

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
  image?: string;
}

export interface BroadcastCardData {
  id: string;
  name: string;
  position: string;
  team: string;
  spread: number | null;
  image: string;
  draft: string;
  injury_status: string; 
  stats: {
    projected: number;
    floor: number;
    average: number;
    over_under: string; 
    snap_count: number;
    snap_percentage: number;
  };
  props?: PropBet[]; 
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

export interface MatchupData {
    matchup: string;
    week: number;
    home_roster: BroadcastCardData[];
    away_roster: BroadcastCardData[];
}

const transformToCardData = (d: any): BroadcastCardData => {
    let oddsDisplay = "No Lines";
    if (d.odds) {
        const lines = [];
        if (d.odds.prop_passing_yards) lines.push(`${d.odds.prop_passing_yards} Pass`);
        if (d.odds.prop_rushing_yards) lines.push(`${d.odds.prop_rushing_yards} Rush`);
        if (d.odds.prop_receiving_yards) lines.push(`${d.odds.prop_receiving_yards} Rec`);
        if (lines.length > 0) oddsDisplay = lines.join(" | ");
    }

    return {
        id: d.player_id,
        name: d.player_name,
        position: d.position,
        team: d.team,
        image: d.image,
        draft: d.draft_position,
        injury_status: d.injury_status || "Active", 
        spread: d.spread !== undefined ? d.spread : null,
        props: d.props || [], 
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
        console.error("Failed to fetch week, defaulting to 1:", err);
        setWeek(1); 
        setLoading(false);
      });
  }, []);

  return { currentWeek: week || 1, loadingWeek: loading || week === null };
};

export const usePastRankings = (week: number) => {
  const [data, setData] = useState<Player[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (week < 1) return;
    setLoading(true);
    axios.get(`${API_BASE_URL}/rankings/past/${week}`)
      .then(res => setData(res.data))
      .catch(err => console.error("Error fetching trending down:", err))
      .finally(() => setLoading(false));
  }, [week]);

  return { pastRankings: data, loadingPast: loading };
};

export const useFutureRankings = (week: number) => {
  const [data, setData] = useState<Player[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (week < 1) return;
    setLoading(true);
    axios.get(`${API_BASE_URL}/rankings/future/${week}`)
      .then(res => setData(res.data))
      .catch(err => console.error("Error fetching trending up:", err))
      .finally(() => setLoading(false));
  }, [week]);

  return { futureRankings: data, loadingFuture: loading };
};

export const useSchedule = (week: number) => {
  const [games, setGames] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!week || week < 1) return; 
    setLoading(true);
    axios.get(`${API_BASE_URL}/schedule/${week}`)
      .then(res => setGames(res.data))
      .catch(err => console.error("Error fetching schedule:", err))
      .finally(() => setLoading(false));
  }, [week]); 

  return { games, loadingSchedule: loading };
};

export const usePlayerHistory = (playerId: string | null) => {
    const [history, setHistory] = useState<HistoryEntry[]>([]);
    const [loading, setLoading] = useState(false);
  
    useEffect(() => {
      if (!playerId) return;
      setLoading(true);
      axios.get(`${API_BASE_URL}/player/history/${playerId}`)
        .then(res => setHistory(res.data))
        .catch(err => console.error("History error:", err))
        .finally(() => setLoading(false));
    }, [playerId]);
  
    return { history, loadingHistory: loading };
  };

export const useBroadcastCard = (playerName: string | null, week?: number) => {
  const [cardData, setCardData] = useState<BroadcastCardData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!playerName) return;
    setLoading(true);
    
    axios.post(`${API_BASE_URL}/predict`, { player_name: playerName, week: week })
      .then(res => {
        setCardData(transformToCardData(res.data));
      })
      .catch(err => {
          console.error("Broadcast card error:", err);
          setCardData(null);
      })
      .finally(() => setLoading(false));
  }, [playerName, week]);

  return { cardData, loadingCard: loading };
};

export const usePlayerProfileById = (playerId: string | null) => {
    const [cardData, setCardData] = useState<BroadcastCardData | null>(null);
    const [loading, setLoading] = useState(false);
  
    useEffect(() => {
      if (!playerId) return;
      setLoading(true);
      
      axios.get(`${API_BASE_URL}/player/${playerId}`)
        .then(res => {
          setCardData(transformToCardData(res.data));
        })
        .catch(err => {
            console.error("Profile by ID error:", err);
            setCardData(null);
        })
        .finally(() => setLoading(false));
    }, [playerId]);
  
    return { cardData, loadingProfile: loading };
  };

export const useMatchupDeepDive = (week: number, home: string, away: string | null) => {
    const [matchupData, setMatchupData] = useState<MatchupData | null>(null);
    const [loading, setLoading] = useState(false);
  
    useEffect(() => {
      if (!home || !away) return;
      
      setLoading(true);
      axios.get(`${API_BASE_URL}/matchup/${week}/${home}/${away}`)
        .then(res => {
            const raw = res.data;
            const cleanData: MatchupData = {
                matchup: raw.matchup,
                week: raw.week,
                home_roster: Array.isArray(raw.home_roster) ? raw.home_roster.map(transformToCardData) : [],
                away_roster: Array.isArray(raw.away_roster) ? raw.away_roster.map(transformToCardData) : []
            };
            setMatchupData(cleanData);
        })
        .catch(err => console.error("Failed to fetch matchup deep dive:", err))
        .finally(() => setLoading(false));
    }, [week, home, away]);
  
    return { matchupData, loadingMatchup: loading };
  };

export const usePlayerSearch = (query: string) => {
    const [results, setResults] = useState<Player[]>([]);
    
    useEffect(() => {
        if (query.length < 2) { setResults([]); return; }
        const delayDebounce = setTimeout(() => {
            axios.get(`${API_BASE_URL}/players/search?q=${query}`)
                .then(res => setResults(res.data))
                .catch(err => console.error(err));
        }, 300); 
        return () => clearTimeout(delayDebounce);
    }, [query]);

    return results;
};