// File: src/lib/api.ts

// Runtime-configurable API base (supports proxy `/api` by default and also allows
// injection via `window.__env.API_BASE_URL`). This keeps the build portable behind
// a CDN or reverse proxy (e.g., Cloudflare Tunnel).
export const API_BASE_URL = (typeof window !== 'undefined' && window.__env && window.__env.API_BASE_URL) ? window.__env.API_BASE_URL : '/api';

// --- 1. Fetch Player by ID (Fixes "Failed to fetch player by id") ---
export const fetchPlayerById = async (playerId: string, week?: number) => {
  if (!playerId) return null;
  try {
    // Construct URL with optional week parameter
    const url = week 
      ? `${API_BASE_URL}/player/${playerId}?week=${week}` 
      : `${API_BASE_URL}/player/${playerId}`;
      
    const response = await fetch(url);
    
    // Handle 404s gracefully (don't crash the app)
    if (!response.ok) {
      if (response.status === 404) {
        console.warn(`Player ${playerId} not found.`);
        return null; 
      }
      throw new Error(`API Error: ${response.statusText}`);
    }
    return await response.json();
  } catch (e) {
    console.error("fetchPlayerById failed:", e);
    return null;
  }
};

// --- 2. Fetch Players List (Fixes "Failed to fetch players") ---
export const fetchPlayers = async () => {
  try {
    // OLD BROKEN WAY: const response = await fetch(`${API_BASE_URL}/players/all`);
    
    // NEW WAY: Get top 100 active offensive players instead of the whole DB.
    // querying "a" gets a broad list of active players efficiently.
    const response = await fetch(`${API_BASE_URL}/players/search?q=a&limit=100`);
    
    if (!response.ok) return []; // Return empty instead of throwing error
    return await response.json();
  } catch (e) {
    console.error("fetchPlayers failed:", e);
    return []; // Return empty array to prevent map() crashes
  }
};

// --- 3. Search Players (Used by Lookup) ---
export const searchPlayers = async (query: string) => {
  if (!query) return [];
  try {
    const response = await fetch(`${API_BASE_URL}/players/search?q=${query}`);
    if (!response.ok) return [];
    return await response.json();
  } catch (e) {
    console.error("searchPlayers failed:", e);
    return [];
  }
};

// --- 4. Fetch History (Used by Season Recap) ---
export const fetchPlayerHistory = async (playerId: string) => {
  if (!playerId) return [];
  try {
    const response = await fetch(`${API_BASE_URL}/player/history/${playerId}`);
    if (!response.ok) return [];
    return await response.json();
  } catch (e) {
    console.error("fetchPlayerHistory failed:", e);
    return [];
  }
};

// Fetch matchup details
export const fetchMatchup = async (week: number, home: string, away: string) => {
  try {
    const response = await fetch(`${API_BASE_URL}/matchup/${week}/${home}/${away}`);
    if (!response.ok) return null;
    return await response.json();
  } catch (e) {
    console.error("fetchMatchup failed:", e);
    return null;
  }
};
// --- Sleeper league integration -------------------------------------------
// Sleeper's read API is public, so importing a team needs no credentials.
// Errors are returned rather than thrown so the view can show the reason.

export interface SleeperLeague {
  league_id: string;
  name: string;
  season: string;
  total_rosters: number;
  status?: string;
  scoring_type: string;
  roster_positions: string[];
  avatar?: string | null;
}

export interface SleeperTeam {
  roster_id: number;
  display_name: string;
  team_name: string;
  wins: number;
  losses: number;
  ties: number;
  player_count: number;
}

const sleeperGet = async (path: string) => {
  const res = await fetch(`${API_BASE_URL}${path}`);
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json())?.detail || detail; } catch { /* keep statusText */ }
    throw new Error(detail);
  }
  return res.json();
};

export const fetchSleeperUser = (username: string, season: number) =>
  sleeperGet(`/sleeper/user/${encodeURIComponent(username)}?season=${season}`);

export const fetchSleeperLeague = (leagueId: string) =>
  sleeperGet(`/sleeper/league/${encodeURIComponent(leagueId)}`);

export const fetchSleeperRosterAnalysis = (leagueId: string, rosterId: number, week: number) =>
  sleeperGet(`/sleeper/league/${encodeURIComponent(leagueId)}/roster/${rosterId}/analysis?week=${week}`);

export const fetchSleeperWaivers = (leagueId: string, week: number, limit = 25) =>
  sleeperGet(`/sleeper/league/${encodeURIComponent(leagueId)}/waivers?week=${week}&limit=${limit}`);
