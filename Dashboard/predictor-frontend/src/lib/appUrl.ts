/**
 * The URL is the contract between the backend's screen-action tools and the
 * browser. Every view the app can show maps to a path here, and every path the
 * backend builds (see backend/agent/screen_actions.py) must parse back to the
 * same location. No react-router: these are pure functions, and App.tsx wires
 * them to history.pushState/popstate.
 */

// Must match the backend's open_* tools exactly.
// Backend counterpart: backend/agent/screen_actions.py

export type AppLocation =
  | { view: 'SCHEDULE' }
  | { view: 'GAME'; home: string; away: string }
  | { view: 'LOOKUP' }
  | { view: 'COMPARE'; ids: string[] }
  | { view: 'HISTORY'; playerId: string }
  | { view: 'TRENDING' }
  | { view: 'PICKS' }
  | { view: 'PLAYOFFS' }
  | { view: 'TIERS' }
  | { view: 'TEAMS' }
  | { view: 'GAME_RANKS' }
  | { view: 'TEAM_PAGE'; team: string; tab: 'overview' | 'builder' }
  | { view: 'MY_TEAM'; tab: 'LINEUP' | 'WAIVERS' | 'LEAGUE' };

const TEAM_RE = /^[A-Z]{2,3}$/;
const PLAYER_ID_RE = /^[A-Za-z0-9_.-]{1,40}$/;
const MAX_COMPARE_IDS = 4;

// Segment-name matching tolerates leading/trailing slashes and case, but the
// dynamic values (team abbrevs, player ids) are validated strictly after
// uppercasing/extracting.
function cleanSegments(path: string): string[] {
  return path.split('/').map((s) => s.trim()).filter(Boolean);
}

function enc(seg: string): string {
  return encodeURIComponent(seg);
}

export function formatAppUrl(loc: AppLocation): string {
  switch (loc.view) {
    case 'SCHEDULE':
      return '/';
    case 'GAME':
      return `/game/${enc(loc.away)}/${enc(loc.home)}`;
    case 'LOOKUP':
      return '/lookup';
    case 'COMPARE':
      return loc.ids.length ? `/compare?ids=${loc.ids.map(enc).join(',')}` : '/compare';
    case 'HISTORY':
      return `/player/${enc(loc.playerId)}`;
    case 'TRENDING':
      return '/trending';
    case 'PICKS':
      return '/picks';
    case 'PLAYOFFS':
      return '/playoffs';
    case 'TIERS':
      return '/tiers';
    case 'TEAMS':
      return '/teams';
    case 'GAME_RANKS':
      return '/ranks';
    case 'TEAM_PAGE':
      return loc.tab === 'builder'
        ? `/team/${enc(loc.team)}?tab=builder`
        : `/team/${enc(loc.team)}`;
    case 'MY_TEAM':
      if (loc.tab === 'WAIVERS') return '/my-team/waivers';
      if (loc.tab === 'LEAGUE') return '/my-team/league';
      return '/my-team';
  }
}

export function parseAppUrl(pathnameAndSearch: string): AppLocation | null {
  let pathname = pathnameAndSearch;
  let search = '';
  const qIdx = pathnameAndSearch.indexOf('?');
  if (qIdx !== -1) {
    pathname = pathnameAndSearch.slice(0, qIdx);
    search = pathnameAndSearch.slice(qIdx + 1);
  }

  const segs = cleanSegments(pathname);
  if (segs.length === 0) return { view: 'SCHEDULE' };

  const first = segs[0].toLowerCase();

  // --- static single-segment views ---
  const staticViews: Record<string, AppLocation> = {
    lookup: { view: 'LOOKUP' },
    trending: { view: 'TRENDING' },
    picks: { view: 'PICKS' },
    playoffs: { view: 'PLAYOFFS' },
    tiers: { view: 'TIERS' },
    teams: { view: 'TEAMS' },
    ranks: { view: 'GAME_RANKS' },
  };
  if (first in staticViews && segs.length === 1) return staticViews[first];

  // --- compare ---
  if (first === 'compare' && segs.length === 1) {
    if (!search) return { view: 'COMPARE', ids: [] };
    const params = new URLSearchParams(search);
    const raw = params.get('ids');
    if (!raw) return { view: 'COMPARE', ids: [] };
    const ids = raw
      .split(',')
      .map((s) => s.trim())
      .filter((s) => PLAYER_ID_RE.test(s))
      .slice(0, MAX_COMPARE_IDS);
    return { view: 'COMPARE', ids };
  }

  // --- game: /game/{away}/{home} ---
  if (first === 'game' && segs.length === 3) {
    const away = segs[1].toUpperCase();
    const home = segs[2].toUpperCase();
    if (!TEAM_RE.test(away) || !TEAM_RE.test(home)) return null;
    return { view: 'GAME', away, home };
  }

  // --- player history: /player/{playerId} ---
  if (first === 'player' && segs.length === 2) {
    const pid = segs[1];
    if (!PLAYER_ID_RE.test(pid)) return null;
    return { view: 'HISTORY', playerId: pid };
  }

  // --- team page: /team/{TEAM} or /team/{TEAM}?tab=builder ---
  if (first === 'team' && segs.length === 2) {
    const team = segs[1].toUpperCase();
    if (!TEAM_RE.test(team)) return null;
    let tab: 'overview' | 'builder' = 'overview';
    if (search) {
      const params = new URLSearchParams(search);
      const t = params.get('tab');
      if (t === 'builder') tab = 'builder';
      else if (t && t.toLowerCase() !== 'overview') return null;
    }
    return { view: 'TEAM_PAGE', team, tab };
  }

  // --- my team: /my-team, /my-team/waivers, /my-team/league ---
  if (first === 'my-team' || first === 'my_team') {
    if (segs.length === 1) return { view: 'MY_TEAM', tab: 'LINEUP' };
    if (segs.length === 2) {
      const sub = segs[1].toLowerCase();
      if (sub === 'waivers') return { view: 'MY_TEAM', tab: 'WAIVERS' };
      if (sub === 'league') return { view: 'MY_TEAM', tab: 'LEAGUE' };
    }
    return null;
  }

  return null;
}