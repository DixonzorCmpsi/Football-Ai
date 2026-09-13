import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Loader2, Search, Users, TrendingUp, AlertTriangle, CheckCircle2, ArrowLeftRight, ChevronRight, Pin, PinOff, Scale, Check, X } from 'lucide-react';
import PlayerCompareModal from './PlayerCompareModal';
import LeagueInsights from './LeagueInsights';
import type { LeagueInsightsData } from './LeagueInsights';
import {
  fetchSleeperUser,
  fetchSleeperLeague,
  fetchSleeperRosterAnalysis,
  fetchSleeperWaivers,
  fetchSleeperLeagueInsights,
  type SleeperLeague,
  type SleeperTeam,
} from '../lib/api';
import { sizedPlayerImage } from '../utils/playerImage';

interface SleeperViewProps {
  week: number;
  season: number;
  onOpenHistory?: (playerId: string) => void;
  /** Lets the host header's Back button walk back one step inside this view. */
  onInnerNav?: (entry: { label: string; back: () => void } | null) => void;
}

type Stage = 'USER' | 'LEAGUE' | 'TEAM';
type Tab = 'LINEUP' | 'WAIVERS' | 'LEAGUE';

const num = (v: unknown) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

// Remembering the handle in the browser is enough: there is no account system,
// and re-typing a username to get back to a roster you were just looking at is
// the whole complaint. Kept for a week, cleared by "Start over".
const STORE_KEY = 'spotai.sleeper.session.v1';
const STORE_TTL_MS = 7 * 24 * 60 * 60 * 1000;

type PinnedLeague = {
  league_id: string;
  name: string;
  season: string;
  scoring_type: string;
  total_rosters: number;
  rosterId?: number | null;
};

type Saved = {
  username: string;
  season: number;
  leagueId?: string | null;
  rosterId?: number | null;
  pinned?: PinnedLeague[];
  savedAt: number;
};

const loadSaved = (): Saved | null => {
  try {
    const raw = localStorage.getItem(STORE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Saved;
    if (!parsed?.username) return null;
    if (Date.now() - (parsed.savedAt || 0) > STORE_TTL_MS) {
      localStorage.removeItem(STORE_KEY);
      return null;
    }
    return parsed;
  } catch {
    return null; // private mode, blocked storage, or corrupt value
  }
};

const saveSession = (patch: Partial<Saved>) => {
  try {
    const current = loadSaved() || ({} as Saved);
    const next = { ...current, ...patch, savedAt: Date.now() };
    if (!next.username) return;
    localStorage.setItem(STORE_KEY, JSON.stringify(next));
  } catch {
    /* storage unavailable: the view still works, it just won't remember */
  }
};

const clearSession = () => {
  try { localStorage.removeItem(STORE_KEY); } catch { /* noop */ }
};

const PlayerRow: React.FC<{
  player: any;
  badge?: React.ReactNode;
  onOpenHistory?: (id: string) => void;
  /** Present for players we project: toggles them into the comparison. */
  compare?: { selected: boolean; disabled: boolean; toggle: () => void };
  /** Where the number comes from, when it isn't our model. */
  sourceLabel?: string;
}> = ({ player, badge, onOpenHistory, compare, sourceLabel }) => (
  <div className={`flex items-stretch rounded-lg bg-white dark:bg-slate-900 border transition-colors ${
    compare?.selected ? 'border-blue-500 ring-1 ring-blue-500/30' : 'border-slate-200 dark:border-slate-700 hover:border-blue-400 dark:hover:border-blue-500'
  }`}>
  <button
    type="button"
    onClick={() => onOpenHistory && player.player_id && onOpenHistory(player.player_id)}
    data-testid="sleeper-player-row"
    data-position={player.position}
    className="flex-1 min-w-0 text-left flex items-center gap-3 p-2.5"
  >
    <div className="w-9 h-9 rounded-full overflow-hidden bg-slate-100 dark:bg-slate-800 shrink-0 flex items-center justify-center">
      {player.image ? (
        <img
          src={sizedPlayerImage(player.image, 36)}
          alt={player.player_name}
          width={36}
          height={36}
          loading="lazy"
          decoding="async"
          className="w-full h-full object-cover"
        />
      ) : (
        <span className="text-[10px] font-bold text-slate-400">{player.position}</span>
      )}
    </div>
    <div className="min-w-0 flex-1">
      <div className="font-bold text-sm text-slate-800 dark:text-slate-100 truncate">
        {player.player_name}
      </div>
      <div className="text-[11px] text-slate-500 flex items-center gap-1.5">
        <span className="font-mono font-bold bg-slate-100 dark:bg-slate-800 px-1 rounded">
          {player.position}
        </span>
        <span>{player.team}</span>
        {player.opponent ? <span className="text-slate-400">vs {player.opponent}</span> : null}
        {player.injury_status && player.injury_status !== 'Active' ? (
          <span className="text-red-500 font-bold uppercase">{player.injury_status}</span>
        ) : null}
      </div>
    </div>
    {badge}
    <div className="text-right shrink-0">
      <div className="text-[10px] uppercase tracking-wider text-slate-400 font-bold">{sourceLabel ?? 'Proj'}</div>
      <div className="font-black text-slate-800 dark:text-slate-100 tabular-nums">
        {player.prediction === null || player.prediction === undefined ? '–' : num(player.prediction).toFixed(1)}
      </div>
    </div>
  </button>
  {compare && (
    <button
      type="button"
      onClick={compare.toggle}
      disabled={compare.disabled && !compare.selected}
      data-testid="sleeper-compare-toggle"
      data-selected={compare.selected}
      title={compare.selected ? 'Remove from comparison' : compare.disabled ? 'Compare up to 4 players' : 'Add to comparison'}
      aria-label={compare.selected ? `Remove ${player.player_name} from comparison` : `Compare ${player.player_name}`}
      className={`shrink-0 w-10 flex items-center justify-center border-l rounded-r-lg transition-colors disabled:opacity-30 ${
        compare.selected
          ? 'border-blue-500 bg-blue-600 text-white'
          : 'border-slate-200 dark:border-slate-700 text-slate-400 hover:text-blue-600 hover:bg-slate-50 dark:hover:bg-slate-800'
      }`}
    >
      {compare.selected ? <Check size={14} strokeWidth={3} /> : <Scale size={14} />}
    </button>
  )}
  </div>
);

const SleeperView: React.FC<SleeperViewProps> = ({ week, season, onOpenHistory, onInnerNav }) => {
  const [stage, setStage] = useState<Stage>('USER');
  const [username, setUsername] = useState('');
  const [seasonInput, setSeasonInput] = useState(season);
  const [loading, setLoading] = useState(false);
  const [restoring, setRestoring] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [user, setUser] = useState<any>(null);
  const [leagues, setLeagues] = useState<SleeperLeague[]>([]);
  const [league, setLeague] = useState<SleeperLeague | null>(null);
  const [teams, setTeams] = useState<SleeperTeam[]>([]);
  const [analysis, setAnalysis] = useState<any>(null);
  const [waivers, setWaivers] = useState<any>(null);
  const [leagueData, setLeagueData] = useState<LeagueInsightsData | null>(null);
  const [tab, setTab] = useState<Tab>('LINEUP');
  // Leagues the user pinned, kept in the browser so they are one click away on
  // every visit without re-entering a handle.
  const [pinned, setPinned] = useState<PinnedLeague[]>(() => loadSaved()?.pinned || []);

  const isPinned = useCallback(
    (id: string) => pinned.some((p) => p.league_id === id),
    [pinned],
  );

  const togglePin = useCallback((lg: SleeperLeague | PinnedLeague) => {
    setPinned((prev) => {
      const exists = prev.some((p) => p.league_id === lg.league_id);
      const next = exists
        ? prev.filter((p) => p.league_id !== lg.league_id)
        : [...prev, {
            league_id: lg.league_id,
            name: lg.name,
            season: String(lg.season),
            scoring_type: lg.scoring_type,
            total_rosters: lg.total_rosters,
          }];
      saveSession({ pinned: next });
      return next;
    });
  }, []);

  const weekRef = useRef(week);
  weekRef.current = week;

  const run = useCallback(async (fn: () => Promise<void>) => {
    setLoading(true);
    setError(null);
    try {
      await fn();
    } catch (e: any) {
      setError(e?.message || 'Something went wrong');
    } finally {
      setLoading(false);
    }
  }, []);

  // ---- restore a previous session ----------------------------------------
  const restoredOnce = useRef(false);
  useEffect(() => {
    if (restoredOnce.current) return;
    restoredOnce.current = true;
    const saved = loadSaved();
    if (!saved?.username) return;

    setUsername(saved.username);
    setSeasonInput(saved.season ?? season);
    setRestoring(true);
    (async () => {
      try {
        const data = await fetchSleeperUser(saved.username, saved.season ?? season);
        setUser(data.user);
        setLeagues(data.leagues || []);
        setStage('LEAGUE');
        if (saved.leagueId) {
          const lgData = await fetchSleeperLeague(saved.leagueId);
          setLeague(lgData.league);
          setTeams(lgData.teams || []);
          setStage('TEAM');
          if (saved.rosterId !== null && saved.rosterId !== undefined) {
            setAnalysis(await fetchSleeperRosterAnalysis(
              saved.leagueId, saved.rosterId, weekRef.current));
          }
        }
      } catch {
        // A stale league or renamed account just drops us back to the form.
        clearSession();
      } finally {
        setRestoring(false);
      }
    })();
  }, [season]);

  // ---- actions ------------------------------------------------------------
  const lookupUser = useCallback(() => {
    if (!username.trim()) return;
    run(async () => {
      const data = await fetchSleeperUser(username.trim(), seasonInput);
      setUser(data.user);
      setLeagues(data.leagues || []);
      setLeague(null); setTeams([]); setAnalysis(null); setWaivers(null);
      setStage('LEAGUE');
      saveSession({ username: username.trim(), season: seasonInput, leagueId: null, rosterId: null });
    });
  }, [username, seasonInput, run]);

  const chooseLeague = useCallback((lg: SleeperLeague) => {
    run(async () => {
      const data = await fetchSleeperLeague(lg.league_id);
      setLeague(data.league);
      setTeams(data.teams || []);
      setAnalysis(null); setWaivers(null);
      setStage('TEAM');
      saveSession({ leagueId: lg.league_id, rosterId: null });
    });
  }, [run]);

  const chooseTeam = useCallback((rosterId: number) => {
    if (!league) return;
    run(async () => {
      setAnalysis(await fetchSleeperRosterAnalysis(league.league_id, rosterId, week));
      setWaivers(null);
      setLeagueData(null);
      setTab('LINEUP');
      saveSession({ rosterId });
      setPinned((prev) => {
        const next = prev.map((p) =>
          p.league_id === league.league_id ? { ...p, rosterId } : p);
        saveSession({ pinned: next });
        return next;
      });
    });
  }, [league, week, run]);

  const loadWaivers = useCallback(() => {
    if (!league) return;
    setTab('WAIVERS');
    if (waivers) return;
    run(async () => {
      setWaivers(await fetchSleeperWaivers(league.league_id, week, 25));
    });
  }, [league, week, waivers, run]);

  const loadLeague = useCallback(() => {
    if (!league) return;
    setTab('LEAGUE');
    if (leagueData && leagueData.league.week === week && leagueData.league.league_id === league.league_id) return;
    run(async () => {
      setLeagueData(await fetchSleeperLeagueInsights(league.league_id, week, analysis?.roster_id ?? null));
    });
  }, [league, week, analysis, leagueData, run]);

  const openPinned = useCallback((pin: PinnedLeague) => {
    run(async () => {
      const data = await fetchSleeperLeague(pin.league_id);
      setLeague(data.league);
      setTeams(data.teams || []);
      setStage('TEAM');
      saveSession({ leagueId: pin.league_id });
      if (pin.rosterId !== null && pin.rosterId !== undefined) {
        setAnalysis(await fetchSleeperRosterAnalysis(pin.league_id, pin.rosterId, week));
        setWaivers(null);
        setTab('LINEUP');
        saveSession({ rosterId: pin.rosterId });
      }
    });
  }, [run, week]);

  const reset = useCallback(() => {
    clearSession();
    setStage('USER'); setUser(null); setLeagues([]); setLeague(null);
    setTeams([]); setAnalysis(null); setWaivers(null); setError(null);
    setUsername('');
    setPinned([]);
  }, []);

  // ---- one granular step back --------------------------------------------
  // Each of these is a place the user can actually be, so Back should land on
  // the previous one rather than dumping them at the username form (or, worse,
  // out of the view entirely) and making them type their handle again.
  const backStep = useMemo(() => {
    if (analysis && (tab === 'WAIVERS' || tab === 'LEAGUE')) {
      return { label: 'lineup', back: () => setTab('LINEUP') };
    }
    if (analysis) {
      return { label: 'teams', back: () => { setAnalysis(null); setWaivers(null); saveSession({ rosterId: null }); } };
    }
    if (stage === 'TEAM') {
      return { label: 'leagues', back: () => { setLeague(null); setTeams([]); setStage('LEAGUE'); saveSession({ leagueId: null, rosterId: null }); } };
    }
    if (stage === 'LEAGUE') {
      return { label: 'search', back: () => { setUser(null); setLeagues([]); setStage('USER'); } };
    }
    return null;
  }, [analysis, tab, stage]);

  useEffect(() => {
    if (!onInnerNav) return;
    onInnerNav(backStep);
    return () => onInnerNav(null);
  }, [backStep, onInnerNav]);

  const crumbs = useMemo(() => {
    const out: { label: string; onClick?: () => void }[] = [{ label: 'Search', onClick: stage !== 'USER' ? () => { setUser(null); setLeagues([]); setStage('USER'); } : undefined }];
    if (user) out.push({ label: user.display_name || user.username, onClick: stage !== 'LEAGUE' ? () => { setLeague(null); setTeams([]); setAnalysis(null); setStage('LEAGUE'); } : undefined });
    if (league) out.push({ label: league.name, onClick: analysis ? () => { setAnalysis(null); setWaivers(null); } : undefined });
    if (analysis) {
      const t = teams.find((x) => x.roster_id === analysis.roster_id);
      out.push({ label: t?.team_name || `Roster ${analysis.roster_id}`, onClick: tab !== 'LINEUP' ? () => setTab('LINEUP') : undefined });
    }
    if (analysis && tab === 'WAIVERS') out.push({ label: 'Waiver wire' });
    if (analysis && tab === 'LEAGUE') out.push({ label: 'League' });
    return out;
  }, [stage, user, league, analysis, teams, tab]);

  const recommendedIds = useMemo(
    () => new Set((analysis?.recommended_starters || []).map((c: any) => c.sleeper_id)),
    [analysis],
  );
  const bench = useMemo(
    () => (analysis?.players || []).filter((p: any) => !recommendedIds.has(p.sleeper_id)),
    [analysis, recommendedIds],
  );

  // Players picked for the comparison popup: anyone we project, roster or waiver wire.
  const [compareIds, setCompareIds] = useState<string[]>([]);
  const [compareOpen, setCompareOpen] = useState(false);
  const compareFor = (player: any) => player.player_id ? {
    selected: compareIds.includes(player.player_id),
    disabled: compareIds.length >= 4,
    toggle: () => setCompareIds((prev) => prev.includes(player.player_id)
      ? prev.filter((id) => id !== player.player_id)
      : [...prev, player.player_id].slice(0, 4)),
  } : undefined;
  const compareNames = useMemo(() => {
    const all = [...(analysis?.players || []), ...(waivers?.players || [])];
    return compareIds.map((id) => all.find((p: any) => p.player_id === id)?.player_name || 'Player');
  }, [compareIds, analysis, waivers]);

  return (
    <div className="space-y-5" data-testid="sleeper-view">
      <div>
        <div className="text-[11px] font-bold tracking-[0.2em] text-slate-400 uppercase">
          My Team · Sleeper
        </div>
        <h1 className="text-2xl font-black text-slate-800 dark:text-slate-100">
          {analysis ? 'Your roster, projected' : 'Import your fantasy roster'}
        </h1>
      </div>

      {/* Breadcrumb — every level is clickable, so no step is a dead end. */}
      <nav data-testid="sleeper-breadcrumb" className="flex items-center flex-wrap gap-1 text-xs">
        {crumbs.map((c, i) => (
          <span key={`${c.label}-${i}`} className="flex items-center gap-1">
            {i > 0 && <ChevronRight size={12} className="text-slate-400" />}
            {c.onClick ? (
              <button
                onClick={c.onClick}
                data-testid="sleeper-crumb"
                className="font-bold text-blue-600 dark:text-blue-400 hover:underline"
              >
                {c.label}
              </button>
            ) : (
              <span data-testid="sleeper-crumb" className="font-bold text-slate-500 dark:text-slate-400">{c.label}</span>
            )}
          </span>
        ))}
      </nav>

      {/* Pinned leagues: always visible, one click to the roster, no handle needed. */}
      {pinned.length > 0 && (
        <div data-testid="sleeper-pinned">
          <h2 className="text-[11px] font-black uppercase tracking-wider text-slate-400 mb-2 flex items-center gap-1.5">
            <Pin size={12} /> Pinned leagues
          </h2>
          <div className="flex flex-wrap gap-2">
            {pinned.map((pin) => (
              <div
                key={pin.league_id}
                data-testid="sleeper-pin"
                className={`flex items-center gap-2 pl-3 pr-1.5 py-1.5 rounded-full border text-xs transition-colors ${
                  league?.league_id === pin.league_id
                    ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                    : 'border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900'
                }`}
              >
                <button
                  onClick={() => openPinned(pin)}
                  data-testid="sleeper-pin-open"
                  className="font-bold text-slate-700 dark:text-slate-200 hover:text-blue-600 dark:hover:text-blue-400"
                >
                  {pin.name}
                  <span className="ml-1.5 font-normal text-slate-400">
                    {pin.scoring_type} · {pin.season}
                  </span>
                </button>
                <button
                  onClick={() => togglePin(pin)}
                  title="Unpin"
                  aria-label={`Unpin ${pin.name}`}
                  className="p-1 rounded-full text-slate-400 hover:text-red-500 hover:bg-slate-100 dark:hover:bg-slate-800"
                >
                  <PinOff size={12} />
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {restoring && (
        <div className="flex items-center gap-2 text-sm text-slate-500" data-testid="sleeper-restoring">
          <Loader2 size={14} className="animate-spin" /> Restoring your last team…
        </div>
      )}

      {error && (
        <div className="flex items-start gap-2 p-3 rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 text-sm text-red-700 dark:text-red-300">
          <AlertTriangle size={16} className="mt-0.5 shrink-0" />
          <span data-testid="sleeper-error">{error}</span>
        </div>
      )}

      {/* Step 1 — username. Collapses once a team is loaded so the analysis
          gets the screen; the breadcrumb walks back to it. */}
      {!analysis && (
        <div className="flex flex-wrap items-end gap-2">
          <div className="flex-1 min-w-[220px]">
            <label className="text-[10px] uppercase font-bold tracking-wider text-slate-400">
              Sleeper username
            </label>
            <input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && lookupUser()}
              placeholder="e.g. yourhandle"
              data-testid="sleeper-username"
              className="w-full mt-1 px-3 py-2 rounded-lg bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-700 text-sm outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div className="w-28">
            <label className="text-[10px] uppercase font-bold tracking-wider text-slate-400">
              Season
            </label>
            <input
              type="number"
              value={seasonInput}
              onChange={(e) => setSeasonInput(Number(e.target.value))}
              className="w-full mt-1 px-3 py-2 rounded-lg bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-700 text-sm outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <button
            onClick={lookupUser}
            disabled={loading || !username.trim()}
            data-testid="sleeper-lookup"
            className="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-700 disabled:opacity-40 text-white text-sm font-bold flex items-center gap-2"
          >
            {loading ? <Loader2 size={14} className="animate-spin" /> : <Search size={14} />}
            Find leagues
          </button>
          {stage !== 'USER' && (
            <button onClick={reset} data-testid="sleeper-reset" className="px-3 py-2 rounded-lg text-sm font-bold text-slate-500 hover:text-slate-700 dark:hover:text-slate-300">
              Start over
            </button>
          )}
        </div>
      )}

      {/* Step 2 — league */}
      {stage !== 'USER' && !analysis && (
        <div>
          <h2 className="text-sm font-black uppercase tracking-wider text-slate-500 mb-2 flex items-center gap-2">
            <Users size={14} /> {user?.display_name || user?.username}'s leagues ({leagues.length})
          </h2>
          {leagues.length === 0 ? (
            <div className="p-4 rounded-lg border border-dashed border-slate-300 dark:border-slate-700 text-sm text-slate-500">
              No NFL leagues found for {seasonInput}. Try a different season — Sleeper keeps each
              season separately, and a league only appears once it has been created for that year.
            </div>
          ) : (
            <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
              {leagues.map((lg) => (
                <div
                  key={lg.league_id}
                  className={`flex items-start gap-2 p-3 rounded-lg border transition-colors ${
                    league?.league_id === lg.league_id
                      ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                      : 'border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 hover:border-blue-400'
                  }`}
                >
                  <button
                    onClick={() => chooseLeague(lg)}
                    data-testid="sleeper-league"
                    className="text-left min-w-0 flex-1"
                  >
                    <div className="font-bold text-sm text-slate-800 dark:text-slate-100 truncate">{lg.name}</div>
                    <div className="text-[11px] text-slate-500">
                      {lg.total_rosters} teams · {lg.scoring_type} · {lg.season}
                    </div>
                  </button>
                  <button
                    onClick={() => togglePin(lg)}
                    data-testid="sleeper-pin-toggle"
                    data-pinned={isPinned(lg.league_id)}
                    title={isPinned(lg.league_id) ? 'Unpin this league' : 'Pin this league'}
                    aria-label={isPinned(lg.league_id) ? `Unpin ${lg.name}` : `Pin ${lg.name}`}
                    className={`p-1.5 rounded-lg shrink-0 transition-colors ${
                      isPinned(lg.league_id)
                        ? 'text-blue-600 dark:text-blue-400 bg-blue-100 dark:bg-blue-900/30'
                        : 'text-slate-400 hover:text-blue-600 hover:bg-slate-100 dark:hover:bg-slate-800'
                    }`}
                  >
                    <Pin size={13} />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Step 3 — team */}
      {stage === 'TEAM' && teams.length > 0 && !analysis && (
        <div>
          <h2 className="text-sm font-black uppercase tracking-wider text-slate-500 mb-2">
            Pick your team in {league?.name}
          </h2>
          <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
            {teams.map((t) => (
              <button
                key={t.roster_id}
                onClick={() => chooseTeam(t.roster_id)}
                data-testid="sleeper-team"
                className="text-left p-3 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 hover:border-blue-400 transition-colors"
              >
                <div className="font-bold text-sm text-slate-800 dark:text-slate-100 truncate">{t.team_name}</div>
                <div className="text-[11px] text-slate-500">
                  {t.display_name} · {t.wins}-{t.losses}{t.ties ? `-${t.ties}` : ''} · {t.player_count} players
                </div>
              </button>
            ))}
          </div>
        </div>
      )}

      {loading && !restoring && (
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <Loader2 size={16} className="animate-spin" /> Working…
        </div>
      )}

      {/* Analysis */}
      {analysis && (
        <div className="space-y-4">
          <div className="flex flex-wrap items-center gap-2 border-b border-slate-200 dark:border-slate-700 pb-2">
            <button
              onClick={() => setTab('LINEUP')}
              data-testid="sleeper-tab-lineup"
              data-active={tab === 'LINEUP'}
              className={`px-3 py-1.5 rounded-lg text-xs font-bold ${tab === 'LINEUP' ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800'}`}
            >
              Lineup
            </button>
            <button
              onClick={loadWaivers}
              data-testid="sleeper-tab-waivers"
              data-active={tab === 'WAIVERS'}
              className={`px-3 py-1.5 rounded-lg text-xs font-bold ${tab === 'WAIVERS' ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800'}`}
            >
              Waiver wire
            </button>
            <button
              onClick={loadLeague}
              data-testid="sleeper-tab-league"
              data-active={tab === 'LEAGUE'}
              className={`px-3 py-1.5 rounded-lg text-xs font-bold ${tab === 'LEAGUE' ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800'}`}
            >
              League
            </button>
            <button onClick={reset} data-testid="sleeper-reset" className="px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400 hover:text-slate-600 dark:hover:text-slate-300">
              Start over
            </button>
            <div className="ml-auto text-xs text-slate-500">
              Week {analysis.week} · projected{' '}
              <span className="font-black text-slate-800 dark:text-slate-100" data-testid="sleeper-projected-total">
                {num(analysis.projected_total).toFixed(1)}
              </span>{' '}
              pts
              {num(analysis.special_teams_projected_total) > 0 && (
                <span className="text-slate-400" title="Your saved kicker and defense, projected by Sleeper">
                  {' '}+ {num(analysis.special_teams_projected_total).toFixed(1)} K/DEF (Sleeper)
                </span>
              )}
            </div>
          </div>

          {tab === 'LEAGUE' ? (
            leagueData ? (
              <LeagueInsights data={leagueData} onFindWaivers={() => loadWaivers()} />
            ) : null
          ) : tab === 'LINEUP' ? (
            <div className="grid gap-4 lg:grid-cols-2">
              <div>
                <h3 className="text-xs font-black uppercase tracking-wider text-slate-500 mb-2 flex items-center gap-1.5">
                  <CheckCircle2 size={13} className="text-green-500" /> Recommended starters
                </h3>
                <div className="space-y-2">
                  {(analysis.recommended_starters || []).map((p: any) => (
                    <PlayerRow
                      key={p.sleeper_id}
                      player={p}
                      onOpenHistory={onOpenHistory}
                      compare={compareFor(p)}
                      badge={!p.in_saved_lineup ? (
                        <span className="text-[9px] font-black uppercase px-1.5 py-0.5 rounded bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400">
                          Bench → Start
                        </span>
                      ) : undefined}
                    />
                  ))}
                </div>
              </div>
              <div>
                <h3 className="text-xs font-black uppercase tracking-wider text-slate-500 mb-2">
                  Bench ({bench.length})
                </h3>
                <div className="space-y-2">
                  {bench.map((p: any) => (
                    <PlayerRow
                      key={p.sleeper_id}
                      player={p}
                      onOpenHistory={onOpenHistory}
                      compare={compareFor(p)}
                      badge={p.in_saved_lineup ? (
                        <span className="text-[9px] font-black uppercase px-1.5 py-0.5 rounded bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400">
                          Start → Sit
                        </span>
                      ) : undefined}
                    />
                  ))}
                </div>
              </div>

              {(analysis.bench_but_should_start?.length > 0 || analysis.start_but_should_sit?.length > 0) && (
                <div className="lg:col-span-2 p-3 rounded-lg bg-slate-100 dark:bg-slate-800/60 text-sm">
                  <div className="font-black text-xs uppercase tracking-wider text-slate-500 mb-1 flex items-center gap-1.5">
                    <ArrowLeftRight size={13} /> Changes vs your saved lineup
                  </div>
                  {analysis.bench_but_should_start?.length > 0 && (
                    <div className="text-green-700 dark:text-green-400">
                      Start: {analysis.bench_but_should_start.map((p: any) => p.player_name).join(', ')}
                    </div>
                  )}
                  {analysis.start_but_should_sit?.length > 0 && (
                    <div className="text-orange-700 dark:text-orange-400">
                      Sit: {analysis.start_but_should_sit.map((p: any) => p.player_name).join(', ')}
                    </div>
                  )}
                </div>
              )}

              {analysis.special_teams?.length > 0 && (
                <div className="lg:col-span-2" data-testid="sleeper-special-teams">
                  <h3 className="text-xs font-black uppercase tracking-wider text-slate-500 mb-1">
                    Kicker &amp; defense
                  </h3>
                  <p className="text-[11px] text-slate-400 mb-2">
                    We don't model these, so they carry Sleeper's projection and get no start/sit call from us.
                  </p>
                  <div className="grid gap-2 sm:grid-cols-2">
                    {analysis.special_teams.map((p: any) => (
                      <PlayerRow
                        key={p.sleeper_id}
                        player={p}
                        sourceLabel="Sleeper"
                        onOpenHistory={p.position === 'K' ? onOpenHistory : undefined}
                        badge={p.in_saved_lineup ? (
                          <span className="text-[9px] font-black uppercase px-1.5 py-0.5 rounded bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400">
                            In lineup
                          </span>
                        ) : undefined}
                      />
                    ))}
                  </div>
                </div>
              )}

              {analysis.unmatched_sleeper_ids?.length > 0 && (
                <div className="lg:col-span-2 text-[11px] text-slate-400">
                  {analysis.unmatched_sleeper_ids.length} rostered player(s) could not be matched to
                  our player database.
                </div>
              )}
            </div>
          ) : (
            <div>
              <h3 className="text-xs font-black uppercase tracking-wider text-slate-500 mb-2 flex items-center gap-1.5">
                <TrendingUp size={13} /> Free agents ranked by our projection
                {waivers ? <span className="font-normal text-slate-400">· {waivers.rostered} rostered leaguewide</span> : null}
              </h3>
              <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
                {(waivers?.players || []).map((p: any) => (
                  <PlayerRow
                    key={p.sleeper_id}
                    player={p}
                    onOpenHistory={onOpenHistory}
                    compare={compareFor(p)}
                    badge={p.trending_adds ? (
                      <span className="text-[9px] font-black px-1.5 py-0.5 rounded bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400">
                        +{p.trending_adds > 999 ? `${Math.round(p.trending_adds / 1000)}K` : p.trending_adds}
                      </span>
                    ) : undefined}
                  />
                ))}
              </div>
              {!loading && waivers && (waivers.players || []).length === 0 && (
                <div className="text-sm text-slate-500">No available free agents matched.</div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Compare bar: bottom-left, clear of the assistant button on the right. */}
      {analysis && compareIds.length > 0 && (
        <div className="fixed z-40 bottom-20 md:bottom-6 left-1/2 -translate-x-1/2 xl:left-[22rem] xl:translate-x-0 flex items-center gap-2 pl-3 pr-1.5 py-1.5 rounded-full bg-slate-900 text-white shadow-xl max-w-[calc(100vw-2rem)]" data-testid="sleeper-compare-bar">
          <Scale size={14} className="shrink-0" />
          <span className="text-[12px] font-bold truncate">
            {compareIds.length === 1 ? `${compareNames[0]}: pick one more` : compareNames.join(' vs ')}
          </span>
          <button
            type="button"
            onClick={() => setCompareOpen(true)}
            disabled={compareIds.length < 1}
            data-testid="sleeper-compare-open"
            className="text-[12px] font-black px-3 py-1 rounded-full bg-blue-600 hover:bg-blue-500 disabled:opacity-40 shrink-0"
          >
            Compare
          </button>
          <button type="button" onClick={() => setCompareIds([])} aria-label="Clear comparison" className="p-1 rounded-full text-slate-400 hover:text-white shrink-0">
            <X size={13} />
          </button>
        </div>
      )}

      {compareOpen && (
        <PlayerCompareModal
          week={week}
          initialIds={compareIds}
          onClose={() => setCompareOpen(false)}
          onOpenHistory={(id) => { setCompareOpen(false); onOpenHistory?.(id); }}
        />
      )}
    </div>
  );
};

export default SleeperView;
