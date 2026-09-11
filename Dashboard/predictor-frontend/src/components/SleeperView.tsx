import React, { useCallback, useMemo, useState } from 'react';
import { Loader2, Search, Users, TrendingUp, AlertTriangle, CheckCircle2, ArrowLeftRight } from 'lucide-react';
import {
  fetchSleeperUser,
  fetchSleeperLeague,
  fetchSleeperRosterAnalysis,
  fetchSleeperWaivers,
  type SleeperLeague,
  type SleeperTeam,
} from '../lib/api';
import { sizedPlayerImage } from '../utils/playerImage';

interface SleeperViewProps {
  week: number;
  season: number;
  onOpenHistory?: (playerId: string) => void;
}

type Stage = 'USER' | 'LEAGUE' | 'TEAM';
type Tab = 'LINEUP' | 'WAIVERS';

const num = (v: unknown) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

const PlayerRow: React.FC<{
  player: any;
  badge?: React.ReactNode;
  onOpenHistory?: (id: string) => void;
}> = ({ player, badge, onOpenHistory }) => (
  <button
    type="button"
    onClick={() => onOpenHistory && onOpenHistory(player.player_id)}
    data-testid="sleeper-player-row"
    data-position={player.position}
    className="w-full text-left flex items-center gap-3 p-2.5 rounded-lg bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 hover:border-blue-400 dark:hover:border-blue-500 transition-colors"
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
      <div className="text-[10px] uppercase tracking-wider text-slate-400 font-bold">Proj</div>
      <div className="font-black text-slate-800 dark:text-slate-100 tabular-nums">
        {num(player.prediction).toFixed(1)}
      </div>
    </div>
  </button>
);

const SleeperView: React.FC<SleeperViewProps> = ({ week, season, onOpenHistory }) => {
  const [stage, setStage] = useState<Stage>('USER');
  const [username, setUsername] = useState('');
  const [seasonInput, setSeasonInput] = useState(season);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [user, setUser] = useState<any>(null);
  const [leagues, setLeagues] = useState<SleeperLeague[]>([]);
  const [league, setLeague] = useState<SleeperLeague | null>(null);
  const [teams, setTeams] = useState<SleeperTeam[]>([]);
  const [analysis, setAnalysis] = useState<any>(null);
  const [waivers, setWaivers] = useState<any>(null);
  const [tab, setTab] = useState<Tab>('LINEUP');

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

  const lookupUser = useCallback(() => {
    if (!username.trim()) return;
    run(async () => {
      const data = await fetchSleeperUser(username.trim(), seasonInput);
      setUser(data.user);
      setLeagues(data.leagues || []);
      setStage('LEAGUE');
    });
  }, [username, seasonInput, run]);

  const chooseLeague = useCallback((lg: SleeperLeague) => {
    run(async () => {
      const data = await fetchSleeperLeague(lg.league_id);
      setLeague(data.league);
      setTeams(data.teams || []);
      setStage('TEAM');
    });
  }, [run]);

  const chooseTeam = useCallback((rosterId: number) => {
    if (!league) return;
    run(async () => {
      const data = await fetchSleeperRosterAnalysis(league.league_id, rosterId, week);
      setAnalysis(data);
      setTab('LINEUP');
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

  const reset = () => {
    setStage('USER'); setUser(null); setLeagues([]); setLeague(null);
    setTeams([]); setAnalysis(null); setWaivers(null); setError(null);
  };

  const recommendedIds = useMemo(
    () => new Set((analysis?.recommended_starters || []).map((c: any) => c.sleeper_id)),
    [analysis],
  );
  const bench = useMemo(
    () => (analysis?.players || []).filter((p: any) => !recommendedIds.has(p.sleeper_id)),
    [analysis, recommendedIds],
  );

  return (
    <div className="space-y-5" data-testid="sleeper-view">
      <div>
        <div className="text-[11px] font-bold tracking-[0.2em] text-slate-400 uppercase">
          My Team · Sleeper
        </div>
        <h1 className="text-2xl font-black text-slate-800 dark:text-slate-100">
          Import your fantasy roster
        </h1>
        <p className="text-sm text-slate-500 mt-1">
          Sleeper's read API is public, so this needs your username only — never a password.
        </p>
      </div>

      {error && (
        <div className="flex items-start gap-2 p-3 rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 text-sm text-red-700 dark:text-red-300">
          <AlertTriangle size={16} className="mt-0.5 shrink-0" />
          <span data-testid="sleeper-error">{error}</span>
        </div>
      )}

      {/* Step 1 — username */}
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
          <button onClick={reset} className="px-3 py-2 rounded-lg text-sm font-bold text-slate-500 hover:text-slate-700 dark:hover:text-slate-300">
            Start over
          </button>
        )}
      </div>

      {/* Step 2 — league */}
      {stage !== 'USER' && (
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
                <button
                  key={lg.league_id}
                  onClick={() => chooseLeague(lg)}
                  data-testid="sleeper-league"
                  className={`text-left p-3 rounded-lg border transition-colors ${
                    league?.league_id === lg.league_id
                      ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                      : 'border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 hover:border-blue-400'
                  }`}
                >
                  <div className="font-bold text-sm text-slate-800 dark:text-slate-100 truncate">{lg.name}</div>
                  <div className="text-[11px] text-slate-500">
                    {lg.total_rosters} teams · {lg.scoring_type} · {lg.season}
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Step 3 — team */}
      {stage === 'TEAM' && teams.length > 0 && (
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
                className={`text-left p-3 rounded-lg border transition-colors ${
                  analysis?.roster_id === t.roster_id
                    ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                    : 'border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 hover:border-blue-400'
                }`}
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

      {loading && (
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
              className={`px-3 py-1.5 rounded-lg text-xs font-bold ${tab === 'LINEUP' ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800'}`}
            >
              Lineup
            </button>
            <button
              onClick={loadWaivers}
              data-testid="sleeper-tab-waivers"
              className={`px-3 py-1.5 rounded-lg text-xs font-bold ${tab === 'WAIVERS' ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800'}`}
            >
              Waiver wire
            </button>
            <div className="ml-auto text-xs text-slate-500">
              Week {analysis.week} · projected{' '}
              <span className="font-black text-slate-800 dark:text-slate-100" data-testid="sleeper-projected-total">
                {num(analysis.projected_total).toFixed(1)}
              </span>{' '}
              pts
            </div>
          </div>

          {tab === 'LINEUP' ? (
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

              {analysis.unmatched_sleeper_ids?.length > 0 && (
                <div className="lg:col-span-2 text-[11px] text-slate-400">
                  {analysis.unmatched_sleeper_ids.length} rostered player(s) could not be matched to
                  our player database (usually kickers or team defenses, which this model does not
                  project).
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
    </div>
  );
};

export default SleeperView;
