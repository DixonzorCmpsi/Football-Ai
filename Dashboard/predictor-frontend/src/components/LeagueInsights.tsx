/**
 * The whole league: where each team stands, how lucky it has been, how strong its
 * lineup projects, position by position, and this week's matchups.
 *
 * Built by the backend from Sleeper's public API plus our projections
 * (services/league_insights.py). The methodology footnote says how each number
 * is made, since "power" and "luck" mean something different on every site.
 */

import { Fragment, useMemo, useState } from 'react';
import { ChevronDown, ChevronRight, Crown, Info, Swords, TrendingDown, TrendingUp } from 'lucide-react';

type Starter = { slot: string; name: string; position: string; team?: string; projection: number; source?: string; injury_status?: string };

export type LeagueTeam = {
  roster_id: number;
  team_name: string;
  owner?: string;
  wins: number;
  losses: number;
  ties: number;
  points_for: number;
  points_against: number;
  max_points_for: number | null;
  efficiency: number | null;
  all_play: { wins: number; losses: number; ties: number; pct: number | null };
  luck: number | null;
  streak: string | null;
  projected_total: number;
  by_group: Record<string, number>;
  position_ranks: Record<string, number>;
  starters: Starter[];
  bench_top3: number;
  power_score: number;
  power_rank: number;
  standing_rank: number;
};

export type LeagueInsightsData = {
  league: { league_id: string; name: string; week: number; weeks_played: number; scoring_type: string; total_rosters: number };
  teams: LeagueTeam[];
  position_groups: string[];
  matchups: { matchup_id: number; teams: { roster_id: number; team_name: string; projected: number; live_points: number; win_probability: number }[] }[];
  you: null | {
    roster_id: number; standing_rank: number; power_rank: number; projected_rank: number; teams: number;
    strongest: string | null; weakest: string | null;
    opponent: { roster_id: number; team_name: string; projected: number; win_probability: number } | null;
  };
  method: { power_weights: { projection: number; all_play: number; points_for: number }; notes: string[] };
};

type SortKey = 'power_rank' | 'standing_rank' | 'points_for' | 'projected_total' | 'luck' | 'efficiency';

const ordinal = (n: number) => `${n}${['th', 'st', 'nd', 'rd'][(n % 100 - 20) % 10] || ['th', 'st', 'nd', 'rd'][n % 100] || 'th'}`;

/** Green for 1st, red for last. */
function rankColor(rank: number, of: number): string {
  if (of <= 1) return 'hsl(142 60% 45% / 0.25)';
  const t = (rank - 1) / (of - 1);
  return `hsl(${Math.round(142 - 142 * t)} 65% 45% / 0.22)`;
}

export default function LeagueInsights({
  data,
  onFindWaivers,
}: {
  data: LeagueInsightsData;
  onFindWaivers?: (position: string) => void;
}) {
  const [sort, setSort] = useState<SortKey>('power_rank');
  const [open, setOpen] = useState<number | null>(null);
  const n = data.teams.length;
  const mine = data.you?.roster_id;
  const played = data.league.weeks_played;

  const teams = useMemo(() => {
    const out = [...data.teams];
    const desc = sort === 'points_for' || sort === 'projected_total' || sort === 'luck' || sort === 'efficiency';
    out.sort((a, b) => {
      const va = (a[sort] ?? -Infinity) as number;
      const vb = (b[sort] ?? -Infinity) as number;
      return desc ? vb - va : va - vb;
    });
    return out;
  }, [data.teams, sort]);

  const yours = data.teams.find((t) => t.roster_id === mine);
  const w = data.method.power_weights;

  const header = (key: SortKey, label: string, title?: string) => (
    <th className="px-2 py-2 text-right font-bold whitespace-nowrap">
      <button type="button" title={title} onClick={() => setSort(key)} className={`uppercase tracking-wider text-[10px] ${sort === key ? 'text-blue-600 dark:text-blue-400' : 'text-slate-400 hover:text-slate-600'}`}>
        {label}
      </button>
    </th>
  );

  return (
    <div className="space-y-5" data-testid="league-insights">
      {/* Where you stand */}
      {data.you && yours && (
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4" data-testid="league-you">
          <div className="p-3 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900">
            <div className="text-[10px] font-black uppercase tracking-widest text-slate-400">Standings</div>
            <div className="text-2xl font-black text-slate-800 dark:text-slate-100">{ordinal(data.you.standing_rank)} <span className="text-sm font-bold text-slate-400">of {n}</span></div>
            <div className="text-[11px] text-slate-500">{yours.wins}-{yours.losses}{yours.ties ? `-${yours.ties}` : ''} · {yours.points_for.toFixed(1)} PF{played === 0 ? ' · no games scored yet' : ''}</div>
          </div>
          <div className="p-3 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900">
            <div className="text-[10px] font-black uppercase tracking-widest text-slate-400">Power rank</div>
            <div className="text-2xl font-black text-slate-800 dark:text-slate-100">{ordinal(data.you.power_rank)} <span className="text-sm font-bold text-slate-400">of {n}</span></div>
            <div className="text-[11px] text-slate-500">Projected lineup {ordinal(data.you.projected_rank)} this week ({yours.projected_total.toFixed(1)})</div>
          </div>
          <div className="p-3 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900">
            <div className="text-[10px] font-black uppercase tracking-widest text-slate-400">Your roster</div>
            <div className="mt-1 flex flex-wrap gap-1.5 text-[11px]">
              {data.you.strongest && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300 font-bold">
                  <TrendingUp size={11} /> {data.you.strongest} {ordinal(yours.position_ranks[data.you.strongest])}
                </span>
              )}
              {data.you.weakest && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300 font-bold">
                  <TrendingDown size={11} /> {data.you.weakest} {ordinal(yours.position_ranks[data.you.weakest])}
                </span>
              )}
            </div>
            {data.you.weakest && onFindWaivers && (
              <button type="button" onClick={() => onFindWaivers(data.you!.weakest!)} className="mt-2 text-[11px] font-bold text-blue-600 dark:text-blue-400 hover:underline">
                Look for a {data.you.weakest === 'FLEX' ? 'RB/WR/TE' : data.you.weakest} on waivers →
              </button>
            )}
          </div>
          <div className="p-3 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900">
            <div className="text-[10px] font-black uppercase tracking-widest text-slate-400">Week {data.league.week} matchup</div>
            {data.you.opponent ? (
              <>
                <div className="text-sm font-black text-slate-800 dark:text-slate-100 truncate">vs {data.you.opponent.team_name}</div>
                <div className="text-[11px] text-slate-500">
                  {yours.projected_total.toFixed(1)} to {data.you.opponent.projected.toFixed(1)} projected
                </div>
                <div className="mt-1.5 h-1.5 rounded-full bg-slate-200 dark:bg-slate-700 overflow-hidden">
                  <div className="h-full bg-blue-600" style={{ width: `${Math.round((1 - data.you.opponent.win_probability) * 100)}%` }} />
                </div>
                <div className="text-[10px] text-slate-400 mt-0.5">{Math.round((1 - data.you.opponent.win_probability) * 100)}% to win (rough)</div>
              </>
            ) : (
              <div className="text-[11px] text-slate-500 mt-1">No matchup found for this week.</div>
            )}
          </div>
        </div>
      )}

      {/* Power rankings */}
      <div>
        <h3 className="text-xs font-black uppercase tracking-wider text-slate-500 mb-2 flex items-center gap-1.5">
          <Crown size={13} /> League table
          <span className="font-normal normal-case tracking-normal text-slate-400">· click a column to sort, a team to see its projected lineup</span>
        </h3>
        <div className="overflow-x-auto rounded-xl border border-slate-200 dark:border-slate-700">
          <table className="w-full min-w-[760px] text-[12px] bg-white dark:bg-slate-900" data-testid="league-table">
            <thead className="bg-slate-50 dark:bg-slate-800/60">
              <tr>
                {header('power_rank', 'Power', 'Power rank')}
                <th className="px-2 py-2 text-left text-[10px] uppercase tracking-wider text-slate-400">Team</th>
                {header('standing_rank', 'Record', 'Standings order')}
                {header('points_for', 'PF')}
                <th className="px-2 py-2 text-right text-[10px] uppercase tracking-wider text-slate-400">PA</th>
                <th className="px-2 py-2 text-right text-[10px] uppercase tracking-wider text-slate-400" title="Record if you played every team every week">All-play</th>
                {header('luck', 'Luck', 'Real win rate minus all-play win rate')}
                {header('efficiency', 'Eff.', 'Points scored as a share of the best possible lineup')}
                {header('projected_total', `Proj wk ${data.league.week}`)}
              </tr>
            </thead>
            <tbody>
              {teams.map((t) => {
                const isMine = t.roster_id === mine;
                return (
                  <Fragment key={t.roster_id}>
                    <tr
                      onClick={() => setOpen(open === t.roster_id ? null : t.roster_id)}
                      data-testid="league-row"
                      className={`cursor-pointer border-t border-slate-100 dark:border-slate-800 ${isMine ? 'bg-blue-50/70 dark:bg-blue-900/20' : 'hover:bg-slate-50 dark:hover:bg-slate-800/40'}`}
                    >
                      <td className="px-2 py-2 text-right font-black tabular-nums">{t.power_rank}</td>
                      <td className="px-2 py-2">
                        <div className="flex items-center gap-1.5 min-w-0">
                          {open === t.roster_id ? <ChevronDown size={12} className="text-slate-400 shrink-0" /> : <ChevronRight size={12} className="text-slate-400 shrink-0" />}
                          <span className={`font-bold truncate ${isMine ? 'text-blue-700 dark:text-blue-300' : 'text-slate-800 dark:text-slate-100'}`}>{t.team_name}</span>
                          {isMine && <span className="text-[9px] font-black uppercase px-1 rounded bg-blue-600 text-white shrink-0">You</span>}
                          {t.streak && <span className="text-[10px] text-slate-400 shrink-0">{t.streak}</span>}
                        </div>
                        {t.owner && <div className="text-[10px] text-slate-400 pl-5 truncate">{t.owner}</div>}
                      </td>
                      <td className="px-2 py-2 text-right tabular-nums">{t.wins}-{t.losses}{t.ties ? `-${t.ties}` : ''} <span className="text-[10px] text-slate-400">({ordinal(t.standing_rank)})</span></td>
                      <td className="px-2 py-2 text-right tabular-nums">{t.points_for.toFixed(1)}</td>
                      <td className="px-2 py-2 text-right tabular-nums text-slate-500">{t.points_against.toFixed(1)}</td>
                      <td className="px-2 py-2 text-right tabular-nums text-slate-500">{played ? `${t.all_play.wins}-${t.all_play.losses}${t.all_play.ties ? `-${t.all_play.ties}` : ''}` : '–'}</td>
                      <td className={`px-2 py-2 text-right tabular-nums font-bold ${t.luck == null ? 'text-slate-400' : t.luck > 0.05 ? 'text-amber-600' : t.luck < -0.05 ? 'text-sky-600' : 'text-slate-500'}`}>
                        {t.luck == null ? '–' : `${t.luck > 0 ? '+' : ''}${Math.round(t.luck * 100)}%`}
                      </td>
                      <td className="px-2 py-2 text-right tabular-nums text-slate-500">{t.efficiency == null ? '–' : `${Math.round(t.efficiency * 100)}%`}</td>
                      <td className="px-2 py-2 text-right tabular-nums font-black">{t.projected_total.toFixed(1)}</td>
                    </tr>
                    {open === t.roster_id && (
                      <tr className="bg-slate-50 dark:bg-slate-800/30">
                        <td />
                        <td colSpan={8} className="px-2 py-2">
                          <div className="flex flex-wrap gap-1.5">
                            {t.starters.map((s, i) => (
                              <span key={`${s.name}-${i}`} className="text-[11px] px-2 py-1 rounded-lg bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700">
                                <span className="font-black text-slate-400 mr-1">{s.slot}</span>
                                <span className="font-bold text-slate-700 dark:text-slate-200">{s.name}</span>
                                <span className="ml-1 tabular-nums text-slate-500">{s.projection.toFixed(1)}</span>
                                {s.source === 'sleeper' && <span className="ml-1 text-[9px] text-slate-400">Sleeper</span>}
                                {s.injury_status && !/^act(ive)?$/i.test(s.injury_status) && <span className="ml-1 text-[9px] font-black text-red-500 uppercase">{s.injury_status}</span>}
                              </span>
                            ))}
                            <span className="text-[11px] px-2 py-1 text-slate-400">Top 3 bench: {t.bench_top3.toFixed(1)}</span>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Position strength heat map */}
      <div>
        <h3 className="text-xs font-black uppercase tracking-wider text-slate-500 mb-2">Projected strength by position, week {data.league.week}</h3>
        <div className="overflow-x-auto rounded-xl border border-slate-200 dark:border-slate-700">
          <table className="w-full min-w-[560px] text-[12px] bg-white dark:bg-slate-900" data-testid="league-positions">
            <thead className="bg-slate-50 dark:bg-slate-800/60">
              <tr>
                <th className="px-2 py-2 text-left text-[10px] uppercase tracking-wider text-slate-400">Team</th>
                {data.position_groups.map((g) => (
                  <th key={g} className="px-2 py-2 text-center text-[10px] uppercase tracking-wider text-slate-400">{g}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[...data.teams].sort((a, b) => a.power_rank - b.power_rank).map((t) => (
                <tr key={t.roster_id} className={`border-t border-slate-100 dark:border-slate-800 ${t.roster_id === mine ? 'font-black' : ''}`}>
                  <td className={`px-2 py-1.5 truncate max-w-[180px] ${t.roster_id === mine ? 'text-blue-700 dark:text-blue-300' : 'text-slate-700 dark:text-slate-200'}`}>{t.team_name}</td>
                  {data.position_groups.map((g) => {
                    const rank = t.position_ranks[g];
                    return (
                      <td key={g} className="px-1 py-1 text-center">
                        <div className="rounded-md py-1 tabular-nums" style={{ backgroundColor: rankColor(rank, n) }} title={`${ordinal(rank)} of ${n}`}>
                          <div className="font-bold text-slate-800 dark:text-slate-100">{(t.by_group[g] ?? 0).toFixed(1)}</div>
                          <div className="text-[9px] text-slate-500">{ordinal(rank)}</div>
                        </div>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Matchups */}
      {data.matchups.length > 0 && (
        <div>
          <h3 className="text-xs font-black uppercase tracking-wider text-slate-500 mb-2 flex items-center gap-1.5"><Swords size={13} /> Week {data.league.week} matchups</h3>
          <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-3" data-testid="league-matchups">
            {data.matchups.map((m) => {
              const [a, b] = m.teams;
              const live = a.live_points > 0 || b.live_points > 0;
              const involvesMe = m.teams.some((s) => s.roster_id === mine);
              return (
                <div key={m.matchup_id} className={`p-3 rounded-xl border bg-white dark:bg-slate-900 ${involvesMe ? 'border-blue-400' : 'border-slate-200 dark:border-slate-700'}`}>
                  {[a, b].map((s) => (
                    <div key={s.roster_id} className="flex items-center justify-between gap-2 text-[12px]">
                      <span className={`truncate ${s.roster_id === mine ? 'font-black text-blue-700 dark:text-blue-300' : 'font-bold text-slate-700 dark:text-slate-200'}`}>{s.team_name}</span>
                      <span className="tabular-nums text-slate-500 shrink-0">
                        {live && <span className="font-black text-slate-800 dark:text-slate-100 mr-1.5">{s.live_points.toFixed(1)}</span>}
                        {s.projected.toFixed(1)} proj · {Math.round(s.win_probability * 100)}%
                      </span>
                    </div>
                  ))}
                  <div className="mt-2 h-1.5 rounded-full bg-slate-200 dark:bg-slate-700 overflow-hidden flex">
                    <div className="h-full bg-blue-600" style={{ width: `${Math.round(a.win_probability * 100)}%` }} />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div className="text-[10px] text-slate-400 flex items-start gap-1.5 leading-relaxed">
        <Info size={11} className="mt-0.5 shrink-0" />
        <span>
          Power blends projected lineup strength ({Math.round(w.projection * 100)}%), all-play record ({Math.round(w.all_play * 100)}%) and points per game ({Math.round(w.points_for * 100)}%), shifting toward results as weeks are played ({played} so far).{' '}
          {data.method.notes.join(' ')}
        </span>
      </div>
    </div>
  );
}
