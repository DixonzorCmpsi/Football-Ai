import { useEffect, useState } from 'react';
import { X } from 'lucide-react';
import { API_BASE_URL } from '../lib/api';
import { seriesColor } from './visualSeries';

interface UsageGame {
  week: number;
  opponent: string | null;
  snaps: number | null;
  snap_pct: number | null;
  carries: number;
  team_carries: number;
  targets: number;
  team_targets: number;
  pass_att: number;
  team_pass_att: number;
  carry_share: number | null;
  target_share: number | null;
  pass_att_share: number | null;
  rec_yd_share: number | null;
  air_yd_share: number | null;
}

interface Summary {
  games: number;
  carries: number;
  team_carries: number;
  targets: number;
  team_targets: number;
  pass_att: number;
  team_pass_att: number;
  carries_per_game: number;
  targets_per_game: number;
  snap_pct: number | null;
  carry_share: number | null;
  target_share: number | null;
  pass_att_share: number | null;
  rush_yd_share: number | null;
  rec_yd_share: number | null;
  air_yd_share: number | null;
}

interface Mate {
  player_id: string;
  name: string;
  position: string;
  games: number;
  carries: number;
  targets: number;
  pass_att: number;
  carry_share: number | null;
  target_share: number | null;
  pass_att_share: number | null;
  rank: number;
}

interface Usage {
  player_id: string;
  name?: string;
  season: number | null;
  team: string | null;
  position: string | null;
  games: UsageGame[];
  summary: Summary | null;
  teammates: Mate[];
  team_totals?: { carries: number; targets: number; pass_att: number };
}

interface Props {
  playerId: string;
  /** The season picked in the page header; null until the history has loaded. */
  season: number | null;
  teamColor: string;
  onSelectPlayer?: (playerId: string) => void;
  /** Players picked for comparison (not including this one) and how to drop one. */
  extraIds?: string[];
  onRemove?: (id: string) => void;
}

const pct = (v: number | null | undefined) => (v == null ? '–' : `${v.toFixed(1)}%`);

function Bar({ value, color, height = 'h-2' }: { value: number | null | undefined; color: string; height?: string }) {
  return (
    <div className={`w-full ${height} rounded-full bg-slate-200 dark:bg-slate-700 overflow-hidden`}>
      <div className={`${height} rounded-full`} style={{ width: `${Math.min(100, Math.max(0, value ?? 0))}%`, background: color }} />
    </div>
  );
}

/** The stat every table leads with for this position. */
type Lead = 'carries' | 'targets' | 'pass_att';
const leadFor = (pos: string | null): Lead =>
  pos === 'QB' ? 'pass_att' : pos === 'RB' || pos === 'FB' ? 'carries' : 'targets';

/** Loads each player's usage; the result is keyed by id and fills in as the requests land. */
function useUsages(ids: string[], season: number | null) {
  const [data, setData] = useState<Record<string, Usage>>({});
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const key = ids.join(',');

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    const q = season != null ? `?season=${season}` : '';
    Promise.all(
      ids.map((id) =>
        fetch(`${API_BASE_URL}/player/${id}/usage${q}`)
          .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
          .then((d: Usage) => [id, d] as const),
      ),
    )
      .then((pairs) => !cancelled && setData(Object.fromEntries(pairs)))
      .catch((e) => !cancelled && setError(String(e.message || e)))
      .finally(() => !cancelled && setLoading(false));
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, season]);

  return { data, error, loading };
}

/** A player's loaded usage plus the name and position the page already knows. */
function useNames(ids: string[]) {
  const [names, setNames] = useState<Record<string, { name: string; position: string; team: string }>>({});
  const key = ids.join(',');
  useEffect(() => {
    let cancelled = false;
    ids.forEach((id) => {
      fetch(`${API_BASE_URL}/player/${id}/bio`)
        .then((r) => (r.ok ? r.json() : null))
        .then((b) => b && !cancelled && setNames((p) => ({ ...p, [id]: { name: b.name, position: b.position, team: b.team } })))
        .catch(() => {});
    });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);
  return names;
}

// ---------------------------------------------------------------------------
// One player
// ---------------------------------------------------------------------------

function Card({ label, value, sub, bar, color }: { label: string; value: string; sub: string; bar: number | null; color: string }) {
  // Every card has the same four rows (label, number, note, bar), so the bars line up across the row.
  return (
    <div className="rounded-lg border border-slate-200 dark:border-slate-700 p-3 min-w-0 flex flex-col" data-testid="usage-card">
      <div className="text-[10px] uppercase font-bold tracking-wider text-slate-400 truncate">{label}</div>
      <div className="text-2xl font-black text-slate-800 dark:text-slate-100 leading-tight">{value}</div>
      <div className="text-xs text-slate-500 dark:text-slate-400 mb-2 min-h-[1rem] truncate">{sub}</div>
      <div className="mt-auto"><Bar value={bar} color={color} /></div>
    </div>
  );
}

function SingleUsage({ data, playerId, teamColor, onSelectPlayer }: { data: Usage; playerId: string; teamColor: string; onSelectPlayer?: (id: string) => void }) {
  const s = data.summary!;
  const lead = leadFor(data.position);
  const snapSub = `${data.games.reduce((n, g) => n + (g.snaps ?? 0), 0)} snaps`;
  const inGames = `in his ${s.games} game${s.games === 1 ? '' : 's'}`;
  const cards =
    lead === 'pass_att'
      ? [
          { label: 'Pass attempts', value: pct(s.pass_att_share), sub: `${s.pass_att} of ${s.team_pass_att} team attempts ${inGames}`, bar: s.pass_att_share },
          { label: 'Carries', value: pct(s.carry_share), sub: `${s.carries} of ${s.team_carries} team carries ${inGames}`, bar: s.carry_share },
          { label: 'Snap share', value: pct(s.snap_pct), sub: snapSub, bar: s.snap_pct },
        ]
      : lead === 'carries'
        ? [
            { label: 'Carry share', value: pct(s.carry_share), sub: `${s.carries} of ${s.team_carries} team carries ${inGames}`, bar: s.carry_share },
            { label: 'Target share', value: pct(s.target_share), sub: `${s.targets} of ${s.team_targets} team targets ${inGames}`, bar: s.target_share },
            { label: 'Snap share', value: pct(s.snap_pct), sub: snapSub, bar: s.snap_pct },
            { label: 'Rushing yards share', value: pct(s.rush_yd_share), sub: 'of team rushing yards', bar: s.rush_yd_share },
          ]
        : [
            { label: 'Target share', value: pct(s.target_share), sub: `${s.targets} of ${s.team_targets} team targets ${inGames}`, bar: s.target_share },
            { label: 'Air yards share', value: pct(s.air_yd_share), sub: 'of team air yards', bar: s.air_yd_share },
            { label: 'Snap share', value: pct(s.snap_pct), sub: snapSub, bar: s.snap_pct },
            { label: 'Receiving yards share', value: pct(s.rec_yd_share), sub: 'of team receiving yards', bar: s.rec_yd_share },
          ];

  // A back is used as a runner and a receiver, so he gets both shares; a quarterback his throws.
  const shareCols: { label: string; of: (g: UsageGame) => number | null }[] =
    lead === 'pass_att'
      ? [{ label: 'Pass att share', of: (g) => g.pass_att_share }]
      : lead === 'carries'
        ? [{ label: 'Carry share', of: (g) => g.carry_share }, { label: 'Target share', of: (g) => g.target_share }]
        : [{ label: 'Target share', of: (g) => g.target_share }];

  const mateKey = lead === 'pass_att' ? 'pass_att_share' : lead === 'carries' ? 'carry_share' : 'target_share';
  const mateLabel = lead === 'pass_att' ? 'pass attempts' : lead === 'carries' ? 'carries' : 'targets';
  const maxMate = Math.max(1, ...data.teammates.map((m) => m[mateKey] ?? 0));
  const countKey = lead === 'pass_att' ? 'pass_att' : lead === 'carries' ? 'carries' : 'targets';
  const teamCount = data.team_totals?.[countKey];

  return (
    <>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        {cards.map((c) => <Card key={c.label} {...c} color={teamColor} />)}
      </div>

      <section>
        <h3 className="text-[11px] font-black uppercase tracking-wider text-slate-400 mb-2">Game by game</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm" data-testid="usage-games">
            <thead>
              <tr className="text-left text-[10px] uppercase tracking-wider text-slate-400">
                <th className="py-1.5 pr-3">Wk</th>
                <th className="pr-3">Opp</th>
                <th className="pr-3 text-right">Snaps</th>
                <th className="pr-3 text-right">Snap %</th>
                {lead !== 'targets' && <th className="pr-3 text-right">{lead === 'pass_att' ? 'Att' : 'Carries'}</th>}
                {lead !== 'pass_att' && <th className="pr-3 text-right">Targets</th>}
                {shareCols.map((c) => <th key={c.label} className="w-40">{c.label}</th>)}
              </tr>
            </thead>
            <tbody>
              {data.games.map((g) => {
                return (
                  <tr key={g.week} className="border-t border-slate-100 dark:border-slate-800">
                    <td className="py-1.5 pr-3 font-bold">{g.week}</td>
                    <td className="pr-3 text-slate-500">{g.opponent ?? '–'}</td>
                    <td className="pr-3 text-right tabular-nums">{g.snaps ?? '–'}</td>
                    <td className="pr-3 text-right tabular-nums">{pct(g.snap_pct)}</td>
                    {lead !== 'targets' && (
                      <td className="pr-3 text-right tabular-nums">
                        {lead === 'pass_att' ? `${g.pass_att}/${g.team_pass_att}` : `${g.carries}/${g.team_carries}`}
                      </td>
                    )}
                    {lead !== 'pass_att' && <td className="pr-3 text-right tabular-nums">{g.targets}/{g.team_targets}</td>}
                    {shareCols.map((c) => (
                      <td key={c.label} className="pr-3">
                        <div className="flex items-center gap-2">
                          <Bar value={c.of(g)} color={teamColor} />
                          <span className="w-12 text-right tabular-nums text-xs font-bold">{pct(c.of(g))}</span>
                        </div>
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      {data.teammates.length > 1 && (
        <section>
          <h3 className="text-[11px] font-black uppercase tracking-wider text-slate-400 mb-2">
            Pecking order · {data.position}s ranked by {mateLabel}
          </h3>
          <p className="text-[11px] text-slate-400 mb-2">
            Each bar is his share of ALL {teamCount ?? ''} {data.team} {mateLabel} this season, so teammates compare on the
            same scale. It reads lower than the cards above when a player missed games, because the cards only count the
            games he played.
          </p>
          <ol className="space-y-1.5" data-testid="usage-teammates">
            {data.teammates.map((m) => {
              const me = m.player_id === playerId;
              return (
                <li key={m.player_id} className="flex items-center gap-3 text-sm">
                  <span className="w-5 text-right text-xs text-slate-400 tabular-nums">{m.rank}</span>
                  <button
                    type="button"
                    disabled={me || !onSelectPlayer}
                    onClick={() => onSelectPlayer?.(m.player_id)}
                    className={`w-40 truncate text-left ${me ? 'font-black text-slate-900 dark:text-white' : 'text-slate-600 dark:text-slate-300 hover:underline disabled:no-underline'}`}
                  >
                    {m.name}
                  </button>
                  <div className="flex-1">
                    <Bar value={((m[mateKey] ?? 0) / maxMate) * 100} color={me ? teamColor : '#94a3b8'} height="h-2.5" />
                  </div>
                  <span className="w-28 text-right tabular-nums text-xs">
                    <b>{pct(m[mateKey])}</b> <span className="text-slate-400">· {m[countKey]} in {m.games} g</span>
                  </span>
                </li>
              );
            })}
          </ol>
        </section>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Several players
// ---------------------------------------------------------------------------

interface Metric {
  key: string;
  label: string;
  season: (s: Summary) => number | null;
  game?: (g: UsageGame) => number | null;
}

const METRICS: Metric[] = [
  { key: 'snap', label: 'Snap share', season: (s) => s.snap_pct, game: (g) => g.snap_pct },
  { key: 'carry', label: 'Carry share', season: (s) => s.carry_share, game: (g) => g.carry_share },
  { key: 'target', label: 'Target share', season: (s) => s.target_share, game: (g) => g.target_share },
  { key: 'pass', label: 'Pass attempt share', season: (s) => s.pass_att_share, game: (g) => g.pass_att_share },
  { key: 'rushyd', label: 'Rushing yards share', season: (s) => s.rush_yd_share },
  { key: 'recyd', label: 'Receiving yards share', season: (s) => s.rec_yd_share, game: (g) => g.rec_yd_share },
  { key: 'air', label: 'Air yards share', season: (s) => s.air_yd_share, game: (g) => g.air_yd_share },
];

interface Entry {
  id: string;
  name: string;
  position: string;
  team: string;
  color: string;
  usage: Usage | null;
}

function CompareUsage({ entries, onRemove }: { entries: Entry[]; onRemove?: (id: string) => void }) {
  const ready = entries.filter((e) => e.usage?.summary);
  // Only show a measure someone actually has: a running back's pass attempts would be a row of zeros.
  const shown = METRICS.filter((m) => m.key === 'snap' || ready.some((e) => (m.season(e.usage!.summary!) ?? 0) > 0));
  const weekly = shown.filter((m) => m.game);
  const [weeklyKey, setWeeklyKey] = useState<string>('');
  const active = weekly.find((m) => m.key === weeklyKey) ?? weekly.find((m) => m.key !== 'snap') ?? weekly[0];
  const weeks = [...new Set(ready.flatMap((e) => e.usage!.games.map((g) => g.week)))].sort((a, b) => a - b);

  return (
    <>
      <div className="flex flex-wrap gap-2" data-testid="usage-legend">
        {entries.map((e) => (
          <span key={e.id} className="inline-flex items-center gap-2 rounded-full border border-slate-200 dark:border-slate-700 pl-2.5 pr-1.5 py-1 text-xs">
            <span className="w-2.5 h-2.5 rounded-full" style={{ background: e.color }} />
            <b className="text-slate-800 dark:text-slate-100">{e.name}</b>
            <span className="text-slate-500">{e.position} · {e.team}{e.usage?.season ? ` · ${e.usage.season}` : ''}</span>
            {onRemove && e.id !== entries[0].id && (
              <button onClick={() => onRemove(e.id)} aria-label={`Remove ${e.name} from comparison`} className="p-0.5 rounded-full text-slate-400 hover:text-slate-700 dark:hover:text-white">
                <X size={12} />
              </button>
            )}
          </span>
        ))}
      </div>

      <section>
        <h3 className="text-[11px] font-black uppercase tracking-wider text-slate-400 mb-3">Season share of team</h3>
        <div className="space-y-4" data-testid="usage-compare-season">
          {shown.map((m) => (
            <div key={m.key}>
              <div className="text-xs font-bold text-slate-500 dark:text-slate-400 mb-1">{m.label}</div>
              <div className="space-y-1">
                {ready.map((e) => {
                  const v = m.season(e.usage!.summary!);
                  return (
                    <div key={e.id} className="flex items-center gap-3">
                      <div className="flex-1"><Bar value={v} color={e.color} height="h-2.5" /></div>
                      <span className="w-14 text-right tabular-nums text-xs font-black" style={{ color: e.color }}>{pct(v)}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      </section>

      {active && weeks.length > 0 && (
        <section>
          <div className="flex flex-wrap items-center gap-2 mb-3">
            <h3 className="text-[11px] font-black uppercase tracking-wider text-slate-400 mr-2">Week by week</h3>
            {weekly.map((m) => (
              <button
                key={m.key}
                onClick={() => setWeeklyKey(m.key)}
                className={`px-2.5 py-1 rounded-md text-xs font-black transition ${
                  active.key === m.key
                    ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm border border-slate-200 dark:border-slate-600'
                    : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'
                }`}
              >
                {m.label}
              </button>
            ))}
          </div>
          <div className="flex items-end gap-3 overflow-x-auto pb-1" data-testid="usage-compare-weekly">
            {weeks.map((w) => (
              <div key={w} className="flex flex-col items-center shrink-0">
                <div className="flex items-end gap-1 h-32">
                  {ready.map((e) => {
                    const g = e.usage!.games.find((x) => x.week === w);
                    const v = g ? active.game!(g) : null;
                    return (
                      <div key={e.id} className="flex flex-col items-center justify-end h-full w-5" title={`${e.name}, week ${w}: ${pct(v)}`}>
                        <span className="text-[9px] tabular-nums font-bold mb-0.5" style={{ color: e.color }}>{v == null ? '' : Math.round(v)}</span>
                        <div className="w-full rounded-t" style={{ height: `${Math.min(100, Math.max(0, v ?? 0)) * 0.9}%`, background: e.color }} />
                      </div>
                    );
                  })}
                </div>
                <span className="text-[10px] text-slate-400 mt-1">W{w}</span>
              </div>
            ))}
          </div>
        </section>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------

export default function PlayerUsage({ playerId, season, teamColor, onSelectPlayer, extraIds = [], onRemove }: Props) {
  const ids = [playerId, ...extraIds];
  const { data, error, loading } = useUsages(ids, season);
  const names = useNames(extraIds.length ? ids : []);

  if (loading && !data[playerId]) return <div className="p-20 text-center text-slate-400 dark:text-slate-500 animate-pulse font-bold">Loading usage...</div>;
  if (error) {
    return (
      <div className="p-10 text-center">
        <p className="text-sm font-black text-slate-700 dark:text-slate-200">Couldn't load usage</p>
        <p className="text-xs text-slate-400 mt-1">{error}</p>
      </div>
    );
  }
  const main = data[playerId];
  if (!main || !main.summary || main.games.length === 0) {
    return <div className="p-20 text-center text-slate-400 dark:text-slate-500 font-bold" data-testid="usage-empty">No usage data for this season yet</div>;
  }

  const compare = extraIds.length > 0;
  const entries: Entry[] = ids.map((id, i) => ({
    id,
    name: names[id]?.name ?? data[id]?.name ?? (id === playerId ? 'This player' : id),
    position: names[id]?.position ?? data[id]?.position ?? '',
    team: names[id]?.team ?? data[id]?.team ?? '',
    color: seriesColor(i),
    usage: data[id] ?? null,
  }));

  return (
    <div className="p-4 sm:p-6 space-y-6" data-testid="player-usage">
      <p className="text-xs text-slate-500 dark:text-slate-400">
        {compare ? `${main.season} · each player's share of his own team's totals.` : `${main.team} · ${main.season} · share of the team's totals over ${main.summary.games} game${main.summary.games === 1 ? '' : 's'}.`}{' '}
        Route participation isn't tracked in our data, so targets and snap share stand in for it.
      </p>
      {compare ? (
        <CompareUsage entries={entries} onRemove={onRemove} />
      ) : (
        <SingleUsage data={main} playerId={playerId} teamColor={teamColor} onSelectPlayer={onSelectPlayer} />
      )}
    </div>
  );
}
