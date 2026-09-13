/**
 * Head-to-head in a popup, opened from the My Team roster.
 *
 * The verdict and its reasons come from the backend's own numbers
 * (services/player_compare.py), so they load with the table and cost nothing.
 * "Ask the assistant" hands the same pair to the agent for a fuller take.
 */

import { useEffect, useMemo, useState } from 'react';
import { AlertTriangle, Loader2, MessageSquare, Plus, Scale, Search, X } from 'lucide-react';
import { API_BASE_URL, searchPlayers } from '../lib/api';
import { useAgentChatContext } from '../contexts/AgentChatContext';
import { sizedPlayerImage } from '../utils/playerImage';
import { getTeamColor } from '../utils/nflColors';

const MAX_PLAYERS = 4;

type Metrics = {
  projection: number | null;
  floor: number | null;
  season_avg: number | null;
  recent_form: number | null;
  model_adjustment: number | null;
  implied_total: number | null;
  prop_line: number | null;
  prop_label: string | null;
  anytime_td_prob: number | null;
  snap_pct: number | null;
};

type CompareCard = {
  player_id: string;
  player_name: string;
  position: string;
  team: string;
  opponent?: string | null;
  image?: string | null;
  injury_status?: string;
  metrics: Metrics;
};

type Verdict = {
  leader_id: string | null;
  runner_up_id?: string | null;
  confidence: 'clear' | 'lean' | 'toss-up' | 'risky' | null;
  headline: string;
  reasons: string[];
  counterpoints: string[];
  notes?: string[];
};

type CompareResponse = { week: number; players: CompareCard[]; missing: string[]; verdict: Verdict };

const ROWS: { key: keyof Metrics; label: string; fmt: (v: number) => string; hint?: string }[] = [
  { key: 'projection', label: 'Projection', fmt: (v) => v.toFixed(1) },
  { key: 'floor', label: 'Floor', fmt: (v) => v.toFixed(1) },
  { key: 'season_avg', label: 'Season avg', fmt: (v) => v.toFixed(1), hint: 'Every game this season, zeros included; last season early on' },
  { key: 'recent_form', label: 'Recent form', fmt: (v) => v.toFixed(1), hint: 'Last four games with points' },
  { key: 'model_adjustment', label: 'Model adj.', fmt: (v) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}`, hint: "The model's read on this week's setup" },
  { key: 'implied_total', label: 'Team implied pts', fmt: (v) => v.toFixed(1) },
  { key: 'prop_line', label: 'Market yards line', fmt: (v) => v.toFixed(1) },
  { key: 'anytime_td_prob', label: 'Anytime TD', fmt: (v) => `${v.toFixed(0)}%` },
  { key: 'snap_pct', label: 'Snap share', fmt: (v) => `${v.toFixed(0)}%` },
];

const CONFIDENCE_STYLE: Record<string, string> = {
  clear: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300',
  lean: 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300',
  'toss-up': 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300',
  risky: 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300',
};

function AddPlayer({ existing, onAdd }: { existing: string[]; onAdd: (id: string) => void }) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<{ player_id: string; player_name: string; position: string; team_abbr: string }[]>([]);

  const shown = query.trim().length >= 2 ? results : [];

  useEffect(() => {
    if (query.trim().length < 2) return;
    const timer = setTimeout(() => {
      searchPlayers(query.trim(), 'skill').then((r) => setResults((r || []).slice(0, 6))).catch(() => setResults([]));
    }, 250);
    return () => clearTimeout(timer);
  }, [query]);

  return (
    <div className="relative">
      <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg border border-dashed border-slate-300 dark:border-slate-600">
        <Search size={13} className="text-slate-400 shrink-0" />
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Add any player…"
          data-testid="compare-add-input"
          className="w-full bg-transparent text-[12px] outline-none placeholder:text-slate-400"
        />
      </div>
      {shown.length > 0 && (
        <div className="absolute z-10 mt-1 w-full rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 shadow-xl overflow-hidden">
          {shown.map((p) => (
            <button
              key={p.player_id}
              type="button"
              disabled={existing.includes(p.player_id)}
              onClick={() => { onAdd(p.player_id); setQuery(''); setResults([]); }}
              className="w-full text-left px-3 py-2 text-[12px] flex items-center justify-between hover:bg-slate-50 dark:hover:bg-slate-700 disabled:opacity-40"
            >
              <span className="font-bold text-slate-700 dark:text-slate-200">{p.player_name}</span>
              <span className="text-[10px] text-slate-400">{p.position} · {p.team_abbr}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export default function PlayerCompareModal({
  week,
  initialIds,
  onClose,
  onOpenHistory,
}: {
  week: number;
  initialIds: string[];
  onClose: () => void;
  onOpenHistory?: (id: string) => void;
}) {
  const { ask, openDock } = useAgentChatContext();
  const [ids, setIds] = useState<string[]>(initialIds.slice(0, MAX_PLAYERS));
  const [data, setData] = useState<CompareResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Under two players the view shows a prompt; columns come from `ids`, so stale data is harmless.
    if (ids.length < 2) return;
    let cancelled = false;
    Promise.resolve().then(() => { if (!cancelled) { setLoading(true); setError(null); } });
    fetch(`${API_BASE_URL}/compare/players?ids=${ids.map(encodeURIComponent).join(',')}${week ? `&week=${week}` : ''}`)
      .then(async (r) => {
        const body = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(body.detail || `HTTP ${r.status}`);
        return body as CompareResponse;
      })
      .then((d) => !cancelled && setData(d))
      .catch((e) => !cancelled && setError(String(e.message || e)))
      .finally(() => !cancelled && setLoading(false));
    return () => { cancelled = true; };
  }, [ids, week]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  // Keep the chosen order, not the server's ranking, so columns don't jump when one is added.
  const players = useMemo(() => {
    if (!data) return [];
    const byId = new Map(data.players.map((p) => [p.player_id, p]));
    return ids.map((id) => byId.get(id)).filter(Boolean) as CompareCard[];
  }, [data, ids]);

  const best = useMemo(() => {
    const out: Partial<Record<keyof Metrics, number>> = {};
    for (const row of ROWS) {
      const values = players.map((p) => p.metrics[row.key]).filter((v): v is number => typeof v === 'number');
      if (values.length >= 2 && new Set(values).size > 1) out[row.key] = Math.max(...values);
    }
    return out;
  }, [players]);

  const verdict = data?.verdict;
  const leader = players.find((p) => p.player_id === verdict?.leader_id);
  const runnerUp = players.find((p) => p.player_id === verdict?.runner_up_id);

  const askAssistant = () => {
    const names = players.map((p) => `${p.player_name} (${p.position}, ${p.team})`).join(' vs ');
    ask(`Week ${data?.week ?? week}: ${names}. Who should I start and why? Weigh matchup, usage, injuries and recent news.`);
    openDock('chat');
    onClose();
  };

  const prettyPropLabel = players.find((p) => p.metrics.prop_label)?.metrics.prop_label;

  return (
    <div className="fixed inset-0 z-[60] flex items-end sm:items-center justify-center bg-slate-900/50 backdrop-blur-sm sm:p-6" onClick={onClose} data-testid="compare-modal">
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Compare players"
        onClick={(e) => e.stopPropagation()}
        className="w-full sm:max-w-4xl max-h-[92vh] overflow-y-auto rounded-t-2xl sm:rounded-2xl bg-white dark:bg-slate-800 shadow-2xl border border-slate-200 dark:border-slate-700"
      >
        <div className="sticky top-0 z-10 flex items-center justify-between px-5 py-3 border-b border-slate-100 dark:border-slate-700 bg-white/95 dark:bg-slate-800/95 backdrop-blur">
          <div className="flex items-center gap-2">
            <Scale size={16} className="text-blue-600 dark:text-blue-400" />
            <h2 className="text-sm font-black uppercase tracking-widest text-slate-700 dark:text-slate-200">Compare</h2>
            <span className="text-[11px] text-slate-400">Week {data?.week ?? week}</span>
          </div>
          <button type="button" onClick={onClose} aria-label="Close" className="p-1 rounded-lg text-slate-400 hover:text-slate-700 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-700">
            <X size={16} />
          </button>
        </div>

        <div className="p-5 space-y-5">
          {/* Verdict */}
          {ids.length < 2 ? (
            <p className="text-sm text-slate-500">Add at least one more player to compare.</p>
          ) : error ? (
            <p className="text-sm text-red-500">Couldn't compare: {error}</p>
          ) : !verdict || loading ? (
            <div className="flex items-center justify-center gap-2 py-6 text-sm text-slate-500">
              <Loader2 size={16} className="animate-spin" /> Comparing…
            </div>
          ) : (
            <div className="rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900/40 p-4 space-y-3" data-testid="compare-verdict" data-confidence={verdict.confidence ?? ''}>
              <div className="flex flex-wrap items-center gap-2">
                {verdict.confidence && (
                  <span className={`text-[10px] font-black uppercase tracking-widest px-2 py-0.5 rounded ${CONFIDENCE_STYLE[verdict.confidence]}`}>
                    {verdict.confidence}
                  </span>
                )}
                <p className="text-[14px] font-bold text-slate-800 dark:text-slate-100">{verdict.headline}</p>
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                {verdict.reasons.length > 0 && (
                  <div>
                    <div className="text-[10px] font-black uppercase tracking-widest text-emerald-600 dark:text-emerald-400 mb-1">
                      Why {leader?.player_name ?? 'the leader'}
                    </div>
                    <ul className="space-y-1">
                      {verdict.reasons.map((r) => (
                        <li key={r} className="text-[12px] text-slate-600 dark:text-slate-300 flex gap-2">
                          <span className="mt-1.5 w-1 h-1 rounded-full bg-emerald-500 shrink-0" />
                          <span>{r.charAt(0).toUpperCase() + r.slice(1)}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {verdict.counterpoints.length > 0 && (
                  <div>
                    <div className="text-[10px] font-black uppercase tracking-widest text-amber-600 dark:text-amber-400 mb-1">
                      In {runnerUp?.player_name ?? 'the other'}’s favor
                    </div>
                    <ul className="space-y-1">
                      {verdict.counterpoints.map((r) => (
                        <li key={r} className="text-[12px] text-slate-600 dark:text-slate-300 flex gap-2">
                          <span className="mt-1.5 w-1 h-1 rounded-full bg-amber-500 shrink-0" />
                          <span>{r.charAt(0).toUpperCase() + r.slice(1)}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
              {verdict.reasons.length === 0 && verdict.counterpoints.length === 0 && (
                <p className="text-[12px] text-slate-500">Nothing in the numbers separates them beyond the projection itself.</p>
              )}
              {(verdict.notes || []).map((n) => (
                <p key={n} className="text-[11px] text-slate-500 flex items-center gap-1.5"><AlertTriangle size={11} /> {n}</p>
              ))}

              <div className="flex flex-wrap items-center gap-3 pt-1">
                <button type="button" onClick={askAssistant} data-testid="compare-ask-ai" className="inline-flex items-center gap-1.5 text-[12px] font-bold px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700">
                  <MessageSquare size={13} /> Ask the assistant for a deeper take
                </button>
                <span className="text-[10px] text-slate-400">Projections typically miss by about 6 points, so small gaps are close calls.</span>
              </div>
            </div>
          )}

          {/* Table */}
          <div className="overflow-x-auto -mx-5 px-5">
            <table className="w-full min-w-[520px] text-[12px] border-separate border-spacing-0">
              <thead>
                <tr>
                  <th className="w-32" />
                  {ids.map((id) => {
                    const p = players.find((x) => x.player_id === id);
                    const isLeader = p && p.player_id === verdict?.leader_id;
                    return (
                      <th key={id} className="align-top pb-3 px-1.5 font-normal">
                        <div className={`relative rounded-xl p-2.5 text-left border ${isLeader ? 'border-emerald-400 dark:border-emerald-600 bg-emerald-50/60 dark:bg-emerald-900/10' : 'border-slate-200 dark:border-slate-700'}`}>
                          <button
                            type="button"
                            onClick={() => setIds((prev) => prev.filter((x) => x !== id))}
                            aria-label="Remove from comparison"
                            className="absolute top-1.5 right-1.5 p-0.5 rounded text-slate-300 hover:text-red-500"
                          >
                            <X size={12} />
                          </button>
                          {p ? (
                            <button type="button" onClick={() => onOpenHistory?.(p.player_id)} className="flex items-center gap-2 text-left w-full pr-4">
                              <div className="w-9 h-9 rounded-full overflow-hidden bg-slate-100 dark:bg-slate-700 shrink-0 border-2" style={{ borderColor: getTeamColor(p.team) }}>
                                {p.image && <img src={sizedPlayerImage(p.image, 36)} alt="" className="w-full h-full object-cover" />}
                              </div>
                              <div className="min-w-0">
                                <div className="font-black text-slate-800 dark:text-slate-100 truncate">{p.player_name}</div>
                                <div className="text-[10px] text-slate-400">
                                  {p.position} · {p.team}{p.opponent ? ` vs ${p.opponent}` : ''}
                                </div>
                                {p.injury_status && !/^act(ive)?$/i.test(p.injury_status) && (
                                  <div className="text-[10px] font-black uppercase text-red-500">{p.injury_status}</div>
                                )}
                              </div>
                            </button>
                          ) : (
                            <div className="h-9 flex items-center text-slate-400"><Loader2 size={14} className="animate-spin" /></div>
                          )}
                        </div>
                      </th>
                    );
                  })}
                  {ids.length < MAX_PLAYERS && (
                    <th className="align-top pb-3 px-1.5 font-normal w-44">
                      <AddPlayer existing={ids} onAdd={(id) => setIds((prev) => [...prev, id].slice(0, MAX_PLAYERS))} />
                    </th>
                  )}
                </tr>
              </thead>
              <tbody>
                {players.length > 0 && ROWS.map((row) => {
                  if (players.every((p) => p.metrics[row.key] === null || p.metrics[row.key] === undefined)) return null;
                  const label = row.key === 'prop_line' && prettyPropLabel ? `Market ${prettyPropLabel}` : row.label;
                  return (
                    <tr key={row.key}>
                      <td className="py-1.5 pr-2 text-[11px] font-bold text-slate-500 border-t border-slate-100 dark:border-slate-700/60" title={row.hint}>
                        {label}
                      </td>
                      {ids.map((id) => {
                        const p = players.find((x) => x.player_id === id);
                        const v = p?.metrics[row.key];
                        const isBest = typeof v === 'number' && best[row.key] === v;
                        return (
                          <td key={id} className={`py-1.5 px-3 tabular-nums border-t border-slate-100 dark:border-slate-700/60 ${row.key === 'projection' ? 'text-[15px] font-black' : ''} ${isBest ? 'text-emerald-600 dark:text-emerald-400 font-bold' : 'text-slate-700 dark:text-slate-200'}`}>
                            {typeof v === 'number' ? row.fmt(v) : '–'}
                          </td>
                        );
                      })}
                      {ids.length < MAX_PLAYERS && <td className="border-t border-slate-100 dark:border-slate-700/60" />}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {ids.length < MAX_PLAYERS && ids.length >= 2 && (
            <p className="text-[10px] text-slate-400 flex items-center gap-1"><Plus size={10} /> Add a free agent or anyone else to see how they stack up.</p>
          )}
        </div>
      </div>
    </div>
  );
}
