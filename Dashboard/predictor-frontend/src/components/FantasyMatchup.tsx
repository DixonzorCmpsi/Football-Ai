/**
 * This week's fantasy matchup, side by side with the team you're playing.
 *
 * Reads top to bottom the way a manager plans a week: who's favored and by how
 * much, what needs fixing in your lineup, where each position group wins or
 * loses, then every slot head-to-head with the game script, Vegas line,
 * touchdown chance and defense matchup behind each projection.
 */

import React from 'react';
import { AlertTriangle, ArrowRight, Flame, Info, Link2, Swords } from 'lucide-react';
import { sizedPlayerImage } from '../utils/playerImage';

export interface MatchupPlayer {
  sleeper_id: string | null;
  slot: string;
  player_id?: string | null;
  player_name: string;
  position: string | null;
  team?: string | null;
  opponent?: string | null;
  home?: boolean | null;
  kickoff?: string | null;
  game_final?: boolean;
  bye?: boolean;
  empty?: boolean;
  image?: string | null;
  injury_status?: string | null;
  injury_flag?: string | null;
  projection: number;
  floor: number | null;
  ceiling: number | null;
  source: 'model' | 'sleeper' | null;
  live_points?: number | null;
  vegas?: { spread: number | null; total: number | null; implied_total: number | null; moneyline: string | null; source: string | null };
  script?: { label: string; tone: 'good' | 'bad' | 'neutral'; summary: string } | null;
  td?: { probability: number; source: 'bovada' | 'model' } | null;
  props?: { market: string; line: number; odds: string }[];
  defense_rank?: { rank: number; allowed: number; teams: number } | null;
}

export interface MatchupSide {
  roster_id: number;
  team_name: string;
  owner?: string | null;
  record: string;
  starters: MatchupPlayer[];
  projected_total: number;
  live_projected_total: number;
  points_so_far: number;
  sd: number;
  by_group: Record<string, number>;
}

export interface FantasyMatchupData {
  league: { league_id: string; name: string; week: number; scoring_type: string };
  you: MatchupSide;
  opponent: MatchupSide | null;
  message?: string;
  win_probability?: number;
  live_win_probability?: number;
  margin_sd?: number;
  group_edges?: { group: string; you: number; opponent: number; edge: number }[];
  shared_games?: { game: string; kickoff: string | null; you: string[]; opponent: string[] }[];
  swing_players?: { side: 'you' | 'opponent'; player_name: string; position: string; projection: number; floor: number; ceiling: number; td?: MatchupPlayer['td'] }[];
  bench_fixes?: { out: string; slot: string; reason: string; replace_with: { player_id: string; player_name: string; position: string; projection: number } | null }[];
  method?: string[];
}

/** Real points once the player's game is final, otherwise the projection. */
const shown = (p?: MatchupPlayer) => (p?.game_final && p.live_points != null ? p.live_points : p?.projection ?? 0);
const pct = (p: number) => `${Math.round(p * 100)}%`;
const signed = (n: number) => `${n > 0 ? '+' : ''}${n.toFixed(1)}`;

function kickoffLabel(k?: string | null) {
  if (!k) return null;
  const [day, time] = k.split(' ');
  const d = new Date(`${day}T00:00:00`);
  if (Number.isNaN(d.getTime())) return k;
  const wd = d.toLocaleDateString(undefined, { weekday: 'short' });
  if (!time) return wd;
  const [h, m] = time.split(':').map(Number);
  const hour = ((h + 11) % 12) + 1;
  return `${wd} ${hour}:${String(m).padStart(2, '0')}${h >= 12 ? 'p' : 'a'}`;
}

const toneClass: Record<string, string> = {
  good: 'bg-emerald-50 text-emerald-700 dark:bg-emerald-900/25 dark:text-emerald-300',
  bad: 'bg-rose-50 text-rose-700 dark:bg-rose-900/25 dark:text-rose-300',
  neutral: 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300',
};

/** Defense rank, read from the offense's side: rank 1 allows the most. */
function matchupTone(rank: number, teams: number) {
  if (rank <= Math.round(teams / 4)) return { tone: 'good', word: 'Soft' };
  if (rank > teams - Math.round(teams / 4)) return { tone: 'bad', word: 'Tough' };
  return { tone: 'neutral', word: 'Avg' };
}

function Chip({ tone = 'neutral', title, children }: { tone?: string; title?: string; children: React.ReactNode }) {
  return (
    <span title={title} className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] font-bold whitespace-nowrap ${toneClass[tone]}`}>
      {children}
    </span>
  );
}

/** Floor-to-ceiling bar on a shared 0..scaleMax axis, with the projection marked. */
function RangeBar({ player, scaleMax, align }: { player: MatchupPlayer; scaleMax: number; align: 'left' | 'right' }) {
  if (player.floor == null || player.ceiling == null) return null;
  const pos = (v: number) => `${Math.min(100, (v / scaleMax) * 100)}%`;
  const style = align === 'left'
    ? { left: pos(player.floor), width: `calc(${pos(player.ceiling)} - ${pos(player.floor)})` }
    : { right: pos(player.floor), width: `calc(${pos(player.ceiling)} - ${pos(player.floor)})` };
  const mark = align === 'left' ? { left: pos(player.projection) } : { right: pos(player.projection) };
  return (
    <div className="relative h-1.5 rounded-full bg-slate-100 dark:bg-slate-800" title={`Range ${player.floor}–${player.ceiling} (10th–90th percentile)`}>
      <div className="absolute inset-y-0 rounded-full bg-blue-200 dark:bg-blue-900/70" style={style} />
      <div className="absolute -top-0.5 h-2.5 w-0.5 rounded bg-blue-600 dark:bg-blue-400" style={mark} />
    </div>
  );
}

function PlayerCell({ player, align, scaleMax, onOpen }: {
  player: MatchupPlayer; align: 'left' | 'right'; scaleMax: number; onOpen?: (id: string) => void;
}) {
  const right = align === 'right';
  if (player.empty) {
    return <div className={`p-3 text-sm font-bold text-rose-600 dark:text-rose-400 ${right ? 'text-right' : ''}`}>Empty slot</div>;
  }
  const dr = player.defense_rank;
  const drTone = dr ? matchupTone(dr.rank, dr.teams) : null;
  const clickable = !!(player.player_id && onOpen && player.source === 'model');
  const shownPoints = shown(player);
  return (
    <div className={`p-3 flex flex-col gap-1.5 min-w-0 ${right ? 'items-end text-right' : ''}`} data-testid="matchup-player">
      <div className={`flex items-center gap-2 min-w-0 w-full ${right ? 'flex-row-reverse' : ''}`}>
        <div className="w-8 h-8 rounded-full overflow-hidden bg-slate-100 dark:bg-slate-800 shrink-0 flex items-center justify-center">
          {player.image
            ? <img src={sizedPlayerImage(player.image, 32)} alt="" width={32} height={32} loading="lazy" className="w-full h-full object-cover" />
            : <span className="text-[9px] font-bold text-slate-400">{player.position}</span>}
        </div>
        <div className={`min-w-0 flex-1 ${right ? 'text-right' : ''}`}>
          <button
            type="button"
            disabled={!clickable}
            onClick={() => clickable && onOpen!(player.player_id!)}
            className="block max-w-full truncate font-bold text-sm text-slate-800 dark:text-slate-100 enabled:hover:text-blue-600 dark:enabled:hover:text-blue-400"
          >
            {player.player_name}
          </button>
          <div className="text-[11px] text-slate-500 truncate">
            {player.position} · {player.team ?? '—'}{' '}
            {player.bye ? <span className="font-bold text-rose-600">BYE</span> : <>{player.home === false ? '@' : 'vs'} {player.opponent}</>}
            {kickoffLabel(player.kickoff) && <span className="text-slate-400"> · {kickoffLabel(player.kickoff)}</span>}
          </div>
        </div>
        <div className="shrink-0 tabular-nums">
          <div className="text-lg font-black leading-none text-slate-800 dark:text-slate-100">{shownPoints.toFixed(1)}</div>
          <div className="text-[9px] font-bold uppercase tracking-wider text-slate-400">
            {player.game_final ? 'final' : player.source === 'sleeper' ? 'Sleeper' : 'proj'}
          </div>
        </div>
      </div>

      <div className="w-full"><RangeBar player={player} scaleMax={scaleMax} align={align} /></div>

      <div className={`flex flex-wrap gap-1 ${right ? 'justify-end' : ''}`}>
        {player.injury_flag && <Chip tone="bad" title={player.injury_status ?? undefined}><AlertTriangle size={9} />{player.injury_flag}</Chip>}
        {player.script && <Chip tone={player.script.tone} title={player.script.summary}>Script: {player.script.label}</Chip>}
        {player.vegas?.implied_total != null && (
          <Chip title={`Spread ${player.vegas.spread != null ? signed(player.vegas.spread) : '—'} · O/U ${player.vegas.total ?? '—'}${player.vegas.source === 'schedule' ? ' (schedule line)' : ''}`}>
            {player.vegas.spread != null ? signed(player.vegas.spread) : ''} · O/U {player.vegas.total ?? '—'} · TT {player.vegas.implied_total}
          </Chip>
        )}
        {player.td && (
          <Chip tone={player.td.probability >= 0.4 ? 'good' : 'neutral'} title={player.td.source === 'bovada' ? 'Anytime TD, Bovada implied' : 'Model estimate: recent TD rate × team implied points'}>
            TD {pct(player.td.probability)}{player.td.source === 'model' ? '*' : ''}
          </Chip>
        )}
        {dr && drTone && (
          <Chip tone={drTone.tone} title={`${player.opponent} allows ${dr.allowed} PPR pts/game to ${player.position}s, #${dr.rank} of ${dr.teams} (1 = most)`}>
            {drTone.word} D #{dr.rank}
          </Chip>
        )}
        {(player.props || []).slice(0, 2).map((p) => (
          <Chip key={p.market} title={`${p.market} over ${p.line} (${p.odds})`}>{shortMarket(p.market)} o{p.line}</Chip>
        ))}
      </div>
    </div>
  );
}

function shortMarket(m: string) {
  return m.replace('Receiving Yards', 'Rec yds').replace('Rushing Yards', 'Rush yds').replace('Passing Yards', 'Pass yds')
    .replace('Receptions', 'Rec').replace('Passing Touchdowns', 'Pass TD').replace('Passing TDs', 'Pass TD');
}

export default function FantasyMatchup({ data, onOpenHistory }: { data: FantasyMatchupData; onOpenHistory?: (id: string) => void }) {
  const { you, opponent } = data;
  const fixes = data.bench_fixes || [];

  if (!opponent) {
    return (
      <div className="space-y-3" data-testid="fantasy-matchup">
        <div className="p-4 rounded-lg border border-dashed border-slate-300 dark:border-slate-700 text-sm text-slate-500">{data.message}</div>
        <Fixes fixes={fixes} />
      </div>
    );
  }

  const winP = data.live_win_probability ?? data.win_probability ?? 0.5;
  const started = [...you.starters, ...opponent.starters].some((p) => p.game_final);
  const scaleMax = Math.max(20, ...[...you.starters, ...opponent.starters].map((p) => p.ceiling ?? p.projection));
  const rows = Math.max(you.starters.length, opponent.starters.length);
  const edges = data.group_edges || [];
  const edgeMax = Math.max(1, ...edges.map((e) => Math.abs(e.edge)));

  return (
    <div className="space-y-5" data-testid="fantasy-matchup">
      {/* Scoreboard */}
      <section className="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 p-4">
        <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-3">
          <TeamScore side={you} label="You" />
          <div className="flex flex-col items-center text-slate-400"><Swords size={18} /><span className="text-[10px] font-bold uppercase tracking-widest">Wk {data.league.week}</span></div>
          <TeamScore side={opponent} label="Opponent" right />
        </div>
        <div className="mt-4">
          <div className="flex justify-between text-[11px] font-bold mb-1">
            <span className="text-blue-600 dark:text-blue-400" data-testid="matchup-win-probability">{pct(winP)} to win</span>
            <span className="text-slate-400">{started ? 'live · final games use real points' : `margin ${signed(you.projected_total - opponent.projected_total)} ± ${data.margin_sd?.toFixed(0)}`}</span>
            <span className="text-rose-500">{pct(1 - winP)}</span>
          </div>
          <div className="h-2.5 rounded-full bg-rose-400/80 dark:bg-rose-500/60 overflow-hidden">
            <div className="h-full bg-blue-600 dark:bg-blue-500 transition-[width]" style={{ width: pct(winP) }} />
          </div>
        </div>
      </section>

      <Fixes fixes={fixes} />

      <div className="grid gap-4 lg:grid-cols-2">
        {/* Position edges */}
        <section className="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 p-4">
          <h3 className="text-[11px] font-black uppercase tracking-wider text-slate-500 mb-3">Where you win and lose</h3>
          <div className="space-y-1.5">
            {edges.map((e) => (
              <div key={e.group} className="grid grid-cols-[3rem_1fr_3.5rem] items-center gap-2 text-xs" data-testid="matchup-edge">
                <span className="font-mono font-bold text-slate-500">{e.group}</span>
                <div className="relative h-4">
                  <div className="absolute inset-y-0 left-1/2 w-px bg-slate-300 dark:bg-slate-600" />
                  <div
                    className={`absolute inset-y-0.5 rounded ${e.edge >= 0 ? 'bg-blue-500 left-1/2' : 'bg-rose-400 right-1/2'}`}
                    style={{ width: `${(Math.abs(e.edge) / edgeMax) * 50}%` }}
                  />
                </div>
                <span className={`text-right font-black tabular-nums ${e.edge >= 0 ? 'text-blue-600 dark:text-blue-400' : 'text-rose-500'}`}>{signed(e.edge)}</span>
              </div>
            ))}
          </div>
          <div className="mt-2 flex justify-between text-[10px] text-slate-400"><span>← opponent stronger</span><span>you stronger →</span></div>
        </section>

        {/* Swing players and shared games */}
        <section className="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 p-4 space-y-4">
          <div>
            <h3 className="text-[11px] font-black uppercase tracking-wider text-slate-500 mb-2 flex items-center gap-1.5"><Flame size={12} className="text-orange-500" /> Swing players</h3>
            <ul className="space-y-1.5">
              {(data.swing_players || []).map((s) => (
                <li key={`${s.side}-${s.player_name}`} className="flex items-center gap-2 text-xs">
                  <span className={`w-1.5 h-1.5 rounded-full ${s.side === 'you' ? 'bg-blue-500' : 'bg-rose-400'}`} />
                  <span className="font-bold text-slate-700 dark:text-slate-200 truncate">{s.player_name}</span>
                  <span className="text-slate-400">{s.position}</span>
                  <span className="ml-auto tabular-nums text-slate-500">{s.floor}–{s.ceiling}</span>
                  {s.td && <span className="tabular-nums text-slate-400 w-12 text-right">TD {pct(s.td.probability)}</span>}
                </li>
              ))}
            </ul>
            <p className="mt-1.5 text-[10px] text-slate-400">Widest ranges on either side: the week most likely turns on these.</p>
          </div>
          {(data.shared_games || []).length > 0 && (
            <div>
              <h3 className="text-[11px] font-black uppercase tracking-wider text-slate-500 mb-2 flex items-center gap-1.5"><Link2 size={12} /> Same game, both sides</h3>
              <ul className="space-y-1 text-xs">
                {data.shared_games!.map((g) => (
                  <li key={g.game} className="text-slate-600 dark:text-slate-300">
                    <span className="font-bold">{g.game}</span>
                    {kickoffLabel(g.kickoff) && <span className="text-slate-400"> · {kickoffLabel(g.kickoff)}</span>}
                    <span className="block text-[11px] text-slate-500">
                      <span className="text-blue-600 dark:text-blue-400">{g.you.join(', ')}</span> vs <span className="text-rose-500">{g.opponent.join(', ')}</span>
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </section>
      </div>

      {/* Slot by slot */}
      <section className="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 overflow-hidden">
        <div className="grid grid-cols-[1fr_3.25rem_1fr] border-b border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/60 text-[10px] font-black uppercase tracking-wider text-slate-500">
          <div className="px-3 py-2 truncate">{you.team_name}</div>
          <div className="py-2 text-center">Slot</div>
          <div className="px-3 py-2 text-right truncate">{opponent.team_name}</div>
        </div>
        {Array.from({ length: rows }, (_, i) => {
          const mine = you.starters[i];
          const theirs = opponent.starters[i];
          const diff = shown(mine) - shown(theirs);
          return (
            <div key={i} className="grid grid-cols-1 md:grid-cols-[1fr_3.25rem_1fr] border-b last:border-b-0 border-slate-100 dark:border-slate-800" data-testid="matchup-slot-row">
              <div className={diff > 0 ? 'bg-blue-50/50 dark:bg-blue-950/20' : ''}>{mine && <PlayerCell player={mine} align="left" scaleMax={scaleMax} onOpen={onOpenHistory} />}</div>
              <div className="flex md:flex-col items-center justify-center gap-2 py-1 md:py-0 border-y md:border-y-0 md:border-x border-slate-100 dark:border-slate-800">
                <span className="font-mono text-[11px] font-black text-slate-500">{mine?.slot ?? theirs?.slot}</span>
                <span className={`text-[10px] font-black tabular-nums ${diff >= 0 ? 'text-blue-600 dark:text-blue-400' : 'text-rose-500'}`}>{signed(diff)}</span>
              </div>
              <div className={diff < 0 ? 'bg-rose-50/50 dark:bg-rose-950/20' : ''}>{theirs && <PlayerCell player={theirs} align="right" scaleMax={scaleMax} onOpen={onOpenHistory} />}</div>
            </div>
          );
        })}
      </section>

      {data.method && (
        <details className="text-[11px] text-slate-500">
          <summary className="cursor-pointer font-bold inline-flex items-center gap-1"><Info size={11} /> How these numbers are made</summary>
          <ul className="mt-1.5 ml-4 list-disc space-y-0.5">
            {data.method.map((m) => <li key={m}>{m}</li>)}
            <li>* marks a model touchdown estimate where no book price is posted yet.</li>
          </ul>
        </details>
      )}
    </div>
  );
}

function TeamScore({ side, label, right }: { side: MatchupSide; label: string; right?: boolean }) {
  return (
    <div className={`min-w-0 ${right ? 'text-right' : ''}`}>
      <div className={`text-[10px] font-black uppercase tracking-widest ${right ? 'text-rose-500' : 'text-blue-600 dark:text-blue-400'}`}>{label}</div>
      <div className="font-black text-slate-800 dark:text-slate-100 truncate" title={side.team_name}>{side.team_name}</div>
      <div className="text-[11px] text-slate-500 truncate">{side.owner} · {side.record}</div>
      <div className="mt-1 text-3xl font-black tabular-nums text-slate-800 dark:text-slate-100">{side.projected_total.toFixed(1)}</div>
      <div className="text-[10px] text-slate-400 tabular-nums">
        projected{side.points_so_far > 0 ? ` · ${side.points_so_far.toFixed(1)} scored` : ''}
      </div>
    </div>
  );
}

function Fixes({ fixes }: { fixes: NonNullable<FantasyMatchupData['bench_fixes']> }) {
  if (!fixes.length) return null;
  return (
    <section className="rounded-xl border border-amber-300 dark:border-amber-700/60 bg-amber-50 dark:bg-amber-900/15 p-3" data-testid="matchup-fixes">
      <h3 className="text-[11px] font-black uppercase tracking-wider text-amber-700 dark:text-amber-400 mb-1.5 flex items-center gap-1.5">
        <AlertTriangle size={12} /> Fix before kickoff
      </h3>
      <ul className="space-y-1 text-sm">
        {fixes.map((f) => (
          <li key={`${f.slot}-${f.out}`} className="flex flex-wrap items-center gap-1.5 text-slate-700 dark:text-slate-200">
            <span className="font-bold">{f.out}</span>
            <span className="text-xs text-slate-500">({f.slot}, {f.reason})</span>
            {f.replace_with ? (
              <>
                <ArrowRight size={12} className="text-slate-400" />
                <span className="font-bold text-emerald-700 dark:text-emerald-400">{f.replace_with.player_name}</span>
                <span className="text-xs text-slate-500 tabular-nums">{f.replace_with.projection.toFixed(1)} proj</span>
              </>
            ) : <span className="text-xs text-slate-500">no healthy bench player fits this slot</span>}
          </li>
        ))}
      </ul>
    </section>
  );
}
