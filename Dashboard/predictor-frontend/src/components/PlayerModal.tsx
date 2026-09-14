import React, { useEffect, useMemo, useState } from 'react';
import { ChevronLeft, ChevronRight, LineChart, Newspaper, Table2, TrendingUp, X } from 'lucide-react';
import { getTeamColor } from '../utils/nflColors';
import type { PlayerData } from '../types';
import { sizedPlayerImage } from '../utils/playerImage';
import { usePlayerHistory } from '../hooks/useNflData';
import { defaultSeason, groupBySeason, weekLabel } from '../utils/playerSeasons';
import { formatOdds, groupProps } from '../utils/playerProps';
import PlayerPerformanceCharts from './PlayerPerformanceCharts';
import PlayerStorylines from './PlayerStorylines';

interface PlayerModalProps {
  player: PlayerData | null;
  onClose: () => void;
  /** Controlled tab, so a link or the assistant can open the card on stats, charts, news or odds. */
  tab?: Tab;
  onTabChange?: (tab: Tab) => void;
}

export type Tab = 'log' | 'visuals' | 'storylines' | 'vegas';

const TABS: { id: Tab; label: string; icon: React.ReactNode }[] = [
  { id: 'log', label: 'Game Log', icon: <Table2 size={13} /> },
  { id: 'visuals', label: 'Visuals', icon: <LineChart size={13} /> },
  { id: 'storylines', label: 'Storylines', icon: <Newspaper size={13} /> },
  { id: 'vegas', label: 'Vegas', icon: <TrendingUp size={13} /> },
];

const fmtSigned = (n: number | null | undefined) => (n === null || n === undefined ? '-' : n > 0 ? `+${n}` : `${n}`);

const PlayerModal: React.FC<PlayerModalProps> = ({ player, onClose, tab: tabProp, onTabChange }) => {
  const [ownTab, setOwnTab] = useState<Tab>('log');
  const tab = tabProp ?? ownTab;
  const setTab = (next: Tab) => {
    setOwnTab(next);
    onTabChange?.(next);
  };
  const { history, loadingHistory } = usePlayerHistory(player?.player_id ?? null);

  // Grouped by season, each in week order. The API is newest-first across
  // seasons, which read as "W1, W22, W21 ..." when shown as one list.
  const seasons = useMemo(() => groupBySeason(history), [history]);
  // The user's pick, if it's one of this player's seasons; otherwise the default.
  const [picked, setSeason] = useState<number | null>(null);
  const season = seasons.some((s) => s.season === picked) ? picked : defaultSeason(seasons);
  const idx = seasons.findIndex((s) => s.season === season);
  const rows = idx >= 0 ? seasons[idx].rows : [];

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      // A story popup opened from this card sits on top; Escape closes that one first.
      if (document.querySelectorAll('[role="dialog"]').length > 1) return;
      onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const markets = useMemo(() => groupProps(player?.props), [player?.props]);
  const [showAlternates, setShowAlternates] = useState(false);
  const alternates = markets.filter((m) => m.alternate).length;
  const shownMarkets = showAlternates ? markets : markets.filter((m) => !m.alternate);
  // Props saved by the old page scrape (before Sep 2026) only ever had the over.
  const hasUnders = markets.some((m) => m.under);

  if (!player) return null;
  const teamColor = getTeamColor(player.team);
  const seasonAvg = (() => {
    const played = rows.filter((r) => (r.points || 0) > 0);
    return played.length ? played.reduce((a, r) => a + (r.points || 0), 0) / played.length : 0;
  })();

  const seasonPicker = seasons.length > 0 && (
    <div className="flex items-center gap-1.5 px-4 py-2 border-b border-slate-100 dark:border-slate-800 flex-wrap">
      <button
        type="button"
        onClick={() => idx < seasons.length - 1 && setSeason(seasons[idx + 1].season)}
        disabled={idx >= seasons.length - 1}
        aria-label="Older season"
        className="w-7 h-7 rounded-md flex items-center justify-center border border-slate-200 dark:border-slate-700 text-slate-500 disabled:opacity-30"
      >
        <ChevronLeft size={14} />
      </button>
      {seasons.map((s) => (
        <button
          key={s.season}
          type="button"
          onClick={() => setSeason(s.season)}
          data-testid="player-modal-season"
          className={`px-2.5 py-1 rounded-md text-xs font-black font-mono ${s.season === season ? 'bg-blue-600 text-white' : 'text-slate-500 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-800'}`}
        >
          {s.season}
        </button>
      ))}
      <button
        type="button"
        onClick={() => idx > 0 && setSeason(seasons[idx - 1].season)}
        disabled={idx <= 0}
        aria-label="Newer season"
        className="w-7 h-7 rounded-md flex items-center justify-center border border-slate-200 dark:border-slate-700 text-slate-500 disabled:opacity-30"
      >
        <ChevronRight size={14} />
      </button>
      <span className="ml-auto text-[10px] font-mono text-slate-400">
        {rows.length} games · avg {seasonAvg.toFixed(1)}
      </span>
    </div>
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-2 sm:p-4 bg-black/50 dark:bg-black/90 backdrop-blur-sm" onClick={onClose}>
      <div
        role="dialog"
        aria-modal="true"
        aria-label={`${player.player_name} details`}
        data-testid="player-modal"
        className="bg-white dark:bg-slate-950 w-full max-w-4xl rounded-2xl border border-slate-200 dark:border-slate-800 shadow-2xl overflow-hidden flex flex-col max-h-[92vh]"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header with team gradient */}
        <div className="p-4 sm:p-6 border-b border-white/10 flex gap-4 sm:gap-5 shrink-0 relative" style={{ background: `linear-gradient(135deg, ${teamColor} 0%, #1e293b 100%)` }}>
          <img src={sizedPlayerImage(player.image, 80)} alt="" className="w-16 h-16 sm:w-20 sm:h-20 rounded-full border-4 border-white/20 bg-black/20 object-cover shadow-lg" decoding="async" />
          <div className="flex-1 min-w-0 text-white">
            <h2 className="text-2xl sm:text-3xl font-black leading-none mb-1 drop-shadow-md truncate">{player.player_name}</h2>
            <p className="opacity-90 font-bold text-sm tracking-wide mb-3 flex items-center gap-2 flex-wrap">
              <span className="bg-black/30 px-2 py-0.5 rounded">{player.position}</span>
              <span>{player.team}</span>
              {player.opponent && <span className="opacity-70">vs {player.opponent}</span>}
            </p>
            <div className="flex gap-6 text-sm">
              <div className="flex flex-col">
                <span className="text-[10px] opacity-60 uppercase font-black">Projection</span>
                <span className="font-mono font-bold text-lg drop-shadow-sm">{player.prediction}</span>
              </div>
              <div className="flex flex-col">
                <span className="text-[10px] opacity-60 uppercase font-black">Avg Pts</span>
                <span className="opacity-90 font-mono font-bold text-lg">{player.average_points}</span>
              </div>
              {player.implied_total != null && (
                <div className="flex flex-col">
                  <span className="text-[10px] opacity-60 uppercase font-black">Team Total</span>
                  <span className="opacity-90 font-mono font-bold text-lg">{player.implied_total}</span>
                </div>
              )}
            </div>
          </div>
          <button onClick={onClose} aria-label="Close" className="absolute top-3 right-3 text-white/60 hover:text-white p-2">
            <X size={18} />
          </button>
        </div>

        {/* Tabs */}
        <div className="grid grid-cols-4 border-b border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900/50 shrink-0">
          {TABS.map((t) => (
            <button
              key={t.id}
              type="button"
              role="tab"
              aria-selected={tab === t.id}
              onClick={() => setTab(t.id)}
              data-testid={`player-modal-tab-${t.id}`}
              className={`flex items-center justify-center gap-1.5 py-3 text-[11px] font-black uppercase tracking-wider transition-colors ${tab === t.id ? 'text-blue-600 dark:text-blue-400 border-b-2 border-blue-500 bg-white dark:bg-slate-900' : 'text-slate-500 hover:text-slate-800 dark:hover:text-slate-300'}`}
            >
              {t.icon}
              <span className="hidden sm:inline">{t.label}</span>
            </button>
          ))}
        </div>

        <div className="overflow-y-auto flex-1 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-300">
          {(tab === 'log' || tab === 'visuals') && (
            <>
              {seasonPicker}
              {loadingHistory ? (
                <div className="p-12 text-center text-slate-400 font-bold animate-pulse">Loading game log…</div>
              ) : rows.length === 0 ? (
                <div className="p-12 text-center text-slate-400 font-bold">No games on record yet.</div>
              ) : tab === 'visuals' ? (
                <PlayerPerformanceCharts rows={rows} position={player.position} season={season ?? new Date().getFullYear()} teamColor={teamColor} />
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm text-left whitespace-nowrap" data-testid="player-modal-log">
                    <thead className="text-[10px] uppercase bg-slate-50 dark:bg-slate-900 text-slate-500 font-bold sticky top-0 z-10 border-b border-slate-200 dark:border-slate-800">
                      <tr>
                        <th className="px-4 py-3">Wk</th>
                        <th className="px-4 py-3">Opp</th>
                        <th className="px-4 py-3 text-center">Snaps</th>
                        <th className="px-4 py-3 text-center">Rec/Tgt</th>
                        <th className="px-4 py-3 text-center">Rush Yds/Att</th>
                        <th className="px-4 py-3 text-right">Pass Yds</th>
                        <th className="px-4 py-3 text-right">Rec Yds</th>
                        <th className="px-4 py-3 text-right">TDs</th>
                        <th className="px-4 py-3 text-right bg-slate-100 dark:bg-slate-900/50">Pts</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                      {rows.map((game) => {
                        // Older seasons often have no snap data; say "-" rather than "0%".
                        const hasSnaps = (game.snap_count || 0) > 0 || (game.snap_percentage || 0) > 0;
                        return (
                          <tr key={`${game.season}-${game.week}`} className="hover:bg-slate-50 dark:hover:bg-slate-900/50">
                            <td className="px-4 py-3 font-mono font-bold text-slate-500" data-testid="player-modal-week">{weekLabel(game.week, game.season)}</td>
                            <td className="px-4 py-3 font-bold text-slate-700 dark:text-slate-400">{game.opponent}</td>
                            <td className="px-4 py-3 text-center">
                              {hasSnaps ? (
                                <div className="flex flex-col items-center">
                                  <span className="font-mono font-bold">{((game.snap_percentage || 0) * 100).toFixed(0)}%</span>
                                  <span className="text-[9px] text-slate-400 font-mono">{game.snap_count} / {game.team_total_snaps || '-'}</span>
                                </div>
                              ) : (
                                <span className="text-slate-400">-</span>
                              )}
                            </td>
                            <td className="px-4 py-3 text-center font-mono">{game.receptions ?? '-'}/{game.targets ?? '-'}</td>
                            <td className="px-4 py-3 text-center font-mono text-slate-500 dark:text-slate-400">{game.rushing_yds || '-'} / {game.carries || '-'}</td>
                            <td className="px-4 py-3 text-right font-mono text-slate-500 dark:text-slate-400">{game.passing_yds || '-'}</td>
                            <td className="px-4 py-3 text-right font-mono text-slate-500 dark:text-slate-400">{game.receiving_yds || '-'}</td>
                            <td className="px-4 py-3 text-right font-mono text-slate-500 dark:text-slate-400">{game.touchdowns || '-'}</td>
                            <td className="px-4 py-3 text-right bg-slate-50 dark:bg-slate-900/30">
                              <span className={`font-black ${(game.points || 0) >= 15 ? 'text-green-600 dark:text-green-400' : 'text-slate-800 dark:text-white'}`}>
                                {(game.points || 0).toFixed(1)}
                              </span>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}

          {tab === 'storylines' && <PlayerStorylines playerId={player.player_id} playerName={player.player_name} teamColor={teamColor} />}

          {tab === 'vegas' && (
            <div className="p-4 sm:p-6 space-y-5" data-testid="player-modal-vegas">
              <div>
                <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-500 mb-2">
                  Game line · {player.team} vs {player.opponent || '?'}
                </h3>
                {player.overunder == null && player.spread == null ? (
                  <p className="text-sm text-slate-500">No line posted for this game yet.</p>
                ) : (
                  <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
                    {[
                      { label: 'Spread', value: fmtSigned(player.spread) },
                      { label: 'Over/Under', value: player.overunder ?? '-' },
                      { label: 'Team total', value: player.implied_total ?? '-' },
                      { label: 'Moneyline', value: formatOdds(player.moneyline) },
                    ].map((c) => (
                      <div key={c.label} className="rounded-xl border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900 p-3">
                        <div className="text-[10px] font-bold uppercase text-slate-400">{c.label}</div>
                        <div className="text-xl font-black font-mono text-slate-900 dark:text-white">{c.value}</div>
                      </div>
                    ))}
                  </div>
                )}
                {player.lines_source === 'schedule' && (
                  <p className="text-[10px] text-slate-400 mt-1.5">Bovada has no board for this game, so this is the consensus line from the NFL schedule feed.</p>
                )}
              </div>

              <div>
                <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-500 mb-2">Player props (Bovada)</h3>
                {markets.length === 0 ? (
                  <p className="text-sm text-slate-500">
                    No props for {player.player_name} this week. Bovada posts them a few days before kickoff and takes them down once a game starts.
                  </p>
                ) : (
                  <div className="overflow-x-auto rounded-xl border border-slate-200 dark:border-slate-800">
                    <table className="w-full text-sm whitespace-nowrap">
                      <thead className="text-[10px] uppercase bg-slate-50 dark:bg-slate-900 text-slate-500 font-bold">
                        <tr>
                          <th className="px-3 py-2 text-left">Market</th>
                          <th className="px-3 py-2 text-right">Line</th>
                          <th className="px-3 py-2 text-right">Over</th>
                          {hasUnders && <th className="px-3 py-2 text-right">Under</th>}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                        {shownMarkets.map((m) => (
                          <tr key={`${m.prop_type}-${m.line}`} data-testid="player-modal-prop">
                            <td className="px-3 py-2 font-bold">{m.prop_type}</td>
                            <td className="px-3 py-2 text-right font-mono">{m.yes ? '-' : m.line ?? '-'}</td>
                            {m.yes ? (
                              <td className="px-3 py-2 text-right font-mono" colSpan={hasUnders ? 2 : 1}>
                                {formatOdds(m.yes.odds)} <span className="text-slate-400">· {m.yes.prob ?? '-'}%</span>
                              </td>
                            ) : (
                              <>
                                <td className="px-3 py-2 text-right font-mono">
                                  {m.over ? <>{formatOdds(m.over.odds)} <span className="text-slate-400">· {m.over.prob ?? '-'}%</span></> : '-'}
                                </td>
                                {hasUnders && <td className="px-3 py-2 text-right font-mono">
                                  {m.under ? <>{formatOdds(m.under.odds)} <span className="text-slate-400">· {m.under.prob ?? '-'}%</span></> : '-'}
                                </td>}
                              </>
                            )}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                {alternates > 0 && (
                  <button
                    type="button"
                    onClick={() => setShowAlternates((v) => !v)}
                    data-testid="player-modal-alternates"
                    className="mt-2 text-[11px] font-bold text-blue-600 dark:text-blue-400 hover:underline"
                  >
                    {showAlternates ? 'Hide alternate lines' : `Show ${alternates} alternate line${alternates === 1 ? '' : 's'}`}
                  </button>
                )}
                {markets.length > 0 && !hasUnders && (
                  <p className="text-[10px] text-slate-400 mt-1.5">Only over prices were saved for this game; under prices are kept for games from mid-September 2026 on.</p>
                )}
                <p className="text-[10px] text-slate-400 mt-1.5">Percentages are the implied probability of each price, including the book's margin.</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default PlayerModal;
