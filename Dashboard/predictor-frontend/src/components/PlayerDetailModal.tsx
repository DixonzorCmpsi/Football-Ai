import React, { useEffect, useMemo, useState } from 'react';
import { X, History, Plus, Check, ChevronLeft, ChevronRight } from 'lucide-react';
import type { OffensePlayer } from './TeamOffenseModal';
import { getTeamColor } from '../utils/nflColors';
import { usePlayerSeasonStats, type SeasonStats } from '../hooks/useNflData';

interface Props {
  player: OffensePlayer;
  onClose: () => void;
  isComparing: boolean;
  onToggleCompare: (id: string) => void;
  onViewHistory: (id: string) => void;
}

// Height in inches → 6'2"
const formatHeight = (inches?: number | null) => {
  if (!inches) return null;
  const ft = Math.floor(inches / 12);
  const inch = inches % 12;
  return `${ft}'${inch}"`;
};

const StatTile: React.FC<{ label: string; value: string | number; sub?: string; tone?: 'default' | 'pos' | 'neg' | 'muted' }> = ({
  label,
  value,
  sub,
  tone = 'default',
}) => {
  const valColor =
    tone === 'pos'
      ? 'text-emerald-600 dark:text-emerald-400'
      : tone === 'neg'
      ? 'text-red-600 dark:text-red-400'
      : tone === 'muted'
      ? 'text-slate-400 dark:text-slate-500'
      : 'text-slate-800 dark:text-slate-100';
  return (
    <div className="bg-slate-50 dark:bg-slate-800/60 rounded-lg p-3 border border-slate-200 dark:border-slate-700">
      <p className="text-[10px] font-bold uppercase tracking-wider text-slate-400 dark:text-slate-500">{label}</p>
      <p className={`text-lg font-black mt-0.5 ${valColor}`}>{value}</p>
      {sub && <p className="text-[10px] text-slate-400 dark:text-slate-500 mt-0.5">{sub}</p>}
    </div>
  );
};

const PlayerDetailModal: React.FC<Props> = ({ player, onClose, isComparing, onToggleCompare, onViewHistory }) => {
  const { seasonData, loadingSeasons } = usePlayerSeasonStats(player.player_id);

  // Sorted newest → oldest. Use server-provided list (already capped at 5).
  const seasons = useMemo<SeasonStats[]>(() => seasonData?.seasons || [], [seasonData]);

  // Default to the most recent season that has games; if none, the most recent entry
  // (so the empty-state still has a season header). Falls back to current player.
  const [selectedSeason, setSelectedSeason] = useState<number | null>(null);
  useEffect(() => {
    if (seasons.length === 0) {
      setSelectedSeason(null);
      return;
    }
    const firstWithGames = seasons.find((s) => s.games_played > 0);
    setSelectedSeason((firstWithGames || seasons[0]).season);
  }, [seasons]);

  const idx = selectedSeason != null ? seasons.findIndex((s) => s.season === selectedSeason) : -1;
  const current: SeasonStats | null = idx >= 0 ? seasons[idx] : null;
  const canPrev = idx >= 0 && idx < seasons.length - 1; // older
  const canNext = idx > 0; // newer

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
      if (e.key === 'ArrowLeft' && canPrev) setSelectedSeason(seasons[idx + 1].season);
      if (e.key === 'ArrowRight' && canNext) setSelectedSeason(seasons[idx - 1].season);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose, canPrev, canNext, idx, seasons]);

  const teamColor = getTeamColor(player.team);
  // "Has stats" now reflects the *selected* season — the prop-level value only
  // covers the current season aggregate (which is 0 in preseason).
  const hasStats = (current?.games_played ?? 0) > 0;
  const positionGroup = (seasonData?.position_group ?? player.position_group) as 'qb' | 'rb' | 'wr' | 'te' | 'ol';
  const status = (player.injury_status || '').toLowerCase();
  const statusBadge =
    status.includes('out') || status.includes('ir')
      ? { label: 'OUT', cls: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' }
      : status.includes('doubtful')
      ? { label: 'DOUBTFUL', cls: 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400' }
      : status.includes('questionable')
      ? { label: 'QUESTIONABLE', cls: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400' }
      : null;

  const height = formatHeight(player.height ?? null);

  return (
    <div
      // z higher than the team modal (z-60) so this stacks on top.
      className="fixed inset-0 z-[70] flex items-center justify-center bg-black/60 backdrop-blur-sm p-4 animate-in fade-in duration-200"
      onClick={onClose}
    >
      <div
        className="bg-white dark:bg-slate-900 rounded-2xl shadow-2xl border border-slate-200 dark:border-slate-700 w-full max-w-2xl max-h-[88vh] flex flex-col overflow-hidden animate-in zoom-in-95 duration-200"
        onClick={(e) => e.stopPropagation()}
      >
        {/* HEADER */}
        <div
          className="px-5 py-4 flex items-start gap-4 border-b border-slate-200 dark:border-slate-700"
          style={{ background: `linear-gradient(135deg, ${teamColor}26 0%, transparent 100%)` }}
        >
          <div className="relative shrink-0">
            <div className="w-20 h-20 rounded-full overflow-hidden bg-slate-100 dark:bg-slate-700 border-2 border-white dark:border-slate-600 shadow-md">
              {player.image && (
                <img src={player.image} alt={player.player_name} className="w-full h-full object-cover" />
              )}
            </div>
            <div
              className="absolute -bottom-1 -right-1 w-7 h-7 rounded-md flex items-center justify-center text-white text-[10px] font-black border-2 border-white dark:border-slate-900"
              style={{ backgroundColor: teamColor }}
            >
              {player.team}
            </div>
          </div>

          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 flex-wrap">
              <h2 className="text-xl font-black text-slate-800 dark:text-slate-100 truncate">{player.player_name}</h2>
              {player.is_rookie && (
                <span className="text-[9px] font-black px-1.5 py-0.5 rounded bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300">
                  ROOKIE
                </span>
              )}
              {statusBadge && (
                <span className={`text-[9px] font-black px-1.5 py-0.5 rounded ${statusBadge.cls}`}>
                  {statusBadge.label}
                </span>
              )}
            </div>
            <div className="flex items-center gap-2 text-xs font-bold text-slate-500 dark:text-slate-400 mt-1">
              <span className="bg-slate-100 dark:bg-slate-800 px-1.5 py-0.5 rounded font-mono">{player.position}</span>
              <span>·</span>
              <span>{player.team}</span>
              {player.draft_number != null ? (
                <>
                  <span>·</span>
                  <span className="text-purple-600 dark:text-purple-400">
                    {player.draft_year} pick #{player.draft_number}
                  </span>
                </>
              ) : player.is_rookie ? (
                <>
                  <span>·</span>
                  <span className="italic text-slate-400">UDFA</span>
                </>
              ) : null}
            </div>
          </div>

          <div className="flex flex-col gap-1 items-end">
            <button onClick={onClose} className="p-1 rounded-md text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800" aria-label="Close">
              <X size={16} />
            </button>
            <button
              onClick={() => onToggleCompare(player.player_id)}
              className={`flex items-center gap-1 px-2 py-1 rounded-md text-[10px] font-bold ${
                isComparing
                  ? 'bg-blue-600 text-white'
                  : 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 hover:bg-blue-100 dark:hover:bg-blue-900/30'
              }`}
            >
              {isComparing ? <Check size={11} strokeWidth={4} /> : <Plus size={11} strokeWidth={3} />}
              Compare
            </button>
            <button
              onClick={() => onViewHistory(player.player_id)}
              className="flex items-center gap-1 px-2 py-1 rounded-md text-[10px] font-bold bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 hover:bg-purple-100 dark:hover:bg-purple-900/30"
            >
              <History size={11} />
              History
            </button>
          </div>
        </div>

        {/* BODY */}
        <div className="flex-1 overflow-y-auto p-5 space-y-5 scrollbar-thin">
          {/* MEASURABLES */}
          <section>
            <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-500 dark:text-slate-400 mb-2">
              Measurables
            </h3>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
              <StatTile label="Height" value={height || '—'} sub={player.height ? `${player.height}"` : undefined} />
              <StatTile label="Weight" value={player.weight ? `${player.weight} lb` : '—'} />
              <StatTile label="Age" value={player.age != null ? `${player.age}y` : '—'} />
              <StatTile
                label={player.is_rookie ? 'Draft' : 'Entered'}
                value={
                  player.draft_number != null
                    ? `#${player.draft_number}`
                    : player.is_rookie
                    ? 'UDFA'
                    : player.draft_year != null
                    ? `${player.draft_year}`
                    : '—'
                }
                sub={player.draft_year ? String(player.draft_year) : undefined}
              />
            </div>
          </section>

          {/* SEASON STATS (last 5, cyclable) */}
          <section>
            <div className="flex items-center justify-between mb-2 gap-2">
              <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
                Season stats
              </h3>
              {seasons.length > 0 && current && (
                <div className="flex items-center gap-1">
                  <button
                    onClick={() => canPrev && setSelectedSeason(seasons[idx + 1].season)}
                    disabled={!canPrev}
                    className="w-6 h-6 rounded flex items-center justify-center text-slate-500 dark:text-slate-400 bg-slate-100 dark:bg-slate-800 hover:bg-blue-100 dark:hover:bg-blue-900/30 disabled:opacity-30 disabled:cursor-not-allowed"
                    aria-label="Older season"
                  >
                    <ChevronLeft size={14} />
                  </button>
                  <div className="flex items-center gap-1 px-2 py-0.5 rounded bg-slate-100 dark:bg-slate-800 min-w-[3.5rem] justify-center">
                    <span className="text-xs font-black text-slate-700 dark:text-slate-200 font-mono">
                      {current.season}
                    </span>
                  </div>
                  <button
                    onClick={() => canNext && setSelectedSeason(seasons[idx - 1].season)}
                    disabled={!canNext}
                    className="w-6 h-6 rounded flex items-center justify-center text-slate-500 dark:text-slate-400 bg-slate-100 dark:bg-slate-800 hover:bg-blue-100 dark:hover:bg-blue-900/30 disabled:opacity-30 disabled:cursor-not-allowed"
                    aria-label="Newer season"
                  >
                    <ChevronRight size={14} />
                  </button>
                </div>
              )}
            </div>

            {seasons.length > 0 && (
              <div className="flex flex-wrap gap-1 mb-2">
                {seasons.map((s) => {
                  const active = s.season === selectedSeason;
                  return (
                    <button
                      key={s.season}
                      onClick={() => setSelectedSeason(s.season)}
                      className={`px-2 py-0.5 rounded text-[10px] font-bold font-mono transition ${
                        active
                          ? 'bg-blue-600 text-white'
                          : s.games_played > 0
                          ? 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 hover:bg-blue-100 dark:hover:bg-blue-900/30'
                          : 'bg-slate-50 dark:bg-slate-800/40 text-slate-400 dark:text-slate-500 italic'
                      }`}
                      title={s.games_played > 0 ? `${s.games_played} games` : 'No games'}
                    >
                      {s.season}
                    </button>
                  );
                })}
              </div>
            )}

            {loadingSeasons ? (
              <div className="bg-slate-50 dark:bg-slate-800/40 border border-dashed border-slate-200 dark:border-slate-700 rounded-xl p-6 text-center">
                <p className="text-xs text-slate-400 dark:text-slate-500">Loading season history…</p>
              </div>
            ) : hasStats && current ? (
              <>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
                  <StatTile
                    label="Games"
                    value={current.games_played}
                    sub={current.snap_pct_avg > 0 ? `${Math.round(current.snap_pct_avg)}% snaps` : undefined}
                  />
                  <StatTile
                    label="PPG (PPR)"
                    value={current.season_avg_pts.toFixed(1)}
                    sub={`${current.season_total_pts.toFixed(0)} total`}
                    tone="pos"
                  />
                  <StatTile
                    label="Recent form"
                    value={current.recent_avg_pts.toFixed(1)}
                    sub="last 4 games"
                  />
                  <StatTile
                    label="Boom / Bust"
                    value={`${current.boom_games} / ${current.bust_games}`}
                    sub="20+ pts / <5 pts"
                  />
                </div>

                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mt-2">
                  <StatTile label="Total yds" value={current.total_yds.toLocaleString()} />
                  <StatTile label="Total TDs" value={current.total_tds} tone="pos" />
                  {(positionGroup === 'wr' || positionGroup === 'te') && (
                    <>
                      <StatTile
                        label="Receptions"
                        value={current.total_receptions}
                        sub={current.total_targets ? `${current.total_targets} tgt` : undefined}
                      />
                      <StatTile
                        label="Catch %"
                        value={
                          current.total_targets > 0
                            ? `${Math.round((current.total_receptions / current.total_targets) * 100)}%`
                            : '—'
                        }
                      />
                    </>
                  )}
                  {positionGroup === 'rb' && (
                    <>
                      <StatTile label="Carries" value={current.total_carries} />
                      <StatTile
                        label="Receptions"
                        value={current.total_receptions}
                        sub={current.total_targets ? `${current.total_targets} tgt` : undefined}
                      />
                    </>
                  )}
                  {positionGroup === 'qb' && (
                    <>
                      <StatTile label="Snaps total" value={current.snaps_total.toLocaleString()} />
                      <StatTile label="" value="" />
                    </>
                  )}
                </div>
              </>
            ) : (
              <div className="bg-slate-50 dark:bg-slate-800/40 border border-dashed border-slate-200 dark:border-slate-700 rounded-xl p-6 text-center">
                <p className="text-sm font-bold text-slate-500 dark:text-slate-400">
                  {current
                    ? `No games for ${current.season}.`
                    : player.is_rookie
                    ? "No NFL stats yet — rookie season hasn't kicked off."
                    : 'No stats available for this player.'}
                </p>
                <p className="text-xs text-slate-400 dark:text-slate-500 mt-1">
                  {seasons.length > 1 ? 'Use the year tabs to view past seasons.' : 'Stats appear once games are played and ETL ingests them.'}
                </p>
              </div>
            )}
          </section>

          {/* CONTEXT */}
          <section>
            <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-500 dark:text-slate-400 mb-2">
              Status
            </h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              <div className="bg-slate-50 dark:bg-slate-800/60 rounded-lg p-3 border border-slate-200 dark:border-slate-700 text-xs">
                <span className="font-bold text-slate-500 dark:text-slate-400">Injury: </span>
                <span className="text-slate-800 dark:text-slate-100">{player.injury_status || 'Active'}</span>
              </div>
              <div className="bg-slate-50 dark:bg-slate-800/60 rounded-lg p-3 border border-slate-200 dark:border-slate-700 text-xs">
                <span className="font-bold text-slate-500 dark:text-slate-400">Depth: </span>
                <span className="text-slate-800 dark:text-slate-100">
                  {player.snap_pct_avg >= 70
                    ? 'Starter (workhorse)'
                    : player.snap_pct_avg >= 40
                    ? 'Starter / committee'
                    : player.snap_pct_avg > 0
                    ? 'Rotational / backup'
                    : 'Unknown'}
                </span>
              </div>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
};

export default PlayerDetailModal;
