import { useMemo, useState, useEffect } from 'react';
import { Check, Plus, ChevronLeft, ChevronRight } from 'lucide-react';
import { usePlayerHistory, usePlayerProfileById, type HistoryEntry } from '../hooks/useNflData';
import { getTeamColor } from '../utils/nflColors';

interface Props {
  playerId: string;
  onBack: () => void;
  compareList: string[];
  onToggleCompare: (id: string) => void;
}

export default function PlayerHistory({ playerId, compareList, onToggleCompare }: Props) {
  const { history, loadingHistory } = usePlayerHistory(playerId);
  const { cardData } = usePlayerProfileById(playerId);

  // Group rows by season, sorted newest → oldest.
  const seasons = useMemo(() => {
    const map = new Map<number, HistoryEntry[]>();
    history.forEach((r) => {
      const s = r.season ?? 0;
      if (!map.has(s)) map.set(s, []);
      map.get(s)!.push(r);
    });
    // Sort each season's weeks low → high so chronological reading is natural.
    map.forEach((rows) => rows.sort((a, b) => a.week - b.week));
    return [...map.entries()]
      .map(([season, rows]) => ({ season, rows }))
      .sort((a, b) => b.season - a.season);
  }, [history]);

  const [selectedSeason, setSelectedSeason] = useState<number | null>(null);
  useEffect(() => {
    if (seasons.length === 0) {
      setSelectedSeason(null);
      return;
    }
    // Default to the most-recent season that actually has any scoring/games.
    const withGames = seasons.find((s) => s.rows.some((r) => (r.points || 0) > 0));
    setSelectedSeason((withGames || seasons[0]).season);
  }, [seasons]);

  const idx = selectedSeason != null ? seasons.findIndex((s) => s.season === selectedSeason) : -1;
  const currentSeason = idx >= 0 ? seasons[idx] : null;
  const canPrev = idx >= 0 && idx < seasons.length - 1; // older
  const canNext = idx > 0; // newer

  // Keyboard cycling.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'ArrowLeft' && canPrev) setSelectedSeason(seasons[idx + 1].season);
      if (e.key === 'ArrowRight' && canNext) setSelectedSeason(seasons[idx - 1].season);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [canPrev, canNext, idx, seasons]);

  const teamColor = cardData ? getTeamColor(cardData.team) : '#1e293b';
  const isSelected = compareList.includes(playerId);
  const isBoosted = cardData?.is_injury_boosted;

  const rows = currentSeason?.rows ?? [];
  const totalPoints = rows.reduce((acc, r) => acc + (r.points || 0), 0);
  const totalTDs = rows.reduce((acc, r) => acc + (r.touchdowns || 0), 0);
  const gamesWithPoints = rows.filter((r) => (r.points || 0) > 0).length;
  const avgPoints = gamesWithPoints > 0 ? (totalPoints / gamesWithPoints).toFixed(1) : '0.0';
  const avgSnapPct =
    rows.length > 0
      ? (rows.reduce((acc, r) => acc + (r.snap_percentage || 0), 0) / rows.length * 100).toFixed(0)
      : '0';

  return (
    <div className="w-full animate-in fade-in slide-in-from-bottom-4 duration-500">
      {/* HEADER CARD WITH GRADIENT BLEND */}
      <div
        className="rounded-xl shadow-lg border border-white/10 p-6 mb-6 text-white relative overflow-hidden transition-all duration-500"
        style={{ background: `linear-gradient(135deg, ${teamColor} 0%, #0f172a 100%)` }}
      >
        <button
          onClick={(e) => {
            e.stopPropagation();
            onToggleCompare(playerId);
          }}
          className={`absolute top-4 right-4 z-30 p-2 rounded-full shadow-lg transition-all transform hover:scale-110 ${
            isSelected
              ? 'bg-blue-600 text-white border border-blue-400'
              : 'bg-white/20 text-white hover:bg-white/40 backdrop-blur-md border border-white/20'
          }`}
        >
          {isSelected ? <Check size={20} strokeWidth={4} /> : <Plus size={20} strokeWidth={3} />}
        </button>

        <div className="absolute -right-20 -top-20 w-96 h-96 bg-white/5 rounded-full blur-[100px] pointer-events-none mix-blend-overlay" />
        {isBoosted && (
          <div className="absolute top-0 right-0 w-1/2 h-full bg-gradient-to-l from-amber-500/20 to-transparent pointer-events-none mix-blend-overlay animate-pulse" />
        )}

        <div className="flex flex-col xl:flex-row items-center justify-between gap-6 relative z-10">
          <div className="flex items-center gap-6 w-full xl:w-auto">
            <div className="h-24 w-24 rounded-full border-4 border-white/20 bg-black/20 overflow-hidden shadow-2xl shrink-0">
              {cardData?.image ? (
                <img src={cardData.image} alt={cardData.name} className="w-full h-full object-cover" />
              ) : (
                <div className="w-full h-full flex items-center justify-center text-white/50 font-bold">...</div>
              )}
            </div>
            <div>
              <div className="flex items-center gap-3">
                <h1 className="text-3xl md:text-4xl font-black tracking-tight uppercase drop-shadow-md">
                  {cardData?.name || 'Loading...'}
                </h1>
                {isBoosted && (
                  <div className="flex items-center gap-1 bg-amber-500/20 border border-amber-400/50 px-2 py-0.5 rounded text-[10px] font-black uppercase text-amber-200 tracking-wider shadow-[0_0_10px_rgba(251,191,36,0.3)] animate-pulse">
                    <span>Usage Boost</span>
                  </div>
                )}
              </div>

              <div className="flex flex-wrap items-center gap-2 md:gap-3 text-white/90 font-bold text-sm mt-1">
                <span className="bg-black/30 px-2 py-0.5 rounded text-white border border-white/10">{cardData?.position}</span>
                <span>{cardData?.team}</span>
                <span className="text-white/50 hidden md:inline">•</span>
                <span>vs {cardData?.opponent}</span>
                <span className="text-white/50 hidden md:inline">•</span>
                <span
                  className={`px-2 py-0.5 rounded font-black text-xs ${
                    cardData?.injury_status === 'Active' ? 'bg-green-500 text-white' : 'bg-red-500 text-white'
                  }`}
                >
                  {cardData?.injury_status || 'ACT'}
                </span>
              </div>
            </div>
          </div>

          {/* Header stats are now scoped to the *selected* season. */}
          <div className="w-full xl:w-auto flex flex-wrap justify-around md:justify-end items-center gap-2 md:gap-6 text-center bg-black/20 p-4 rounded-xl border border-white/10 backdrop-blur-md shadow-inner">
            <div className="min-w-[60px]">
              <div className="text-[10px] font-bold text-white/60 uppercase tracking-widest">Season</div>
              <div className="text-xl md:text-2xl font-black font-mono">{currentSeason?.season || '-'}</div>
            </div>
            <div className="hidden md:block w-px bg-white/10 h-10 mx-auto" />
            <div className="min-w-[60px]">
              <div className="text-[10px] font-bold text-white/60 uppercase tracking-widest">Avg</div>
              <div className="text-xl md:text-2xl font-black">{avgPoints}</div>
            </div>
            <div className="min-w-[60px]">
              <div className="text-[10px] font-bold text-white/60 uppercase tracking-widest">TDs</div>
              <div className="text-xl md:text-2xl font-black">{totalTDs}</div>
            </div>
            <div className="min-w-[60px]">
              <div className="text-[10px] font-bold text-white/60 uppercase tracking-widest">Usage</div>
              <div className="text-xl md:text-2xl font-black">{avgSnapPct}%</div>
            </div>
          </div>
        </div>
      </div>

      {/* SEASON SELECTOR */}
      {seasons.length > 0 && currentSeason && (
        <div className="flex items-center gap-2 mb-3 flex-wrap">
          <button
            onClick={() => canPrev && setSelectedSeason(seasons[idx + 1].season)}
            disabled={!canPrev}
            className="w-7 h-7 rounded-md flex items-center justify-center bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-500 dark:text-slate-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 disabled:opacity-30 disabled:cursor-not-allowed transition"
            aria-label="Older season"
          >
            <ChevronLeft size={14} />
          </button>
          <div className="flex items-center gap-1 bg-slate-100 dark:bg-slate-800/60 p-1 rounded-lg border border-slate-200 dark:border-slate-700">
            {seasons.map((s) => {
              const active = s.season === selectedSeason;
              const empty = s.rows.every((r) => (r.points || 0) === 0);
              return (
                <button
                  key={s.season}
                  onClick={() => setSelectedSeason(s.season)}
                  className={`px-3 py-1 rounded-md text-xs font-black font-mono transition ${
                    active
                      ? 'bg-blue-600 text-white shadow-sm'
                      : empty
                      ? 'text-slate-400 dark:text-slate-500 italic hover:text-slate-600 dark:hover:text-slate-300'
                      : 'text-slate-600 dark:text-slate-300 hover:bg-white dark:hover:bg-slate-700'
                  }`}
                  title={`${s.rows.length} games`}
                >
                  {s.season}
                </button>
              );
            })}
          </div>
          <button
            onClick={() => canNext && setSelectedSeason(seasons[idx - 1].season)}
            disabled={!canNext}
            className="w-7 h-7 rounded-md flex items-center justify-center bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-500 dark:text-slate-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 disabled:opacity-30 disabled:cursor-not-allowed transition"
            aria-label="Newer season"
          >
            <ChevronRight size={14} />
          </button>
          <span className="text-[10px] font-mono text-slate-400 dark:text-slate-500 ml-2">
            ← → to cycle · {rows.length} games
          </span>
        </div>
      )}

      {/* HISTORY TABLE */}
      <div className="bg-white dark:bg-slate-800 rounded-xl shadow-sm border border-slate-200 dark:border-slate-700 overflow-hidden flex flex-col transition-colors duration-300">
        {loadingHistory ? (
          <div className="p-20 text-center text-slate-400 dark:text-slate-500 animate-pulse font-bold">
            Loading Season History...
          </div>
        ) : rows.length === 0 ? (
          <div className="p-20 text-center text-slate-400 dark:text-slate-500 font-bold">
            No games for {currentSeason?.season || 'this season'} yet.
          </div>
        ) : (
          <div className="overflow-x-auto scrollbar-thin dark:scrollbar-thumb-slate-600 dark:scrollbar-track-slate-800">
            <table className="w-full text-sm text-left whitespace-nowrap">
              <thead className="bg-slate-50 dark:bg-slate-900/50 text-slate-500 dark:text-slate-400 font-bold uppercase text-xs tracking-wider border-b border-slate-100 dark:border-slate-700">
                <tr>
                  <th className="px-6 py-4 sticky left-0 bg-slate-50 dark:bg-slate-900 z-10">Week</th>
                  <th className="px-6 py-4">Opponent</th>
                  <th className="px-6 py-4 text-center">Snaps</th>
                  <th className="px-6 py-4 text-center">Rec / Tgt</th>
                  <th className="px-6 py-4 text-center">Rush Yds / Att</th>
                  <th className="px-6 py-4 text-right">Pass Yds</th>
                  <th className="px-6 py-4 text-right">Rec Yds</th>
                  <th className="px-6 py-4 text-right">TDs</th>
                  <th className="px-6 py-4 text-right bg-slate-50/50 dark:bg-slate-800/50">Pts</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-700">
                {rows.map((game) => (
                  <tr
                    key={`${game.season}-${game.week}`}
                    className="hover:bg-slate-50/50 dark:hover:bg-slate-700/50 transition-colors"
                  >
                    <td className="px-6 py-4 font-bold text-slate-800 dark:text-slate-100 sticky left-0 bg-white dark:bg-slate-800 z-10 border-r border-slate-100 dark:border-slate-700">
                      Week {game.week}
                    </td>
                    <td className="px-6 py-4">
                      <span className="px-2 py-1 bg-slate-100 dark:bg-slate-700 rounded text-slate-600 dark:text-slate-300 font-bold text-xs border border-slate-200 dark:border-slate-600">
                        {game.opponent}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-center">
                      <div className="flex flex-col items-center">
                        <span className="font-bold text-slate-700 dark:text-slate-300">
                          {((game.snap_percentage || 0) * 100).toFixed(0)}%
                        </span>
                        <span className="text-[10px] text-slate-400 font-mono">
                          {game.snap_count || 0} / {game.team_total_snaps || '-'}
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-4 text-center font-medium text-slate-600 dark:text-slate-400">
                      {game.receptions}
                      <span className="text-slate-300 dark:text-slate-600 mx-1">/</span>
                      {game.targets}
                    </td>
                    <td className="px-6 py-4 text-center font-medium text-slate-600 dark:text-slate-400">
                      {game.rushing_yds || '-'}
                      <span className="text-slate-300 dark:text-slate-600 mx-1">/</span>
                      {game.carries !== undefined ? game.carries : '-'}
                    </td>
                    <td className="px-6 py-4 text-right text-slate-600 dark:text-slate-400 font-medium">
                      {game.passing_yds || '-'}
                    </td>
                    <td className="px-6 py-4 text-right text-slate-600 dark:text-slate-400 font-medium">
                      {game.receiving_yds || '-'}
                    </td>
                    <td className="px-6 py-4 text-right text-slate-600 dark:text-slate-400 font-medium">
                      {game.touchdowns > 0 ? game.touchdowns : '-'}
                    </td>
                    <td className="px-6 py-4 text-right bg-slate-50/30 dark:bg-slate-900/30">
                      <span
                        className={`font-black text-base ${
                          game.points >= 15
                            ? 'text-green-600 dark:text-green-400'
                            : 'text-slate-700 dark:text-slate-300'
                        }`}
                      >
                        {game.points?.toFixed(1) || '0.0'}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
