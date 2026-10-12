import { useMemo, useState, useEffect } from 'react';
import { Check, Plus, ChevronLeft, ChevronRight, Table2, LineChart, Newspaper, ListOrdered, ArrowLeft, User, BarChart3, TrendingUp, Gauge } from 'lucide-react';
import { usePlayerHistory, usePlayerProfileById, type HistoryEntry } from '../hooks/useNflData';
import { getTeamColor } from '../utils/nflColors';
import { pointColor } from './PlayerPerformanceCharts';
import PlayerStorylines from './PlayerStorylines';
import PlayerDepthChart from './PlayerDepthChart';
import PlayerBio from './PlayerBio';
import PlayerUsage from './PlayerUsage';
import PlayerVegas from './PlayerVegas';
import PlayerRankChips from './PlayerRankChips';
import VisualsPanel from './VisualsPanel';
import type { Series } from './visualSeries';
import { useAgentScreen } from '../contexts/AgentScreenContext';
import type { PlayerTab } from '../lib/appUrl';
import { SeasonStatsTable, GameLogTable } from './PlayerStatTables';
import { sizedPlayerImage } from '../utils/playerImage';

// Flow: this season's game log, how much of the offense runs through him, where he
// sits on the team, every season, then visuals and news. The betting market, then Bio, close it out.
const TABS = [
  ['log', 'Game Log', Table2],
  ['usage', 'Usage', Gauge],
  ['depth', 'Depth Chart', ListOrdered],
  ['seasons', 'Season Stats', BarChart3],
  ['visual', 'Visual', LineChart],
  ['storylines', 'Storylines', Newspaper],
  ['vegas', 'Vegas Data', TrendingUp],
  ['bio', 'Bio', User],
] as const;

interface Props {
  playerId: string;
  /** Controlled tab, so a link or the assistant can open straight onto stats or charts. */
  view?: PlayerTab;
  onViewChange?: (view: PlayerTab) => void;
  onBack: () => void;
  compareList: string[];
  onToggleCompare: (id: string) => void;
}

export default function PlayerHistory({ playerId: rootId, compareList, onToggleCompare, view, onViewChange }: Props) {
  // Clicking a teammate on the depth chart swaps the page to that player in place;
  // `rootId` stays the player the user came in on so they can step back to them.
  const [playerId, setPlayerId] = useState(rootId);
  useEffect(() => setPlayerId(rootId), [rootId]);
  const { history, loadingHistory } = usePlayerHistory(playerId);
  const { cardData } = usePlayerProfileById(playerId);
  const { cardData: rootCard } = usePlayerProfileById(rootId);

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

  const [ownView, setOwnView] = useState<PlayerTab>('log');
  const viewMode = view ?? ownView;
  const setViewMode = (next: PlayerTab) => {
    setOwnView(next);
    onViewChange?.(next);
  };
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
  // Other players picked for comparison (from depth charts or the + button); shown under this one's visuals.
  const extraCompareIds = compareList.filter((id) => id !== playerId);

  const tabLabel = TABS.find(([m]) => m === viewMode)?.[1] ?? '';
  useAgentScreen(
    cardData
      ? {
          view: 'PLAYER',
          title: `${cardData.name}'s player page, ${tabLabel} tab`,
          facts: [
            ...(viewMode === 'log' || viewMode === 'usage' || viewMode === 'visual' ? (selectedSeason ? [`season ${selectedSeason}`] : []) : []),
            ...(playerId !== rootId && rootCard?.name ? [`opened from ${rootCard.name}'s depth chart`] : []),
          ],
          entities: [
            {
              type: 'player',
              name: cardData.name,
              id: playerId,
              detail: [cardData.position, cardData.team, cardData.injury_status].filter(Boolean).join(', '),
            },
          ],
        }
      : null,
  );
  const isSelected = compareList.includes(playerId);
  const isBoosted = cardData?.is_injury_boosted;

  const rows = currentSeason?.rows ?? [];
  const mainSeries: Series | null = cardData
    ? { id: playerId, name: cardData.name, position: cardData.position, team: cardData.team, season: currentSeason?.season ?? null, rows }
    : null;
  const totalPoints = rows.reduce((acc, r) => acc + (r.points || 0), 0);
  const totalTDs = rows.reduce((acc, r) => acc + (r.touchdowns || 0), 0);
  const gamesWithPoints = rows.filter((r) => (r.points || 0) > 0).length;
  const avgPointsNum = gamesWithPoints > 0 ? totalPoints / gamesWithPoints : 0;
  const avgPoints = avgPointsNum.toFixed(1);
  const avgSnapPct =
    rows.length > 0
      ? (rows.reduce((acc, r) => acc + (r.snap_percentage || 0), 0) / rows.length * 100).toFixed(0)
      : '0';

  return (
    <div className="w-full animate-in fade-in slide-in-from-bottom-4 duration-500">
      {playerId !== rootId && (
        <button
          onClick={() => setPlayerId(rootId)}
          className="mb-3 flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-black bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 transition"
        >
          <ArrowLeft size={13} /> Back to {rootCard?.name || 'original player'}
        </button>
      )}

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
                <img src={sizedPlayerImage(cardData.image, 96)} alt={cardData.name} className="w-full h-full object-cover" decoding="async" />
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
              <PlayerRankChips playerId={playerId} className="mt-2" />
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

      {/* ONE ROW: view tabs on the left, season picker on the right.
          The tabs always render (a player with no game logs still needs Storylines).
          The season picker only applies to Game Log, Usage and Visual, so it hides elsewhere. */}
      <div className="flex items-center gap-3 mb-3">
        <div role="tablist" className="flex items-center gap-1 bg-slate-100 dark:bg-slate-800/60 p-1 rounded-lg border border-slate-200 dark:border-slate-700 min-w-0 overflow-x-auto scrollbar-none">
          {TABS.map(([mode, label, Icon]) => (
            <button
              key={mode}
              role="tab"
              aria-selected={viewMode === mode}
              data-testid={`player-modal-tab-${mode}`}
              onClick={() => setViewMode(mode)}
              className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-black whitespace-nowrap transition ${
                viewMode === mode
                  ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm'
                  : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'
              }`}
            >
              <Icon size={13} /> {label}
            </button>
          ))}
        </div>

        {(viewMode === 'log' || viewMode === 'usage' || viewMode === 'visual') && seasons.length > 0 && currentSeason && (
          <div className="ml-auto shrink-0 flex items-center gap-1" title="Use the left and right arrow keys to change season">
            <button
              onClick={() => canPrev && setSelectedSeason(seasons[idx + 1].season)}
              disabled={!canPrev}
              className="w-7 h-7 rounded-md flex items-center justify-center bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-500 dark:text-slate-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 disabled:opacity-30 disabled:cursor-not-allowed transition"
              aria-label="Older season"
            >
              <ChevronLeft size={14} />
            </button>
            <select
              value={selectedSeason ?? ''}
              onChange={(e) => setSelectedSeason(Number(e.target.value))}
              aria-label="Season"
              className="h-7 rounded-md bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-xs font-black font-mono text-slate-700 dark:text-slate-200 px-2"
            >
              {seasons.map((x) => (
                <option key={x.season} value={x.season}>
                  {x.season} · {x.rows.length} G
                </option>
              ))}
            </select>
            <button
              onClick={() => canNext && setSelectedSeason(seasons[idx - 1].season)}
              disabled={!canNext}
              className="w-7 h-7 rounded-md flex items-center justify-center bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-500 dark:text-slate-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 disabled:opacity-30 disabled:cursor-not-allowed transition"
              aria-label="Newer season"
            >
              <ChevronRight size={14} />
            </button>
          </div>
        )}
      </div>

      {/* HISTORY TABLE / VISUAL */}
      <div className="bg-white dark:bg-slate-800 rounded-xl shadow-sm border border-slate-200 dark:border-slate-700 overflow-hidden flex flex-col transition-colors duration-300">
        {/* Storylines are checked BEFORE the loading/empty guards on purpose: a
            player with no games yet (a rookie, or anyone pre-kickoff) is exactly
            who you open this page to read about, and the old guards rendered
            "No games for this season yet" over everything. */}
        {viewMode === 'bio' ? (
          <PlayerBio playerId={playerId} />
        ) : viewMode === 'storylines' ? (
          <PlayerStorylines
            playerId={playerId}
            playerName={cardData?.name}
            teamColor={teamColor}
          />
        ) : viewMode === 'vegas' ? (
          <PlayerVegas playerId={playerId} />
        ) : viewMode === 'usage' ? (
          <PlayerUsage
            playerId={playerId}
            season={selectedSeason}
            teamColor={teamColor}
            onSelectPlayer={setPlayerId}
            extraIds={extraCompareIds}
            onRemove={onToggleCompare}
          />
        ) : viewMode === 'depth' ? (
          <PlayerDepthChart
            team={cardData?.team}
            playerId={playerId}
            teamColor={teamColor}
            onSelectPlayer={setPlayerId}
            compareList={compareList}
            onToggleCompare={onToggleCompare}
          />
        ) : loadingHistory ? (
          <div className="p-20 text-center text-slate-400 dark:text-slate-500 animate-pulse font-bold">
            Loading Season History...
          </div>
        ) : viewMode === 'seasons' ? (
          <SeasonStatsTable
            history={history}
            onOpenSeason={(season) => {
              setSelectedSeason(season);
              setViewMode('log');
            }}
          />
        ) : rows.length === 0 ? (
          <div className="p-20 text-center text-slate-400 dark:text-slate-500 font-bold">
            No games for {currentSeason?.season || 'this season'} yet.
          </div>
        ) : viewMode === 'visual' ? (
          <VisualsPanel main={mainSeries} extraIds={extraCompareIds} onRemove={onToggleCompare} />
        ) : (
          <GameLogTable rows={rows} pointColor={pointColor} avgPoints={avgPointsNum} />
        )}
      </div>
    </div>
  );
}
