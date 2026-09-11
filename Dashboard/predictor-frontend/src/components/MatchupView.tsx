import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Activity, Users, TrendingUp, ListOrdered } from 'lucide-react';
import MatchupBanner from './MatchupBanner';
import PlayerCard from './PlayerCard';
import PlayerModal from './PlayerModal';
import MatchupInsights from './MatchupInsights';
import { GameCard, gameKey } from './GameRanksView';
import type { FetchedMatchup } from './GameRanksView';
import { MatchupSkeleton } from './Skeleton';
import { getTeamColor } from '../utils/nflColors';
import type { MatchupData, InjuryData, ScheduleGame } from '../hooks/useNflData';
import type { PlayerData } from '../types';

interface MatchupViewProps {
  week: number;
  home: string;
  away: string;
  onBack: () => void;
  /** Lets the host header's Back button pop an in-page tab before leaving the view. */
  onInnerNav?: (entry: { label: string; back: () => void } | null) => void;
  compareList: string[];
  onToggleCompare: (id: string) => void;
  onOpenHistory?: (id: string) => void;
}

type PositionFilter = 'ALL' | 'QB' | 'RB' | 'WR' | 'TE';
type ViewTab = 'ROSTER' | 'INJURIES' | 'INSIGHTS' | 'RANK';
type InjuryFilter = 'ALL' | 'OFFENSE' | 'DEFENSE' | 'SKILL';

const InjuryCard = React.memo(({ player }: { player: InjuryData }) => {
    const getStatusColor = (status: string) => {
        const s = status.toLowerCase();
        if (s.includes('out') || s.includes('ir')) return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400';
        if (s.includes('doubtful')) return 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400';
        if (s.includes('questionable')) return 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400';
        return 'bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-400';
    };

    return (
        <div className="flex items-center justify-between p-3 bg-white dark:bg-slate-900 rounded-lg border border-slate-200 dark:border-slate-700 shadow-sm">
            <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-full bg-slate-100 dark:bg-slate-800 overflow-hidden flex items-center justify-center border border-slate-200 dark:border-slate-700">
                    {player.headshot ? (
                        <img src={player.headshot} alt={player.name} className="w-full h-full object-cover" />
                    ) : (
                        <span className="text-xs font-bold text-slate-400">{player.position}</span>
                    )}
                </div>
                <div>
                    <div className="font-bold text-sm text-slate-800 dark:text-slate-200">{player.name}</div>
                    <div className="text-xs text-slate-500 flex items-center gap-2">
                        <span className="font-mono font-bold bg-slate-100 dark:bg-slate-800 px-1 rounded">{player.position}</span>
                        <span>•</span>
                        <span>{player.avg_snaps} snaps {player.avg_pct !== undefined ? `(${player.avg_pct}%)` : ''}</span>
                    </div>
                </div>
            </div>
            <div className={`px-2 py-1 rounded text-[10px] uppercase font-black tracking-wider ${getStatusColor(player.status)}`}>
                {player.status}
            </div>
        </div>
    );
});

const MatchupView: React.FC<MatchupViewProps> = ({ week, home, away, compareList, onToggleCompare, onOpenHistory, onInnerNav }) => {
  const [data, setData] = useState<MatchupData | null>(null);
  const [selectedPlayer, setSelectedPlayer] = useState<PlayerData | null>(null);
  const [loading, setLoading] = useState(true);
  const [filterPos, setFilterPos] = useState<PositionFilter>('ALL');
  const [activeTab, setActiveTab] = useState<ViewTab>('ROSTER');
  // Tabs are real navigation, not just a filter: switching to Rank/Injuries/Insights
  // pushes the tab you came from so the header Back button returns you to the roster
  // instead of skipping the whole game page and landing on the schedule.
  const [tabStack, setTabStack] = useState<ViewTab[]>([]);
  const goToTab = useCallback((next: ViewTab) => {
    if (next === activeTab) return;
    setTabStack((prev) => [...prev, activeTab]);
    setActiveTab(next);
  }, [activeTab]);
  const tabBack = useCallback(() => {
    if (tabStack.length === 0) return;
    setActiveTab(tabStack[tabStack.length - 1]);
    setTabStack((prev) => prev.slice(0, -1));
  }, [tabStack]);
  useEffect(() => {
    if (!onInnerNav) return;
    onInnerNav(
      tabStack.length > 0
        ? { label: tabStack[tabStack.length - 1].toLowerCase(), back: tabBack }
        : null,
    );
    return () => onInnerNav(null);
  }, [tabStack, tabBack, onInnerNav]);
  const [injuryFilter, setInjuryFilter] = useState<InjuryFilter>('ALL');

  useEffect(() => {
    import('../lib/api').then(({ fetchMatchup }) => {
      fetchMatchup(week, home, away)
        .then(d => {
          setData(d);
          setLoading(false);
        })
        .catch(err => { console.error("Matchup Fetch Error:", err); setLoading(false); });
    }).catch(err => { console.error(err); setLoading(false); });
  }, [week, home, away]);

  // Hooks must run unconditionally, before the loading/empty-data early
  // returns below — these previously ran as plain per-render computations
  // (not memoized), which meant every unrelated re-render (opening the
  // player modal, toggling a filter for the OTHER tab, etc.) re-sorted and
  // re-filtered rosters and injury reports that hadn't actually changed.
  // That churn is worse now that the injury filter fix below roughly
  // triples the O-line/defense rows in each team's injury list.
  // Starters first (QB, RB, WR, TE), then everyone else in the same positional
  // order, projection-descending inside each bucket.
  //
  // This used to sort on average_points descending, which threw away the
  // starter-aware order the API returns AND is meaningless early in a season
  // when every average_points is still 0 - the comparator returned 0 for every
  // pair, so the list fell back to arrival order and backups floated to the
  // top of their position group.
  const POSITION_ORDER: Record<string, number> = { QB: 1, RB: 2, WR: 3, TE: 4 };
  const rosterSortKey = (p: PlayerData) => [
    p.is_starter ? 0 : 1,
    POSITION_ORDER[p.position] ?? 99,
    -(p.prediction || 0),
  ];
  const byRosterOrder = (a: PlayerData, b: PlayerData) => {
    const ka = rosterSortKey(a);
    const kb = rosterSortKey(b);
    for (let i = 0; i < ka.length; i++) {
      if (ka[i] !== kb[i]) return ka[i] - kb[i];
    }
    return (a.player_name || '').localeCompare(b.player_name || '');
  };

  const homeRoster = useMemo(() => {
    let processed = [...(data?.home_roster ?? [])];
    if (filterPos !== 'ALL') processed = processed.filter(p => p.position === filterPos);
    return processed.sort(byRosterOrder);
  }, [data?.home_roster, filterPos]);

  const awayRoster = useMemo(() => {
    let processed = [...(data?.away_roster ?? [])];
    if (filterPos !== 'ALL') processed = processed.filter(p => p.position === filterPos);
    return processed.sort(byRosterOrder);
  }, [data?.away_roster, filterPos]);

  const filterInjuries = (injuries: InjuryData[] | undefined) => {
    if (!injuries) return [];
    return injuries.filter(p => {
        if (injuryFilter === 'ALL') return true;
        if (injuryFilter === 'OFFENSE') return ['T', 'G', 'C', 'OT', 'OG', 'OL'].includes(p.position);
        if (injuryFilter === 'DEFENSE') return ['DE', 'DT', 'LB', 'CB', 'S', 'DB', 'ILB', 'OLB', 'NT', 'SS', 'FS', 'DL', 'EDGE'].includes(p.position);
        if (injuryFilter === 'SKILL') return ['QB', 'RB', 'WR', 'TE', 'FB'].includes(p.position);
        return true;
    }).sort((a, b) => b.avg_snaps - a.avg_snaps);
  };

  const homeInjuries = useMemo(() => filterInjuries(data?.home_injuries), [data?.home_injuries, injuryFilter]);
  const awayInjuries = useMemo(() => filterInjuries(data?.away_injuries), [data?.away_injuries, injuryFilter]);

  const rankGame: ScheduleGame | null = data ? {
    home_team: home,
    away_team: away,
    gameday: data.gameday,
    gametime: data.gametime,
    game_total: data.over_under ?? undefined,
  } : null;
  // Already fetched for the Roster tab — hand it straight to the Rank tab's
  // board instead of letting it re-fetch the same matchup from scratch.
  const preloadedMatchup: FetchedMatchup | null = useMemo(() => data ? {
    home_roster: data.home_roster,
    away_roster: data.away_roster,
    weather: data.weather ?? null,
    home_rankings: data.home_rankings ?? null,
    away_rankings: data.away_rankings ?? null,
    game_script: data.game_script ?? null,
    over_under: data.over_under ?? null,
    spread: data.spread ?? null,
    home_win_prob: data.home_win_prob ?? null,
    away_win_prob: data.away_win_prob ?? null,
  } : null, [data]);

  if (loading) return <MatchupSkeleton />;
  if (!data || !rankGame || !preloadedMatchup) return <div className="p-10 text-center text-slate-400">Matchup Data Unavailable</div>;

  const viewTabs = (
    <div className="flex bg-slate-200 dark:bg-slate-800 p-1 rounded-lg">
        <button
            onClick={() => goToTab('ROSTER')}
            data-testid="tab-ROSTER"
            data-active={activeTab === 'ROSTER'}
            className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11px] font-bold transition-all ${activeTab === 'ROSTER' ? 'bg-white dark:bg-slate-700 shadow text-blue-600 dark:text-blue-400' : 'text-slate-500 hover:text-slate-700 dark:text-slate-400'}`}
        >
            <Users size={12} /> Roster
        </button>
        <button
            onClick={() => goToTab('INJURIES')}
            data-testid="tab-INJURIES"
            data-active={activeTab === 'INJURIES'}
            className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11px] font-bold transition-all ${activeTab === 'INJURIES' ? 'bg-white dark:bg-slate-700 shadow text-red-600 dark:text-red-400' : 'text-slate-500 hover:text-slate-700 dark:text-slate-400'}`}
        >
            <Activity size={12} /> Injuries
        </button>
        <button
            onClick={() => goToTab('INSIGHTS')}
            data-testid="tab-INSIGHTS"
            data-active={activeTab === 'INSIGHTS'}
            className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11px] font-bold transition-all ${activeTab === 'INSIGHTS' ? 'bg-white dark:bg-slate-700 shadow text-green-600 dark:text-green-400' : 'text-slate-500 hover:text-slate-700 dark:text-slate-400'}`}
        >
            <TrendingUp size={12} /> Insights
        </button>
        <button
            onClick={() => goToTab('RANK')}
            data-testid="tab-RANK"
            data-active={activeTab === 'RANK'}
            className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11px] font-bold transition-all ${activeTab === 'RANK' ? 'bg-white dark:bg-slate-700 shadow text-purple-600 dark:text-purple-400' : 'text-slate-500 hover:text-slate-700 dark:text-slate-400'}`}
        >
            <ListOrdered size={12} /> Rank
        </button>
    </div>
  );

  return (
    <div className="animate-in fade-in slide-in-from-bottom-4 duration-500 h-full flex flex-col relative bg-slate-50 dark:bg-slate-950 transition-colors duration-300">

      <div
        className={`mb-1 rounded-xl overflow-hidden shadow-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 shrink-0 ${
          activeTab === 'ROSTER' ? 'mr-10 lg:mr-12 xl:mr-14' : ''
        }`}
      >
        <MatchupBanner
          matchup={data.matchup}
          gameTime={data.gametime}
          gameDay={data.gameday}
          overUnder={data.over_under || null}
          spread={data.spread || null}
          homeWinProb={data.home_win_prob || null}
          awayWinProb={data.away_win_prob || null}
          weather={data.weather ?? null}
          gameScript={data.game_script ?? null}
          tabs={viewTabs}
        />
      </div>

      <div className="flex flex-1 min-h-0 relative overflow-hidden">
        <div className={`flex-1 overflow-y-auto overscroll-contain pb-4 ${activeTab === 'ROSTER' ? 'pr-10 lg:pr-12 xl:pr-14' : ''}`} style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}>
          <style>{`.hide-scrollbar::-webkit-scrollbar { display: none; }`}</style>
          
          {activeTab === 'ROSTER' ? (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-x-8 gap-y-4">
                <div>
                <div className="flex items-center justify-between mb-2 border-b border-slate-200 dark:border-slate-700 pb-1 sticky top-0 bg-slate-50/95 dark:bg-slate-950/95 backdrop-blur z-20 pt-1">
                    <h3 className="text-xl font-black text-slate-800 dark:text-slate-100">{away}</h3>
                    <span className="text-[10px] font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest">{filterPos}</span>
                </div>
                <div className="space-y-3">
                    {awayRoster.map(p => (
                        // content-visibility skips layout/paint for cards scrolled out of
                        // view — a full two-team roster can run ~180 cards deep, and that
                        // was the single biggest cost behind sluggish/inconsistent scroll.
                        //
                        // The `auto` in contain-intrinsic-size is load-bearing: PlayerCard is
                        // min-h-[9rem] h-auto and its prop rows render conditionally, so real
                        // heights range ~144-260px. A fixed placeholder guess resolves to a
                        // different height on every reveal, which shifts everything below it
                        // and makes scrolling back up feel like content is snapping into
                        // place. `auto` caches each card's last real size so it only pays
                        // that correction once.
                        <div key={p.player_id} style={{ contentVisibility: 'auto', containIntrinsicSize: 'auto 176px' }}>
                            <PlayerCard
                                data={p}
                                teamColor={getTeamColor(away)}
                                onClick={setSelectedPlayer}
                                isSelected={compareList.includes(p.player_id)}
                                onToggleCompare={onToggleCompare}
                            />
                        </div>
                    ))}
                </div>
                </div>

                <div>
                <div className="flex items-center justify-between mb-2 border-b border-slate-200 dark:border-slate-700 pb-1 sticky top-0 bg-slate-50/95 dark:bg-slate-950/95 backdrop-blur z-20 pt-1">
                    <h3 className="text-xl font-black text-slate-800 dark:text-slate-100">{home}</h3>
                    <span className="text-[10px] font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest">{filterPos}</span>
                </div>
                <div className="space-y-3">
                    {homeRoster.map(p => (
                        <div key={p.player_id} style={{ contentVisibility: 'auto', containIntrinsicSize: 'auto 176px' }}>
                            <PlayerCard
                                data={p}
                                teamColor={getTeamColor(home)}
                                onClick={setSelectedPlayer}
                                isSelected={compareList.includes(p.player_id)}
                                onToggleCompare={onToggleCompare}
                            />
                        </div>
                    ))}
                </div>
                </div>
            </div>
          ) : activeTab === 'INJURIES' ? (
            <div className="space-y-6">
                {/* Injury Filters */}
                <div className="flex justify-center gap-2 sticky top-0 bg-slate-50/95 dark:bg-slate-950/95 backdrop-blur z-20 py-2 border-b border-slate-200 dark:border-slate-700">
                    {(['ALL', 'OFFENSE', 'DEFENSE', 'SKILL'] as InjuryFilter[]).map(f => (
                        <button
                            key={f}
                            onClick={() => setInjuryFilter(f)}
                            className={`px-3 py-1 rounded-full text-[10px] font-bold uppercase tracking-wider transition-colors ${injuryFilter === f ? 'bg-slate-800 text-white dark:bg-slate-200 dark:text-slate-900' : 'bg-slate-200 text-slate-500 dark:bg-slate-800 dark:text-slate-400 hover:bg-slate-300 dark:hover:bg-slate-700'}`}
                        >
                            {f === 'OFFENSE' ? 'O-Line' : f}
                        </button>
                    ))}
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-x-8 gap-y-8">
                    <div>
                        <h3 className="text-lg font-black text-slate-800 dark:text-slate-100 mb-4 flex items-center gap-2">
                            {away} <span className="text-xs font-normal text-slate-400 uppercase">Injury Report</span>
                        </h3>
                        <div className="space-y-2">
                            {awayInjuries.length > 0 ? awayInjuries.map(p => (
                                <div key={p.player_id} style={{ contentVisibility: 'auto', containIntrinsicSize: 'auto 68px' }}>
                                    <InjuryCard player={p} />
                                </div>
                            )) : (
                                <div className="text-center py-8 text-slate-400 text-sm italic">No injuries reported</div>
                            )}
                        </div>
                    </div>
                    <div>
                        <h3 className="text-lg font-black text-slate-800 dark:text-slate-100 mb-4 flex items-center gap-2">
                            {home} <span className="text-xs font-normal text-slate-400 uppercase">Injury Report</span>
                        </h3>
                        <div className="space-y-2">
                            {homeInjuries.length > 0 ? homeInjuries.map(p => (
                                <div key={p.player_id} style={{ contentVisibility: 'auto', containIntrinsicSize: 'auto 68px' }}>
                                    <InjuryCard player={p} />
                                </div>
                            )) : (
                                <div className="text-center py-8 text-slate-400 text-sm italic">No injuries reported</div>
                            )}
                        </div>
                    </div>
                </div>
            </div>
          ) : activeTab === 'INSIGHTS' ? (
            <MatchupInsights week={week} home={home} away={away} />
          ) : (
            <GameCard
              key={gameKey(week, away, home)}
              game={rankGame}
              week={week}
              expanded
              onToggleExpand={() => undefined}
              onToggleCompare={onToggleCompare}
              onOpenHistory={onOpenHistory || (() => undefined)}
              compareList={compareList}
              preloadedMatchup={preloadedMatchup}
              bannerless
            />
          )}

        </div>

        {activeTab === 'ROSTER' && (
            <div className="absolute right-0 top-1/2 -translate-y-1/2 flex flex-col gap-0 z-50">
            {(['ALL', 'QB', 'RB', 'WR', 'TE'] as PositionFilter[]).map((pos) => (
                <button key={pos} onClick={() => setFilterPos(pos)} className={`h-10 w-8 lg:h-12 lg:w-9 xl:h-14 xl:w-10 text-[8px] lg:text-[9px] xl:text-[10px] font-black tracking-widest flex items-center justify-center transition-all duration-200 border-y border-l rounded-l-lg border-r-0 shadow-sm ${filterPos === pos ? 'bg-blue-600 text-white border-blue-500 w-10 lg:w-11 xl:w-12 shadow-lg z-20' : 'bg-white dark:bg-slate-800 text-slate-400 dark:text-slate-500 border-slate-200 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-700 z-10'} [writing-mode:vertical-rl] rotate-180 mb-[-1px]`}>{pos}</button>
            ))}
            </div>
        )}
      </div>

      {selectedPlayer && <PlayerModal player={selectedPlayer} onClose={() => setSelectedPlayer(null)} />}
    </div>
  );
};

export default MatchupView;