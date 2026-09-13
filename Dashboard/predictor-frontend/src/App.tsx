import { useState, useEffect, useCallback } from 'react';
import { Search, BarChart2, PanelLeft, Minimize2, TrendingUp, TrendingDown, Sun, Moon, Plus, Check, Calendar, Trophy, Menu, Layers, ArrowLeft, Shield, ListOrdered, Users } from 'lucide-react';
import { usePastRankings, useFutureRankings, useSchedule, useCurrentWeek } from './hooks/useNflData';
import type { Player } from './hooks/useNflData';
import PlayerLookupView from './components/PlayerLookup';
import SidePanelDrawer from './components/SidePanelDrawer';
import CompareView from './components/CompareView';
import PlayerHistory from './components/PlayerHistory';
import MatchupView from './components/MatchupView';
import MyPicksList from './components/MyPicksList';
import PlayoffView from './components/PlayoffView';
import LiveScoresBar from './components/LiveScoresBar';
import TierListView, { type TierListState } from './components/TierListView';
import TeamOffenseModal from './components/TeamOffenseModal';
import TeamsView from './components/TeamsView';
import GameRanksView from './components/GameRanksView';
import { getTeamColor } from './utils/nflColors';
import { sizedPlayerImage } from './utils/playerImage';
import SleeperView from './components/SleeperView';
import AgentDock from './components/AgentDock';
import AgentPanel from './components/AgentPanel';
import { useAgentScreenContext } from './contexts/AgentScreenContext';
import type { ScreenEntity } from './contexts/AgentScreenContext';
import { useAgentChatContext } from './contexts/AgentChatContext';

// --- HELPER: Status Badge Styles ---
const getStatusColor = (status?: string) => {
  if (!status) return 'bg-gray-100 text-gray-500 border-gray-200 dark:bg-slate-700 dark:text-slate-300 dark:border-slate-600';
  const s = status.toLowerCase();
  
  if (s.includes('out') || s.includes('ir')) return 'bg-red-100 text-red-700 border-red-200 dark:bg-red-900/30 dark:text-red-400 dark:border-red-800';
  if (s.includes('doubtful')) return 'bg-orange-100 text-orange-700 border-orange-200 dark:bg-orange-900/30 dark:text-orange-400 dark:border-orange-800';
  if (s.includes('questionable')) return 'bg-yellow-100 text-yellow-700 border-yellow-200 dark:bg-yellow-900/30 dark:text-yellow-400 dark:border-yellow-800';
  if (s.includes('active')) return 'bg-green-100 text-green-700 border-green-200 dark:bg-green-900/30 dark:text-green-400 dark:border-green-800';
  
  return 'bg-slate-100 text-slate-500 border-slate-200 dark:bg-slate-700 dark:text-slate-300 dark:border-slate-600';
};

const getStatusLabel = (status?: string) => {
  if (!status) return 'ACT';
  const s = status.toLowerCase();
  if (s.includes('out')) return 'OUT';
  if (s.includes('ir')) return 'IR';
  if (s.includes('doubtful')) return 'D';
  if (s.includes('questionable')) return 'Q';
  if (s.includes('active')) return 'ACT';
  return status.substring(0, 3).toUpperCase();
};

// --- HELPER: Format Trending Count ---
const formatTrendingCount = (count: number): string => {
  if (count >= 1000) {
    const k = count / 1000;
    // Show one decimal for counts like 6.7K, but not for 10K+
    return k >= 10 ? `${Math.round(k)}K` : `${k.toFixed(1)}K`;
  }
  return count.toString();
};

// --- COMPONENT: Sidebar Player Item ---
const SidebarPlayerItem = ({ 
    player, 
    type, 
    onClick, 
    onToggleCompare, 
    isSelected 
}: { 
    player: Player, 
    type: 'up' | 'down', 
    onClick?: (id: string) => void,
    onToggleCompare?: (id: string) => void,
    isSelected?: boolean
}) => {
  const statusLabel = getStatusLabel(player.injury_status);
  const statusColor = getStatusColor(player.injury_status);

  return (
    <div className="group relative bg-white dark:bg-slate-800 p-3 rounded-lg shadow-sm border border-slate-200 dark:border-slate-700 flex items-start gap-3 mb-2 transition-shadow hover:shadow-md hover:ring-2 hover:ring-blue-50 dark:hover:ring-blue-900 cursor-pointer">
      <div className="flex-1 flex gap-3 min-w-0" onClick={() => onClick && onClick(player.player_id)}>
          <div className="w-10 h-10 bg-slate-100 dark:bg-slate-700 rounded-full overflow-hidden border border-slate-200 dark:border-slate-600 shrink-0 relative">
            {player.image ? (
              <img src={sizedPlayerImage(player.image, 40)} alt={player.player_name} className="object-cover w-full h-full" decoding="async" />
            ) : (
              <div className="w-full h-full flex items-center justify-center text-slate-300 dark:text-slate-500 text-xs">IMG</div>
            )}
          </div>
          
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
                <p className="font-bold text-sm truncate text-slate-800 dark:text-slate-100">{player.player_name}</p>
                <span className={`text-[9px] font-black px-1.5 py-0.5 rounded border ${statusColor}`}>
                    {statusLabel}
                </span>
            </div>
            <p className="text-[10px] uppercase font-semibold text-slate-400 dark:text-slate-500">{player.position} • {player.team}</p>
          </div>
      </div>

      <div className="flex flex-col items-end gap-1">
        <div className={`text-xs font-black px-2 py-1 rounded-full ${type === 'up' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400'}`}>
            {type === 'up' ? '+' : '-'}{formatTrendingCount(player.trending_count || 0)}
        </div>
        
        {/* Compare Button */}
        <button 
            onClick={(e) => { e.stopPropagation(); if (onToggleCompare) onToggleCompare(player.player_id); }}
            className={`w-6 h-6 flex items-center justify-center rounded-md transition-all ${
                isSelected 
                ? 'bg-blue-600 text-white shadow-md scale-110' 
                : 'bg-slate-100 dark:bg-slate-700 text-slate-400 hover:bg-slate-200 dark:hover:bg-slate-600'
            }`}
        >
            {isSelected ? <Check size={12} strokeWidth={4} /> : <Plus size={12} strokeWidth={3} />}
        </button>
      </div>
    </div>
  );
};

// --- MAIN APP ---

export default function App() {
  const { currentWeek, loadingWeek } = useCurrentWeek();
  const [sidebarTab, setSidebarTab] = useState<'TRENDING' | 'PICKS'>('TRENDING');
  
  // FIX: Initialize to NULL. Do not default to 1.
  const [activeWeek, setActiveWeek] = useState<number | null>(null);

  useEffect(() => {
    // Only update if we have a valid positive week number
    if (currentWeek && currentWeek > 0) {
        // Schedule setState as a microtask to avoid sync setState-in-effect lint error
        Promise.resolve().then(() => setActiveWeek(currentWeek));
    }
  }, [currentWeek]);

  // If activeWeek is null, pass 0 to hooks so they return empty/loading, not Week 1 data
  const safeWeek = activeWeek || 0; 
  
  type ViewMode = 'SCHEDULE' | 'GAME' | 'LOOKUP' | 'COMPARE' | 'HISTORY' | 'TRENDING' | 'PICKS' | 'PLAYOFFS' | 'TIERS' | 'TEAMS' | 'GAME_RANKS' | 'TEAM_PAGE' | 'MY_TEAM';
  const [viewMode, setViewModeRaw] = useState<ViewMode>('SCHEDULE');

  // Navigation stack: every setViewMode() that actually changes view pushes the
  // previous view. goBack() pops. Powers the persistent Back button in the header
  // so users always have a single-click escape route.
  const [navStack, setNavStack] = useState<ViewMode[]>([]);
  // Stash so Back from HISTORY → TIERS reopens the team/player modal the user
  // was looking at when they clicked History.
  type TeamModalTab = 'overview' | 'builder';
  type ModalReturn = { team: string; focusPlayerId?: string | null; detailPlayerId?: string | null; activeTab?: TeamModalTab };
  const [modalReturn, setModalReturn] = useState<ModalReturn | null>(null);
  const setViewMode = useCallback((next: ViewMode) => {
    if (next !== viewMode) {
      setNavStack((prev) => (prev[prev.length - 1] === viewMode ? prev : [...prev, viewMode]));
    }
    setViewModeRaw(next);
  }, [viewMode]);
  // A mounted view can claim the Back button for its own in-page steps (e.g. the
  // game page's Roster/Injuries/Insights/Rank tabs). While claimed, Back pops that
  // step instead of leaving the view entirely.
  const [innerNav, setInnerNav] = useState<{ label: string; back: () => void } | null>(null);
  const handleInnerNav = useCallback(
    (entry: { label: string; back: () => void } | null) => setInnerNav(entry),
    [],
  );
  const goBack = () => {
    if (innerNav) {
      innerNav.back();
      return;
    }
    setNavStack((prev) => {
      if (prev.length === 0) return prev;
      const last = prev[prev.length - 1];
      setViewModeRaw(last);
      // If we're returning to a team-context view and we stashed a modal, restore it.
      if ((last === 'TIERS' || last === 'TEAMS' || last === 'TEAM_PAGE') && modalReturn) {
        setTeamModal({
          team: modalReturn.team,
          focusPlayerId: modalReturn.focusPlayerId ?? null,
          initialDetailPlayerId: modalReturn.detailPlayerId ?? null,
          initialTab: modalReturn.activeTab ?? 'overview',
        });
        setModalReturn(null);
        if (last !== 'TEAM_PAGE') setViewModeRaw('TEAM_PAGE');
      }
      return prev.slice(0, -1);
    });
  };
  const [showSidebars, setShowSidebars] = useState(true); 
  const [mobileDrawerOpen, setMobileDrawerOpen] = useState(false);
  // The agent's conversation and panel visibility are app-wide: the dock floats
  // over every view and the panel takes the right rail's place.
  const { panelOpen } = useAgentChatContext();
  const { setBase: setAgentScreen } = useAgentScreenContext();
  const [selectedGame, setSelectedGame] = useState<{home: string, away: string} | null>(null);
  const [selectedHistoryId, setSelectedHistoryId] = useState<string | null>(null);
  const [historyFrom, setHistoryFrom] = useState<'SCHEDULE' | 'GAME' | 'LOOKUP' | 'COMPARE' | 'TIERS' | 'TEAMS' | 'GAME_RANKS'>('SCHEDULE');

  const [compareList, setCompareList] = useState<string[]>([]);

  // Tier list state — lifted here so it survives navigation to Compare / History
  // and back. Persisted to localStorage so reloads also restore.
  const [tierState, setTierState] = useState<TierListState>(() => {
    try {
      const raw = localStorage.getItem('tierList.state');
      if (raw) return JSON.parse(raw);
    } catch { /* ignore */ }
    return {
      position: 'WR',
      listName: 'My Tiers',
      assignments: {},
      search: '',
      showRookiesOnly: false,
    };
  });
  useEffect(() => {
    try { localStorage.setItem('tierList.state', JSON.stringify(tierState)); } catch { /* ignore */ }
  }, [tierState]);

  const [teamModal, setTeamModal] = useState<{
    team: string;
    focusPlayerId?: string | null;
    initialDetailPlayerId?: string | null;
    initialTab?: TeamModalTab;
  } | null>(null);

  const toggleCompare = useCallback((playerId: string) => {
    setCompareList(prev => {
        if (prev.includes(playerId)) return prev.filter(id => id !== playerId);
        return [...prev, playerId];
    });
  }, []);

  // GameRanksView and TierListView stay mounted for the whole session (hidden via
  // display:none) so their local state survives navigation. That means an unstable
  // prop re-renders a 300+ card tree even while it is off-screen, so every handler
  // they receive is memoised and both components are wrapped in React.memo.
  const openHistoryFromRanks = useCallback((id: string) => {
    setSelectedHistoryId(id);
    setHistoryFrom('GAME_RANKS');
    setViewMode('HISTORY');
  }, [setViewMode]);

  const openHistoryFromTiers = useCallback((id: string) => {
    setSelectedHistoryId(id);
    setHistoryFrom('TIERS');
    setViewMode('HISTORY');
  }, [setViewMode]);

  const openCompareFromTiers = useCallback(() => setViewMode('COMPARE'), [setViewMode]);
  // Team Overview / Team Builder are destinations now, not dialogs: navigating
  // means the header Back button and the nav stack work on them like any other
  // view, and the trending sidebars stay togglable instead of being covered.
  const openTeamPage = useCallback(
    (team: string, focusPlayerId?: string | null, tab: TeamModalTab = 'overview') => {
      setTeamModal({ team, focusPlayerId, initialTab: tab });
      setViewMode('TEAM_PAGE');
    },
    [setViewMode],
  );

  const openTeamFromTiers = useCallback(
    (team: string, focusPlayerId?: string | null) => openTeamPage(team, focusPlayerId),
    [openTeamPage],
  );

  useEffect(() => {
    if (viewMode === 'COMPARE' && compareList.length > 2) {
        // Schedule update as microtask to avoid sync setState-in-effect lint error
        Promise.resolve().then(() => setShowSidebars(false));
    }
  }, [viewMode, compareList.length]);

  const [isDarkMode, setIsDarkMode] = useState(() => {
    if (typeof window !== 'undefined') {
      return localStorage.getItem('theme') === 'dark' || (!('theme' in localStorage) && window.matchMedia('(prefers-color-scheme: dark)').matches);
    }
    return false;
  });

  useEffect(() => {
    if (isDarkMode) {
      document.documentElement.classList.add('dark');
      localStorage.setItem('theme', 'dark');
    } else {
      document.documentElement.classList.remove('dark');
      localStorage.setItem('theme', 'light');
    }
  }, [isDarkMode]);

  // FIX: Syncing screen shows if we don't have an active week yet.
  // This prevents the UI from flashing Week 1 before the real week loads.
  const isSyncing = loadingWeek && !activeWeek;

  // Hooks use safeWeek (0 if null). 0 causes them to return empty data, which is what we want.
  const { pastRankings: trendingDown, loadingPast: loadingDown } = usePastRankings(safeWeek > 1 ? safeWeek - 1 : 1);
  const { futureRankings: trendingUp, loadingFuture: loadingUp } = useFutureRankings(safeWeek);
  const { games, loadingSchedule } = useSchedule(safeWeek);

  // Tell the agent which page it is being asked about. This is the coarse
  // layer -- a mounted view that knows more (the game page knows both teams,
  // a player page knows the player) refines it with useAgentScreen().
  useEffect(() => {
    const facts: string[] = [];
    const entities: ScreenEntity[] = [];
    let title = '';

    switch (viewMode) {
      case 'SCHEDULE':
        title = 'the weekly schedule';
        break;
      case 'GAME':
        title = selectedGame ? `the ${selectedGame.away} at ${selectedGame.home} game page` : 'a game page';
        if (selectedGame) {
          entities.push({ type: 'team', name: selectedGame.away, detail: 'away' });
          entities.push({ type: 'team', name: selectedGame.home, detail: 'home' });
        }
        break;
      case 'GAME_RANKS':
        title = 'the start/sit ranks board';
        break;
      case 'HISTORY':
        title = 'a player game log';
        if (selectedHistoryId) facts.push(`player id ${selectedHistoryId}`);
        break;
      case 'COMPARE':
        title = 'the player comparison view';
        if (compareList.length) facts.push(`comparing player ids ${compareList.join(', ')}`);
        break;
      case 'TIERS':
        title = 'the tier list';
        break;
      case 'TEAMS':
        title = 'the team index';
        break;
      case 'TEAM_PAGE':
        title = teamModal ? `the ${teamModal.team} team page` : 'a team page';
        if (teamModal) entities.push({ type: 'team', name: teamModal.team });
        break;
      case 'MY_TEAM':
        title = 'their own fantasy team (Sleeper)';
        break;
      case 'PLAYOFFS':
        title = 'the playoff picture';
        break;
      case 'LOOKUP':
        title = 'player lookup';
        break;
      default:
        title = viewMode.replace(/_/g, ' ').toLowerCase();
    }

    setAgentScreen({ view: viewMode, title, week: activeWeek, facts, entities });
  }, [viewMode, activeWeek, selectedGame, selectedHistoryId, compareList, teamModal, setAgentScreen]);

  if (isSyncing) {
    return (
      <div className="flex h-screen items-center justify-center bg-slate-50 dark:bg-slate-900 transition-colors duration-300">
         <div className="flex flex-col items-center gap-4">
            <div className="w-8 h-8 border-4 border-blue-600 border-t-transparent rounded-full animate-spin"></div>
            <p className="text-slate-400 font-bold animate-pulse">Syncing with NFL Season...</p>
         </div>
      </div>
    );
  }

  // Shown in whichever right-rail header is mounted, trending or agent.
  const railControls = (
    <div className="flex items-center gap-1 bg-white dark:bg-slate-800 rounded-lg p-1 border border-slate-200 dark:border-slate-700 shadow-sm">
      <button onClick={() => setIsDarkMode(!isDarkMode)} className="p-1.5 text-slate-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-md transition-colors">
        {isDarkMode ? <Sun size={14} /> : <Moon size={14} />}
      </button>
      <div className="text-[10px] font-black text-slate-900 dark:text-slate-100 px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-700 whitespace-nowrap">
        Wk {activeWeek || "-"}
      </div>
    </div>
  );
  const rightRailVisible = showSidebars && viewMode !== 'TIERS' && viewMode !== 'TEAMS';

  return (
    <div className="flex h-screen bg-slate-100 dark:bg-slate-900 font-sans text-slate-900 dark:text-slate-100 overflow-hidden transition-colors duration-300">
      
      {/* LEFT SIDEBAR (hidden where the main view needs the full width) */}
      {showSidebars && viewMode !== 'TIERS' && viewMode !== 'TEAMS' && (
        <aside className="w-80 bg-white dark:bg-slate-800 border-r border-slate-200 dark:border-slate-700 flex flex-col z-20 shadow-[4px_0_24px_rgba(0,0,0,0.02)] shrink-0 hidden xl:flex transition-colors duration-300">
          <div className="p-4 border-b border-slate-100 dark:border-slate-700 bg-slate-50/50 dark:bg-slate-800/50 backdrop-blur">
             <div className="flex items-center justify-between mb-3">
                {/* Title Area */}
                <div className="flex items-center gap-2 text-red-600 dark:text-red-400">
                    <TrendingDown size={16} />
                    <h2 className="text-xs font-black uppercase tracking-widest">Trending Down</h2>
                </div>

                {/* My Picks Toggle */}
                <div className="flex items-center gap-1 bg-white dark:bg-slate-800 rounded-lg p-1 border border-slate-200 dark:border-slate-700 shadow-sm">
                    <button 
                        onClick={() => setSidebarTab(sidebarTab === 'PICKS' ? 'TRENDING' : 'PICKS')}
                        className={`flex items-center gap-1.5 px-2 py-1 rounded-md text-[10px] font-bold transition-all ${sidebarTab === 'PICKS' ? 'bg-blue-600 text-white shadow-sm' : 'text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200'}`}
                    >
                        <Check size={12} strokeWidth={3} />
                        <span>My Picks</span>
                    </button>
                </div>
             </div>

             {/* Subtitle / Description */}
             {sidebarTab === 'TRENDING' ? (
                <p className="text-xs text-slate-400 dark:text-slate-500">Most Dropped Players (24h)</p>
             ) : (
                <p className="text-xs text-slate-400 dark:text-slate-500">Your Saved Predictions</p>
             )}
          </div>
          <div className="flex-1 overflow-y-auto p-4 scrollbar-thin dark:scrollbar-thumb-slate-600 dark:scrollbar-track-slate-800">
            {sidebarTab === 'TRENDING' ? (
                loadingDown ? <p className="text-xs text-slate-400 text-center mt-10">Scanning Market...</p> : 
                trendingDown.map(p => (
                    <SidebarPlayerItem 
                        key={p.player_id} 
                        player={p} 
                        type="down" 
                        isSelected={compareList.includes(p.player_id)}
                        onToggleCompare={toggleCompare}
                        onClick={(id) => { 
                            setSelectedHistoryId(id); 
                            setHistoryFrom('SCHEDULE'); 
                            setViewMode('HISTORY'); 
                        }} 
                    />
                ))
            ) : (
                <MyPicksList currentWeek={activeWeek} />
            )}
          </div>
        </aside>
      )}

      {/* CENTER STAGE */}
      <main className="flex-1 flex flex-col relative min-w-0 bg-slate-50 dark:bg-slate-950 transition-colors duration-300">
        
        {/* HEADER */}
        <header className="h-16 bg-white/80 dark:bg-slate-950/80 backdrop-blur-md border-b border-slate-200 dark:border-slate-800 flex items-center justify-between px-6 shadow-sm sticky top-0 z-30 transition-colors duration-300">
          
          <div className="flex items-center gap-1 pr-8">
             <button
                onClick={() => setShowSidebars(!showSidebars)}
                aria-label={showSidebars ? 'Collapse side panels' : 'Expand side panels'}
                className="hidden xl:block p-2 text-slate-400 hover:text-blue-600 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg transition-colors"
             >
                {showSidebars ? <Minimize2 size={20} /> : <PanelLeft size={20} />}
             </button>

             {(innerNav || navStack.length > 0) && (() => {
               const backLabel = (innerNav ? innerNav.label : navStack[navStack.length - 1]).toLowerCase();
               return (
               <button
                 onClick={goBack}
                 data-testid="header-back"
                 data-back-to={backLabel}
                 className="flex items-center gap-1 px-2 py-1.5 mr-1 text-xs font-bold text-slate-500 dark:text-slate-300 bg-slate-100 dark:bg-slate-800 hover:bg-blue-100 dark:hover:bg-blue-900/30 hover:text-blue-600 dark:hover:text-blue-400 rounded-lg transition"
                 title={`Back to ${backLabel}`}
               >
                 <ArrowLeft size={14} />
                 <span className="hidden sm:inline">Back</span>
                 <span className="hidden md:inline text-slate-400 dark:text-slate-500 font-mono text-[10px] normal-case">
                   · {backLabel}
                 </span>
               </button>
               );
             })()}

             <div className="font-black text-xl italic tracking-tighter select-none cursor-pointer hidden sm:flex items-center gap-1 z-50 relative whitespace-nowrap" onClick={() => setViewMode('SCHEDULE')}>
                <span className="text-2xl font-black text-slate-800 dark:text-slate-100">THE SPOT</span>
                <span className="text-2xl font-black text-blue-600 dark:text-blue-500">AI</span>
                <span className="sr-only">The Spot Ai</span>
             </div>
          </div>
          
          <div className="flex items-center gap-4 z-20 relative">
            <div className="hidden sm:flex gap-2 bg-slate-100 dark:bg-slate-800/50 p-1 rounded-lg border border-slate-200/50 dark:border-slate-700/50" role="tablist" aria-label="Main navigation tabs">
              {(['SCHEDULE', 'PLAYOFFS', 'TEAMS', 'TIERS', 'GAME_RANKS', 'MY_TEAM', 'COMPARE', 'LOOKUP'] as const).map((mode) => (
                <button key={mode} onClick={() => setViewMode(mode)} title={mode === 'GAME_RANKS' ? 'Ranks' : mode === 'MY_TEAM' ? 'My team' : mode.charAt(0) + mode.slice(1).toLowerCase()} className={`px-3 py-1.5 rounded-md text-xs font-bold flex items-center gap-2 whitespace-nowrap transition-all ${viewMode === mode ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm ring-1 ring-black/5 dark:ring-white/5' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`}>
                  {mode === 'SCHEDULE' && <BarChart2 size={14}/>}
                  {mode === 'PLAYOFFS' && <Trophy size={14}/>}
                  {mode === 'TEAMS' && <Shield size={14}/>}
                  {mode === 'TIERS' && <Layers size={14}/>}
                  {mode === 'GAME_RANKS' && <ListOrdered size={14}/>}
                  {mode === 'MY_TEAM' && <Users size={14}/>}
                  {mode === 'COMPARE' && (
                      <div className="flex items-center gap-1">
                          <BarChart2 size={14}/>
                          {compareList.length > 0 && <span className="bg-blue-600 text-white text-[9px] px-1.5 rounded-full">{compareList.length}</span>}
                      </div>
                  )}
                  {mode === 'LOOKUP' && <Search size={14}/>}
                  {/* With both 20rem rails open the header only has the width for
                      labels from ~1900px; at 2xl (1536px) they overflowed into the
                      right rail and "MY TEAM" wrapped. Icons carry a title instead. */}
                  <span className={`${showSidebars ? 'hidden min-[1900px]:inline' : 'hidden lg:inline'}`}>{mode === 'COMPARE' ? 'COMPARE' : mode === 'GAME_RANKS' ? 'RANKS' : mode === 'MY_TEAM' ? 'MY TEAM' : mode}</span>
                </button>
              ))}
            </div>

            <div className={`flex items-center gap-2 bg-white dark:bg-slate-800 rounded-lg p-1 border border-slate-200 dark:border-slate-700 shadow-sm ${showSidebars ? 'lg:hidden' : ''}`}>
                <button onClick={() => setIsDarkMode(!isDarkMode)} className="p-1.5 text-slate-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-md transition-colors">
                  {isDarkMode ? <Sun size={16} /> : <Moon size={16} />}
                </button>
                
                <div className="text-xs font-black text-slate-900 dark:text-slate-100 px-2 py-1 rounded bg-slate-100 dark:bg-slate-700">
                    Wk {activeWeek || "-"}
                </div>
            </div>

            {/* Mobile: Menu access (hidden on lg screens where sidebars exist) */}
            <button onClick={() => setMobileDrawerOpen(true)} className="p-2 text-slate-400 hover:text-blue-600 lg:hidden rounded-lg transition-colors" aria-label="Open Menu">
              <Menu size={18} />
            </button>
          </div>
        </header>

        {/* LIVE SCORES TICKER */}
        {activeWeek && (viewMode === 'SCHEDULE' || viewMode === 'GAME') && (
          <LiveScoresBar 
            week={activeWeek} 
            onGameClick={(home, away) => {
              setSelectedGame({ home, away });
              setViewMode('GAME');
            }}
          />
        )}

        {/* Mobile Drawer */}
        <SidePanelDrawer isOpen={mobileDrawerOpen} onClose={() => setMobileDrawerOpen(false)}>
          <div>
            <h3 className="text-sm font-black">Trending / Menu</h3>
            <div className="mt-4">
              <button onClick={() => { setViewMode('TRENDING'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">Trending</button>
              <button onClick={() => { setViewMode('PICKS'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">My Picks</button>
              <button onClick={() => { setViewMode('PLAYOFFS'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">Playoffs</button>
              <button onClick={() => { setViewMode('TEAMS'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">Teams</button>
              <button onClick={() => { setViewMode('TIERS'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">Tier List</button>
              <button onClick={() => { setViewMode('GAME_RANKS'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">Ranks</button>
              <button onClick={() => { setViewMode('MY_TEAM'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">My Team</button>
              <button onClick={() => { setViewMode('COMPARE'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">Compare</button>
              <button onClick={() => { setViewMode('LOOKUP'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">Lookup</button>
            </div>
          </div>
        </SidePanelDrawer>

        {/* CONTENT */}
        {/* Bottom padding clears the floating agent button: without it the last
            row of every view (the final game's moneyline, at any width) sat
            permanently underneath it with no way to scroll it into view. */}
        <div className="flex-1 overflow-y-auto p-4 md:p-6 pb-28 md:pb-24 dark:scrollbar-thumb-slate-600 dark:scrollbar-track-slate-950">

          {/* Mobile Footer: quick access to Trending / Compare / Lookup */}
          <div className="fixed bottom-4 left-1/2 -translate-x-1/2 z-50 flex sm:hidden max-w-xs">
            <div className="flex items-center gap-2 bg-white dark:bg-slate-800 rounded-xl px-2 py-2 shadow-lg border border-slate-200 dark:border-slate-700">
              <button onClick={() => setViewMode('SCHEDULE')} className="p-2 rounded-md text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-700" aria-label="Schedule">
                <Calendar size={18} />
              </button>
              <button onClick={() => setViewMode('TRENDING')} className="p-2 rounded-md text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-700" aria-label="Trending">
                <TrendingUp size={18} />
              </button>
              <button onClick={() => setViewMode('PICKS')} className="p-2 rounded-md text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-700" aria-label="My Picks">
                <Check size={18} />
              </button>
              <button onClick={() => setViewMode('TIERS')} className="p-2 rounded-md text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-700" aria-label="Tier List">
                <Layers size={18} />
              </button>
              <button onClick={() => setViewMode('TEAMS')} className="p-2 rounded-md text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-700" aria-label="Teams">
                <Shield size={18} />
              </button>
              <button onClick={() => setViewMode('COMPARE')} className="p-2 rounded-md text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-700" aria-label="Compare">
                <BarChart2 size={18} />
              </button>
              <button onClick={() => setViewMode('LOOKUP')} className="p-2 rounded-md text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-700" aria-label="Lookup">
                <Search size={18} />
              </button>
            </div>
          </div>
          
          {viewMode === 'SCHEDULE' && (
            <div className={`mx-auto w-full ${showSidebars ? 'max-w-7xl' : 'max-w-[1600px]'}`}>
              <h2 className="text-xs font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-6">Week {activeWeek} Matchups</h2>
              
              {loadingSchedule ? (
                <div className="flex flex-col items-center justify-center h-64 opacity-50">
                  <div className="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin mb-4"></div>
                  <p className="text-sm text-slate-400 font-bold">Loading Live Odds...</p>
                </div>
              ) : games.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-64 opacity-50">
                  <p className="text-xl font-black text-slate-300 dark:text-slate-600 mb-2">NO MATCHUPS AVAILABLE</p>
                  <p className="text-sm text-slate-400">Schedule data is not available for Week {activeWeek}.</p>
                </div>
              ) : (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {games.map((game, idx) => (
                    <div 
                      key={idx} 
                      data-testid="schedule-game"
                      data-game={`${game.away_team}@${game.home_team}`}
                      onClick={() => { setSelectedGame({ home: game.home_team, away: game.away_team }); setViewMode('GAME'); }} 
                      className="bg-white dark:bg-slate-800 rounded-xl shadow-sm cursor-pointer hover:shadow-md transition-shadow group border border-slate-200 dark:border-slate-700 hover:border-blue-400 dark:hover:border-blue-500 relative overflow-hidden"
                    >
                      {game.game_total && (
                        <div className="absolute top-0 left-1/2 -translate-x-1/2 bg-slate-100 dark:bg-slate-900 px-3 py-1 rounded-b-lg border-x border-b border-slate-200 dark:border-slate-700 shadow-sm z-10">
                          <span className="text-[10px] font-black text-slate-500 uppercase tracking-wider">Total</span>
                          <span className="ml-1 text-xs font-bold text-slate-800 dark:text-slate-200">{game.game_total}</span>
                        </div>
                      )}

                      <div className="flex items-stretch h-28">
                        <div className="flex-1 p-5 flex flex-col justify-center relative">
                          <div className="absolute left-0 top-0 bottom-0 w-1.5" style={{ backgroundColor: getTeamColor(game.away_team) }}></div>
                          <div className="pl-3">
                            <span className="text-3xl font-black text-slate-800 dark:text-slate-100 leading-none">{game.away_team}</span>
                            {game.moneyline_away && (
                              <div className="mt-2">
                                <span className={`text-xs font-bold px-2 py-1 rounded ${String(game.moneyline_away).startsWith('-') ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300' : 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300'}`}>
                                  {(!String(game.moneyline_away).startsWith('-') && !String(game.moneyline_away).startsWith('+') && game.moneyline_away !== 'EVEN') ? '+' : ''}{game.moneyline_away}
                                </span>
                              </div>
                            )}
                          </div>
                        </div>

                        <div className="w-16 flex flex-col items-center justify-center bg-slate-50 dark:bg-slate-900/50 border-x border-slate-100 dark:border-slate-700/50">
                          <span className="text-xs font-black text-slate-300 dark:text-slate-600 italic">VS</span>
                        </div>

                        <div className="flex-1 p-5 flex flex-col justify-center items-end relative">
                          <div className="absolute right-0 top-0 bottom-0 w-1.5" style={{ backgroundColor: getTeamColor(game.home_team) }}></div>
                          <div className="pr-3 text-right">
                            <span className="text-3xl font-black text-slate-800 dark:text-slate-100 leading-none">{game.home_team}</span>
                            {game.moneyline_home && (
                              <div className="mt-2">
                                <span className={`text-xs font-bold px-2 py-1 rounded ${String(game.moneyline_home).startsWith('-') ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300' : 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300'}`}>
                                  {(!String(game.moneyline_home).startsWith('-') && !String(game.moneyline_home).startsWith('+') && game.moneyline_home !== 'EVEN') ? '+' : ''}{game.moneyline_home}
                                </span>
                              </div>
                            )}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* VIEW: TRENDING (mobile) */}
          {viewMode === 'TRENDING' && (
            <div className="mx-auto max-w-3xl">
              <h2 className="text-sm font-black mb-4">Trending Players</h2>
              {loadingUp ? (
                <p className="text-xs text-slate-400">Loading...</p>
              ) : (
                <div className="grid grid-cols-1 gap-3">
                  {trendingUp.map(p => (
                    <SidebarPlayerItem 
                      key={p.player_id}
                      player={p}
                      type="up"
                      onClick={(id) => { setSelectedHistoryId(id); setHistoryFrom('SCHEDULE'); setViewMode('HISTORY'); }}
                      onToggleCompare={toggleCompare}
                      isSelected={compareList.includes(p.player_id)}
                    />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* VIEW: PICKS (mobile) */}
          {viewMode === 'PICKS' && (
            <div className="mx-auto max-w-3xl">
              <h2 className="text-sm font-black mb-4">My Picks</h2>
              <MyPicksList currentWeek={activeWeek} />
            </div>
          )}

          {/* VIEW: PLAYOFFS */}
          {viewMode === 'PLAYOFFS' && (
            <PlayoffView />
          )}

          {/* VIEW: TEAMS */}
          {viewMode === 'TEAMS' && (
            <TeamsView onOpenTeam={(team) => openTeamPage(team)} />
          )}

          {/* VIEW: RANKS — per-game situation explainer + start/sit tiers.
              Always mounted, just hidden when off-screen, so drilling into a
              player's history and coming back preserves the selected game,
              position filter, and tier board exactly as left. */}
          <div
            className={`mx-auto w-full px-2 ${showSidebars ? 'max-w-[1800px]' : 'max-w-[2200px]'}`}
            style={{ display: viewMode === 'GAME_RANKS' ? 'block' : 'none' }}
          >
            <GameRanksView
              games={games}
              loadingSchedule={loadingSchedule}
              week={activeWeek}
              compareList={compareList}
              onToggleCompare={toggleCompare}
              onOpenHistory={openHistoryFromRanks}
              activeGame={selectedGame}
              onSelectGame={setSelectedGame}
            />
          </div>

          {/* VIEW: MY TEAM - import a Sleeper roster and analyze it. */}
          {viewMode === 'MY_TEAM' && (
            <div className="mx-auto w-full max-w-[1600px] px-2">
              <SleeperView
                week={safeWeek}
                season={new Date().getMonth() >= 8 ? new Date().getFullYear() : new Date().getFullYear() - 1}
                onOpenHistory={(id) => { setSelectedHistoryId(id); setHistoryFrom('SCHEDULE'); setViewMode('HISTORY'); }}
                onInnerNav={handleInnerNav}
              />
            </div>
          )}

          {/* VIEW: TEAM PAGE - Overview / Team Builder as a full page rather
              than an overlay, so the sidebars stay usable and Back behaves. */}
          {viewMode === 'TEAM_PAGE' && teamModal && (
            <div className="mx-auto w-full max-w-[2200px] px-2">
              <TeamOffenseModal
                asPage
                team={teamModal.team}
                focusPlayerId={teamModal.focusPlayerId ?? null}
                initialDetailPlayerId={teamModal.initialDetailPlayerId ?? null}
                initialTab={teamModal.initialTab ?? 'overview'}
                onClose={() => { if (navStack.length) goBack(); else setViewMode('TEAMS'); }}
                compareList={compareList}
                onToggleCompare={toggleCompare}
                onViewHistory={(id, ctx) => {
                  // Stash which team/tab/player-detail was open so Back from
                  // HISTORY returns you exactly where you left off.
                  setModalReturn({
                    team: teamModal.team,
                    focusPlayerId: teamModal.focusPlayerId ?? null,
                    detailPlayerId: ctx?.detailPlayerId ?? null,
                    activeTab: ctx?.activeTab ?? teamModal.initialTab ?? 'overview',
                  });
                  setSelectedHistoryId(id);
                  setHistoryFrom('TEAMS');
                  setViewMode('HISTORY');
                }}
              />
            </div>
          )}

          {/* VIEW: TIER LIST — always mounted, just hidden when off-screen,
              so navigating to Compare/History and back preserves all local state
              (drag assignments, search input, scroll position). */}
          <div className="mx-auto w-full max-w-[2200px] px-2" style={{ display: viewMode === 'TIERS' ? 'block' : 'none' }}>
            <TierListView
              state={tierState}
              onStateChange={setTierState}
              compareList={compareList}
              onToggleCompare={toggleCompare}
              onViewHistory={openHistoryFromTiers}
              onOpenCompare={openCompareFromTiers}
              onOpenTeam={openTeamFromTiers}
            />
          </div>

          {/* VIEW: GAME ROSTERS */}
          {viewMode === 'GAME' && selectedGame && (
            <MatchupView 
              week={safeWeek}
              home={selectedGame.home}
              away={selectedGame.away}
              onBack={() => { if (navStack.length) goBack(); else setViewMode('SCHEDULE'); }}
              compareList={compareList}
              onToggleCompare={toggleCompare}
              onOpenHistory={(id) => { setSelectedHistoryId(id); setHistoryFrom('GAME'); setViewMode('HISTORY'); }}
              onInnerNav={handleInnerNav}
            />
          )}

          {/* VIEW: LOOKUP */}
          {viewMode === 'LOOKUP' && (
            <PlayerLookupView 
                onViewHistory={(id) => { setSelectedHistoryId(id); setHistoryFrom('LOOKUP'); setViewMode('HISTORY'); }} 
                compareList={compareList}
                onToggleCompare={toggleCompare}
            />
          )}
          
          {/* VIEW: COMPARE */}
          {viewMode === 'COMPARE' && (
            <CompareView 
                week={safeWeek} // Pass week here
                playerIds={compareList}
                onRemove={(id) => toggleCompare(id)}
                onAdd={(id) => toggleCompare(id)}
                onViewHistory={(id) => { 
                    setSelectedHistoryId(id); 
                    setHistoryFrom('COMPARE'); 
                    setViewMode('HISTORY'); 
                }} 
            />
          )}
          
          {/* VIEW: HISTORY */}
          {viewMode === 'HISTORY' && selectedHistoryId && (
            <div className="w-full max-w-5xl mx-auto">
                <PlayerHistory 
                    playerId={selectedHistoryId} 
                    onBack={() => { if (navStack.length) goBack(); else setViewMode(historyFrom); }}
                    compareList={compareList}
                    onToggleCompare={toggleCompare}
                />
            </div>
          )}

        </div>
      </main>

      {/* RIGHT SIDEBAR (hidden where the main view needs the full width) */}
      {showSidebars && viewMode !== 'TIERS' && viewMode !== 'TEAMS' && (
        <aside className="w-80 bg-white dark:bg-slate-800 border-l border-slate-200 dark:border-slate-700 flex flex-col z-20 shadow-[-4px_0_24px_rgba(0,0,0,0.02)] shrink-0 hidden xl:flex transition-colors duration-300">
          {/* The agent panel slots into this rail rather than overlaying the page.
              Closing it restores the trending list exactly as it was -- the rail
              is the only thing that changes. */}
          {panelOpen ? <AgentPanel headerExtra={railControls} /> : (
          <>
          <div className="p-4 border-b border-slate-100 dark:border-slate-700 bg-slate-50/50 dark:bg-slate-800/50 backdrop-blur flex items-start justify-between">
            <div>
              <div className="flex items-center gap-2 text-green-600 dark:text-green-400 mb-1">
                  <TrendingUp size={16} />
                  <h2 className="text-xs font-black uppercase tracking-widest">Trending Up</h2>
              </div>
              <p className="text-xs text-slate-400 dark:text-slate-500">Most Added Players (24h)</p>
            </div>

            {railControls}
          </div>
          <div className="flex-1 overflow-y-auto p-4 scrollbar-thin dark:scrollbar-thumb-slate-600 dark:scrollbar-track-slate-800">
            {loadingUp ? <p className="text-xs text-slate-400 text-center mt-10">Scanning Market...</p> : 
              trendingUp.map(p => (
                <SidebarPlayerItem 
                    key={p.player_id} 
                    player={p} 
                    type="up" 
                    isSelected={compareList.includes(p.player_id)}
                    onToggleCompare={toggleCompare}
                    onClick={(id) => { 
                        setSelectedHistoryId(id); 
                        setHistoryFrom('SCHEDULE'); 
                        setViewMode('HISTORY'); 
                    }} 
                />
              ))
            }
          </div>
          </>
          )}
        </aside>
      )}

      {/* Agentic entry point: floating on every view, nudged clear of the right
          rail when that rail is on screen. */}
      <AgentDock offsetClass={rightRailVisible ? 'xl:right-[21.5rem]' : ''} />
    </div>
  );
}
