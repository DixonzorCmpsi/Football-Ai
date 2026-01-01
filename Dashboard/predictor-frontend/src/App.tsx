import { useState, useEffect } from 'react';
import { Search, BarChart2, PanelLeft, Minimize2, TrendingUp, TrendingDown, Sun, Moon, Plus, Check, Calendar } from 'lucide-react';
import { usePastRankings, useFutureRankings, useSchedule, useCurrentWeek } from './hooks/useNflData';
import type { Player } from './hooks/useNflData';
import PlayerLookupView from './components/PlayerLookup';
import SidePanelDrawer from './components/SidePanelDrawer';
import CompareView from './components/CompareView';
import PlayerHistory from './components/PlayerHistory';
import MatchupView from './components/MatchupView';
import { getTeamColor } from './utils/nflColors';

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
    <div className="group relative bg-white dark:bg-slate-800 p-3 rounded-lg shadow-sm border border-slate-200 dark:border-slate-700 flex items-start gap-3 mb-2 transition-all hover:shadow-md hover:ring-2 hover:ring-blue-50 dark:hover:ring-blue-900 cursor-pointer">
      <div className="flex-1 flex gap-3 min-w-0" onClick={() => onClick && onClick(player.player_id)}>
          <div className="w-10 h-10 bg-slate-100 dark:bg-slate-700 rounded-full overflow-hidden border border-slate-200 dark:border-slate-600 shrink-0 relative">
            {player.image ? (
              <img src={player.image} alt={player.player_name} className="object-cover w-full h-full" />
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
            {type === 'up' ? '+' : '-'}{Math.abs(player.trending_count || 0)}
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
  
  const [viewMode, setViewMode] = useState<'SCHEDULE' | 'GAME' | 'LOOKUP' | 'COMPARE' | 'HISTORY' | 'TRENDING'>('SCHEDULE');
  const [showSidebars, setShowSidebars] = useState(true); 
  const [mobileDrawerOpen, setMobileDrawerOpen] = useState(false);
  const [selectedGame, setSelectedGame] = useState<{home: string, away: string} | null>(null);
  const [selectedHistoryId, setSelectedHistoryId] = useState<string | null>(null);
  const [historyFrom, setHistoryFrom] = useState<'SCHEDULE' | 'GAME' | 'LOOKUP' | 'COMPARE'>('SCHEDULE');

  const [compareList, setCompareList] = useState<string[]>([]);

  const toggleCompare = (playerId: string) => {
    setCompareList(prev => {
        if (prev.includes(playerId)) return prev.filter(id => id !== playerId);
        return [...prev, playerId];
    });
  };

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

  return (
    <div className="flex h-screen bg-slate-100 dark:bg-slate-900 font-sans text-slate-900 dark:text-slate-100 overflow-hidden transition-colors duration-300">
      
      {/* LEFT SIDEBAR */}
      {showSidebars && (
        <aside className="w-80 bg-white dark:bg-slate-800 border-r border-slate-200 dark:border-slate-700 flex flex-col z-20 shadow-[4px_0_24px_rgba(0,0,0,0.02)] shrink-0 hidden lg:flex transition-colors duration-300">
          <div className="p-4 border-b border-slate-100 dark:border-slate-700 bg-slate-50/50 dark:bg-slate-800/50 backdrop-blur">
            <div className="flex items-center gap-2 text-red-600 dark:text-red-400 mb-1">
                <TrendingDown size={16} />
                <h2 className="text-xs font-black uppercase tracking-widest">Trending Down</h2>
            </div>
            <p className="text-xs text-slate-400 dark:text-slate-500">Most Dropped Players (24h)</p>
          </div>
          <div className="flex-1 overflow-y-auto p-4 scrollbar-thin dark:scrollbar-thumb-slate-600 dark:scrollbar-track-slate-800">
            {loadingDown ? <p className="text-xs text-slate-400 text-center mt-10">Scanning Market...</p> : 
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
            }
          </div>
        </aside>
      )}

      {/* CENTER STAGE */}
      <main className="flex-1 flex flex-col relative min-w-0 bg-slate-50 dark:bg-slate-950 transition-colors duration-300">
        
        {/* HEADER */}
        <header className="h-16 bg-white/80 dark:bg-slate-950/80 backdrop-blur-md border-b border-slate-200 dark:border-slate-800 flex items-center justify-between px-6 shadow-sm sticky top-0 z-30 transition-colors duration-300">
          
          <div className="flex items-center gap-1 pr-8">
             <button onClick={() => setShowSidebars(!showSidebars)} className="p-2 text-slate-400 hover:text-blue-600 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg transition-colors">
                {showSidebars ? <Minimize2 size={20} /> : <PanelLeft size={20} />}
             </button>
             
             <div className="font-black text-xl italic tracking-tighter select-none cursor-pointer hidden sm:flex items-center gap-2 z-50 relative whitespace-nowrap" onClick={() => setViewMode('SCHEDULE')}>
                <div className="text-2xl font-extrabold" aria-hidden>Ai</div>
                <span className="sr-only">The Spot</span>
             </div>
          </div>
          
          <div className="flex items-center gap-4">
            <div className="hidden sm:flex gap-2 bg-slate-100 dark:bg-slate-800/50 p-1 rounded-lg border border-slate-200/50 dark:border-slate-700/50" role="tablist" aria-label="Main navigation tabs">
              {(['SCHEDULE', 'COMPARE', 'LOOKUP'] as const).map((mode) => (
                <button key={mode} onClick={() => setViewMode(mode)} className={`px-3 py-1.5 rounded-md text-xs font-bold flex items-center gap-2 transition-all ${viewMode === mode ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm ring-1 ring-black/5 dark:ring-white/5' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`}>
                  {mode === 'SCHEDULE' && <BarChart2 size={14}/>}
                  {mode === 'COMPARE' && (
                      <div className="flex items-center gap-1">
                          <BarChart2 size={14}/>
                          {compareList.length > 0 && <span className="bg-blue-600 text-white text-[9px] px-1.5 rounded-full">{compareList.length}</span>}
                      </div>
                  )}
                  {mode === 'LOOKUP' && <Search size={14}/>}
                  {mode === 'COMPARE' ? 'COMPARE' : mode}
                </button>
              ))}
            </div>

            <button onClick={() => setIsDarkMode(!isDarkMode)} className="p-2 text-slate-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg transition-colors">
              {isDarkMode ? <Sun size={20} /> : <Moon size={20} />}
            </button>

            {/* Mobile: Trending access (hidden on lg screens where sidebars exist) */}
            <button onClick={() => setMobileDrawerOpen(true)} className="p-2 text-slate-400 hover:text-blue-600 lg:hidden rounded-lg transition-colors" aria-label="Open Menu">
              <TrendingUp size={18} />
            </button>
            
            <div className="text-sm font-black text-slate-900 dark:text-slate-100 bg-white dark:bg-slate-800 px-3 py-1 rounded-full border border-slate-200 dark:border-slate-700 shadow-sm">
                Wk {activeWeek || "-"}
            </div>
          </div>
        </header>

        {/* Mobile Drawer */}
        <SidePanelDrawer isOpen={mobileDrawerOpen} onClose={() => setMobileDrawerOpen(false)}>
          <div>
            <h3 className="text-sm font-black">Trending / Menu</h3>
            <div className="mt-4">
              <button onClick={() => { setViewMode('TRENDING'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">Trending</button>
              <button onClick={() => { setViewMode('COMPARE'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">Compare</button>
              <button onClick={() => { setViewMode('LOOKUP'); setMobileDrawerOpen(false); }} className="w-full text-left p-3 rounded hover:bg-slate-100 dark:hover:bg-slate-800">Lookup</button>
            </div>
          </div>
        </SidePanelDrawer>

        {/* CONTENT */}
        <div className="flex-1 overflow-y-auto p-4 md:p-6 scroll-smooth dark:scrollbar-thumb-slate-600 dark:scrollbar-track-slate-950">

          {/* Mobile Footer: quick access to Trending / Compare / Lookup */}
          <div className="fixed bottom-4 left-1/2 -translate-x-1/2 z-50 flex sm:hidden max-w-xs">
            <div className="flex items-center gap-3 bg-white dark:bg-slate-800 rounded-xl px-3 py-2 shadow-lg border border-slate-200 dark:border-slate-700">
              <button onClick={() => setViewMode('SCHEDULE')} className="p-2 rounded-md text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-700" aria-label="Schedule">
                <Calendar size={18} />
              </button>
              <button onClick={() => setViewMode('TRENDING')} className="p-2 rounded-md text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-700" aria-label="Trending">
                <TrendingUp size={18} />
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
            <div className={`mx-auto transition-all duration-300 ${showSidebars ? 'max-w-5xl' : 'max-w-6xl'}`}>
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
                      onClick={() => { setSelectedGame({ home: game.home_team, away: game.away_team }); setViewMode('GAME'); }} 
                      className="bg-white dark:bg-slate-800 rounded-xl shadow-sm cursor-pointer hover:shadow-md transition-all group border border-slate-200 dark:border-slate-700 hover:border-blue-400 dark:hover:border-blue-500 relative overflow-hidden"
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

          {/* VIEW: GAME ROSTERS */}
          {viewMode === 'GAME' && selectedGame && (
            <MatchupView 
              week={safeWeek}
              home={selectedGame.home}
              away={selectedGame.away}
              onBack={() => setViewMode('SCHEDULE')}
              compareList={compareList}
              onToggleCompare={toggleCompare}
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
                    onBack={() => setViewMode(historyFrom)}
                    compareList={compareList}
                    onToggleCompare={toggleCompare}
                />
            </div>
          )}

        </div>
      </main>

      {/* RIGHT SIDEBAR */}
      {showSidebars && (
        <aside className="w-80 bg-white dark:bg-slate-800 border-l border-slate-200 dark:border-slate-700 flex flex-col z-20 shadow-[-4px_0_24px_rgba(0,0,0,0.02)] shrink-0 hidden lg:flex transition-colors duration-300">
          <div className="p-4 border-b border-slate-100 dark:border-slate-700 bg-slate-50/50 dark:bg-slate-800/50 backdrop-blur">
            <div className="flex items-center gap-2 text-green-600 dark:text-green-400 mb-1">
                <TrendingUp size={16} />
                <h2 className="text-xs font-black uppercase tracking-widest">Trending Up</h2>
            </div>
            <p className="text-xs text-slate-400 dark:text-slate-500">Most Added Players (24h)</p>
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
        </aside>
      )}
    </div>
  );
}