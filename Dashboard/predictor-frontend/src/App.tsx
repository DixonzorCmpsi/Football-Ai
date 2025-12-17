import { useState, useEffect } from 'react';
import { Search, ArrowLeft, BarChart2, PanelLeft, Minimize2, TrendingUp, TrendingDown, Sun, Moon } from 'lucide-react';
import { usePastRankings, useFutureRankings, useSchedule, useMatchupDeepDive, useCurrentWeek } from './hooks/useNflData';
import type { Player } from './hooks/useNflData';
import PlayerLookupView from './components/PlayerLookup';
import CompareView from './components/CompareView';
import PlayerHistory from './components/PlayerHistory';
import BroadcastCard from './components/BroadcastCard';
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
const SidebarPlayerItem = ({ player, type, onClick }: { player: Player, type: 'up' | 'down', onClick?: (id: string) => void }) => {
  const statusLabel = getStatusLabel(player.injury_status);
  const statusColor = getStatusColor(player.injury_status);

  return (
    <div 
      onClick={() => onClick && onClick(player.player_id)}
      className="bg-white dark:bg-slate-800 p-3 rounded-lg shadow-sm border border-slate-200 dark:border-slate-700 flex items-start gap-3 mb-2 transition-all hover:shadow-md hover:ring-2 hover:ring-blue-50 dark:hover:ring-blue-900 cursor-pointer relative group"
    >
      {/* Player Image */}
      <div className="w-10 h-10 bg-slate-100 dark:bg-slate-700 rounded-full overflow-hidden border border-slate-200 dark:border-slate-600 shrink-0 relative">
        {player.image ? (
          <img src={player.image} alt={player.player_name} className="object-cover w-full h-full" />
        ) : (
          <div className="w-full h-full flex items-center justify-center text-slate-300 dark:text-slate-500 text-xs">IMG</div>
        )}
      </div>
      
      {/* Name & Info */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
            <p className="font-bold text-sm truncate text-slate-800 dark:text-slate-100">{player.player_name}</p>
            {/* INJURY STATUS BADGE */}
            <span className={`text-[9px] font-black px-1.5 py-0.5 rounded border ${statusColor}`}>
                {statusLabel}
            </span>
        </div>
        <p className="text-[10px] uppercase font-semibold text-slate-400 dark:text-slate-500">{player.position} • {player.team}</p>
      </div>

      {/* Trend or Score */}
      <div className="text-right shrink-0 mt-0.5">
        {player.trending_count !== undefined ? (
          <div className={`text-xs font-black px-2 py-1 rounded-full ${type === 'up' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400'}`}>
             {type === 'up' ? '+' : '-'}{Math.abs(player.trending_count)}
          </div>
        ) : (
          <div className="font-bold text-blue-600 dark:text-blue-400 text-sm">
              {player.projected_points?.toFixed(1)}
          </div>
        )}
      </div>
    </div>
  );
};

// --- MAIN APP ---
export default function App() {
  const { currentWeek, loadingWeek } = useCurrentWeek();
  
  // State
  const [viewMode, setViewMode] = useState<'SCHEDULE' | 'GAME' | 'LOOKUP' | 'COMPARE' | 'HISTORY'>('SCHEDULE');
  const [showSidebars, setShowSidebars] = useState(true); 
  const [rosterFilter, setRosterFilter] = useState<'ALL' | 'QB' | 'RB' | 'WR' | 'TE'>('ALL'); 
  const [selectedGame, setSelectedGame] = useState<{home: string, away: string} | null>(null);
  const [selectedHistoryId, setSelectedHistoryId] = useState<string | null>(null);
  const [historyFrom, setHistoryFrom] = useState<'SCHEDULE' | 'GAME' | 'LOOKUP' | 'COMPARE'>('SCHEDULE');

  // --- LIFTED STATE FOR COMPARE VIEW ---
  const [compareP1, setCompareP1] = useState<string | null>(null);
  const [compareP2, setCompareP2] = useState<string | null>(null);

  // Theme State
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

  // Data Hooks
  const { pastRankings: trendingDown, loadingPast: loadingDown } = usePastRankings(currentWeek > 1 ? currentWeek - 1 : 1);
  const { futureRankings: trendingUp, loadingFuture: loadingUp } = useFutureRankings(currentWeek);
  const { games, loadingSchedule } = useSchedule(currentWeek);
  const { matchupData } = useMatchupDeepDive(currentWeek, selectedGame?.home || '', selectedGame?.away || '');

  if (loadingWeek) return <div className="flex h-screen items-center justify-center bg-slate-50 dark:bg-slate-900 text-slate-400 font-bold animate-pulse">Connecting...</div>;

  // --- Sort by Average Points ---
  const filterRoster = (list: any[]) => {
    const filtered = rosterFilter === 'ALL' ? list : list.filter(p => p.position === rosterFilter);
    return filtered.sort((a, b) => (b.stats?.average || 0) - (a.stats?.average || 0));
  };

  return (
    <div className="flex h-screen bg-slate-50 dark:bg-slate-900 font-sans text-slate-900 dark:text-slate-100 overflow-hidden transition-colors duration-300">
      
      {/* LEFT SIDEBAR: TRENDING DOWN */}
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
      <main className="flex-1 flex flex-col relative min-w-0 bg-slate-100 dark:bg-slate-950 transition-colors duration-300">
        
        {/* HEADER */}
        <header className="h-16 bg-slate-100/80 dark:bg-slate-950/80 backdrop-blur-md border-b border-slate-200 dark:border-slate-800 flex items-center justify-between px-6 shadow-sm sticky top-0 z-30 transition-colors duration-300">
          
          {/* FIX: Reduced gap-2 to gap-1 AND added pr-8 to force separation between logo and nav buttons */}
          <div className="flex items-center gap-1 pr-8">
             <button onClick={() => setShowSidebars(!showSidebars)} className="p-2 text-slate-400 hover:text-blue-600 hover:bg-white dark:hover:bg-slate-800 rounded-lg transition-colors">
                {showSidebars ? <Minimize2 size={20} /> : <PanelLeft size={20} />}
             </button>
             
             <div className="font-black text-xl italic tracking-tighter select-none cursor-pointer hidden sm:block z-50 relative whitespace-nowrap" onClick={() => setViewMode('SCHEDULE')}>
                The Spot <span className="text-blue-600 dark:text-blue-400">Ai</span>
             </div>
          </div>
          
          <div className="flex items-center gap-4">
            {/* View Mode Toggles */}
            <div className="flex gap-2 bg-white/50 dark:bg-slate-800/50 p-1 rounded-lg border border-slate-200/50 dark:border-slate-700/50">
              {['SCHEDULE', 'COMPARE', 'LOOKUP'].map((mode) => (
                <button key={mode} onClick={() => setViewMode(mode as any)} className={`px-3 py-1.5 rounded-md text-xs font-bold flex items-center gap-2 transition-all ${viewMode === mode ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm ring-1 ring-black/5 dark:ring-white/5' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`}>
                  {mode === 'SCHEDULE' && <BarChart2 size={14}/>}
                  {mode === 'COMPARE' && <BarChart2 size={14}/>}
                  {mode === 'LOOKUP' && <Search size={14}/>}
                  {mode}
                </button>
              ))}
            </div>

            {/* Theme Toggle */}
            <button onClick={() => setIsDarkMode(!isDarkMode)} className="p-2 text-slate-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-white dark:hover:bg-slate-800 rounded-lg transition-colors">
              {isDarkMode ? <Sun size={20} /> : <Moon size={20} />}
            </button>
            
            <div className="text-sm font-black text-slate-900 dark:text-slate-100 bg-white dark:bg-slate-800 px-3 py-1 rounded-full border border-slate-200 dark:border-slate-700 shadow-sm">Wk {currentWeek}</div>
          </div>
        </header>

        {/* CONTENT */}
        <div className="flex-1 overflow-y-auto p-4 md:p-6 scroll-smooth dark:scrollbar-thumb-slate-600 dark:scrollbar-track-slate-950">
          
          {/* VIEW: SCHEDULE */}
          {viewMode === 'SCHEDULE' && (
            <div className={`mx-auto transition-all duration-300 ${showSidebars ? 'max-w-5xl' : 'max-w-6xl'}`}>
              <h2 className="text-xs font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-6">Week {currentWeek} Matchups</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {loadingSchedule ? <p className="text-slate-400 animate-pulse">Loading Schedule...</p> : games.map((game, idx) => (
                    <div key={idx} onClick={() => { setSelectedGame({ home: game.home_team, away: game.away_team }); setViewMode('GAME'); setRosterFilter('ALL'); }} className="bg-white dark:bg-slate-800 rounded-xl shadow-sm cursor-pointer hover:shadow-md transition-all group flex items-stretch overflow-hidden border border-slate-200 dark:border-slate-700 hover:border-blue-300 dark:hover:border-blue-500">
                        <div className="flex-1 p-6 flex items-center justify-between relative overflow-hidden"><div className="absolute left-0 top-0 bottom-0 w-2" style={{ backgroundColor: getTeamColor(game.away_team) }}></div><span className="text-2xl font-black text-slate-800 dark:text-slate-100 pl-2">{game.away_team}</span></div>
                        <div className="w-12 flex items-center justify-center bg-slate-50 dark:bg-slate-900"><span className="text-xs font-black text-slate-300 dark:text-slate-600">VS</span></div>
                        <div className="flex-1 p-6 flex items-center justify-between flex-row-reverse relative overflow-hidden"><div className="absolute right-0 top-0 bottom-0 w-2" style={{ backgroundColor: getTeamColor(game.home_team) }}></div><span className="text-2xl font-black text-slate-800 dark:text-slate-100 pr-2">{game.home_team}</span></div>
                    </div>
                ))}
              </div>
            </div>
          )}

          {/* VIEW: GAME ROSTERS */}
          {viewMode === 'GAME' && matchupData && (
            <div className={`mx-auto animate-in fade-in slide-in-from-bottom-4 duration-500 ${showSidebars ? 'max-w-6xl' : 'max-w-7xl'}`}>
              <div className="flex flex-col md:flex-row items-center justify-between mb-8 gap-4">
                <button onClick={() => setViewMode('SCHEDULE')} className="flex items-center text-sm font-bold text-slate-400 hover:text-blue-600 dark:hover:text-blue-400"><ArrowLeft size={16} className="mr-1"/> BACK</button>
                <div className="flex bg-white dark:bg-slate-800 rounded-lg p-1 shadow-sm border border-slate-200 dark:border-slate-700">
                  {['ALL', 'QB', 'RB', 'WR', 'TE'].map((pos) => (
                    <button key={pos} onClick={() => setRosterFilter(pos as any)} className={`px-4 py-1.5 rounded-md text-xs font-bold transition-all ${rosterFilter === pos ? 'bg-slate-900 dark:bg-slate-100 text-white dark:text-slate-900 shadow-md' : 'text-slate-500 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-700 hover:text-slate-900 dark:hover:text-slate-100'}`}>{pos}</button>
                  ))}
                </div>
              </div>
              
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                 {/* COLUMNS */}
                 {['away', 'home'].map(side => (
                     <div key={side}>
                        <h3 className="text-center font-black text-xl mb-6 text-white py-2 rounded-t-lg shadow-sm" style={{ backgroundColor: getTeamColor((matchupData as any)[`${side}_roster`][0]?.team || 'ARI') }}>{side === 'away' ? 'AWAY' : 'HOME'} ROSTER</h3>
                        <div className="space-y-4">
                          {filterRoster((matchupData as any)[`${side}_roster`]).map((player: any) => (
                             <BroadcastCard 
                                key={player.id} 
                                data={player} 
                                mini={true} 
                                onClick={(id) => { 
                                    setSelectedHistoryId(id); 
                                    setHistoryFrom('GAME'); 
                                    setViewMode('HISTORY'); 
                                }} 
                             />
                          ))}
                        </div>
                     </div>
                 ))}
              </div>
            </div>
          )}

          {/* VIEW: LOOKUP */}
          {viewMode === 'LOOKUP' && <PlayerLookupView onViewHistory={(id) => { setSelectedHistoryId(id); setHistoryFrom('LOOKUP'); setViewMode('HISTORY'); }} />}
          
          {/* VIEW: COMPARE (UPDATED with State Lifting) */}
          {viewMode === 'COMPARE' && (
            <CompareView 
                p1={compareP1}
                p2={compareP2}
                setP1={setCompareP1}
                setP2={setCompareP2}
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
                <PlayerHistory playerId={selectedHistoryId} onBack={() => setViewMode(historyFrom)} />
            </div>
          )}

        </div>
      </main>

      {/* RIGHT SIDEBAR: TRENDING UP */}
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