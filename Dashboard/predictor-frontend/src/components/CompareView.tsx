import React, { useState, useEffect, useMemo } from 'react';
import { X, Swords, TrendingUp, Radar, Activity, Plus, Search, Check } from 'lucide-react';
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
  RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar as RechartRadar,
  BarChart, Bar
} from 'recharts';
import PlayerCard from './PlayerCard'; 
import { getTeamColor } from '../utils/nflColors';
import { usePlayerSearch, useSchedule } from '../hooks/useNflData';
import type { ScheduleGame, HistoryEntry, Player } from '../hooks/useNflData';
import type { PlayerData } from '../types';

interface CompareViewProps {
  week: number;
  playerIds: string[];
  onRemove: (id: string) => void;
  onAdd: (id: string) => void;
  onViewHistory: (id: string) => void;
}

const GRAPH_COLORS = ['#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899'];

// --- SUB-COMPONENT: Single Player Column ---
const LoadedPlayerCard = ({ 
    id, 
    week, 
    schedule, // <-- Receive Schedule
    onRemove, 
    onViewHistory, 
    colorIndex 
}: { 
    id: string, 
    week: number, 
    schedule: ScheduleGame[], 
    onRemove: (id: string) => void, 
    onViewHistory: (id: string) => void, 
    colorIndex: number 
}) => {
    const [data, setData] = useState<PlayerData | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        import('../lib/api').then(({ fetchPlayerById }) => {
            fetchPlayerById(id, week)
                .then(setData)
                .catch(console.error)
                .finally(() => setLoading(false));
        }).catch(err => { console.error(err); setLoading(false); });
    }, [id, week]);

    // --- OPPONENT FALLBACK LOGIC ---
    const displayData = useMemo(() => {
        if (!data) return null;

        // If API already has a valid opponent, use it
        if (data.opponent && data.opponent !== 'BYE') return data;

        // If API says "BYE" (or null), try to find the real opponent in the schedule
        if (schedule && schedule.length > 0) {
            const game = schedule.find((g: ScheduleGame) => g.home_team === data.team || g.away_team === data.team);
            if (game) {
                const realOpponent = game.home_team === data.team ? game.away_team : game.home_team;
                return { ...data, opponent: realOpponent };
            }
        }
        
        return data;
    }, [data, schedule]);

    if (loading) return (
        <div className="h-40 w-72 bg-slate-100 dark:bg-slate-800 rounded-xl animate-pulse flex items-center justify-center border border-slate-200 dark:border-slate-700 shrink-0">
            <span className="text-xs font-bold text-slate-400">LOADING...</span>
        </div>
    );

    if (!displayData) return <div className="text-red-500 font-bold p-4">Error</div>;

    return (
        <div className="relative group animate-in slide-in-from-bottom-4 duration-500 w-64 sm:w-80 shrink-0">
            <div className="h-2 w-full rounded-t-xl mb-[-2px] z-10 relative shadow-sm" style={{ backgroundColor: GRAPH_COLORS[colorIndex % GRAPH_COLORS.length] }}></div>
            <button 
                onClick={(e) => { e.stopPropagation(); onRemove(id); }}
                className="absolute top-4 right-3 bg-red-500 text-white p-1 rounded-full z-30 shadow-md opacity-0 group-hover:opacity-100 transition-opacity hover:bg-red-600 scale-75"
            >
                <X size={16} />
            </button>
            <div className="rounded-b-xl overflow-hidden shadow-lg border border-t-0 border-slate-200 dark:border-slate-700">
                <PlayerCard 
                    data={displayData} // Use the corrected data
                    teamColor={getTeamColor(displayData.team)}
                    onClick={() => onViewHistory(id)}
                />
            </div>
        </div>
    );
};

// --- SUB-COMPONENT: Add Player Card ---
const AddPlayerCard = ({ onAdd, existingIds }: { onAdd: (id: string) => void, existingIds: string[] }) => {
    const [isSearching, setIsSearching] = useState(false);
    const [query, setQuery] = useState("");
    const results = usePlayerSearch(query) as Player[];

    if (!isSearching) {
        return (
            <button 
                onClick={() => setIsSearching(true)}
                className="h-full min-h-[140px] w-24 rounded-xl bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 flex flex-col items-center justify-center gap-2 text-slate-400 hover:text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-900/20 hover:border-blue-200 transition-all group shrink-0"
            >
                <Plus size={24} strokeWidth={3} className="group-hover:scale-110 transition-transform" />
                <span className="text-[10px] font-black uppercase tracking-widest">Add</span>
            </button>
        );
    }

    return (
        <div className="h-auto min-h-[140px] w-72 rounded-xl bg-white dark:bg-slate-800 shadow-xl border border-slate-200 dark:border-slate-700 flex flex-col relative animate-in fade-in zoom-in-95 duration-200 shrink-0 z-50">
            <div className="p-3 border-b border-slate-100 dark:border-slate-700 flex items-center gap-2">
                <Search className="text-slate-400 shrink-0" size={16} />
                <input 
                    autoFocus
                    type="text"
                    placeholder="Search player..."
                    className="flex-1 bg-transparent outline-none text-slate-800 dark:text-slate-200 font-bold text-sm placeholder-slate-400"
                    value={query}
                    onChange={e => setQuery(e.target.value)}
                />
                <button 
                    onClick={() => { setIsSearching(false); setQuery(""); }}
                    className="p-1 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-full text-slate-400 transition-colors"
                >
                    <X size={14} />
                </button>
            </div>

            <div className="flex-1 overflow-y-auto p-1 max-h-60 scrollbar-thin scrollbar-thumb-slate-300 dark:scrollbar-thumb-slate-700">
                {results.length > 0 ? (
                    <div className="space-y-0.5">
                        {results.map((p: Player) => {
                            const isAdded = existingIds.includes(p.player_id);
                            return (
                                <button
                                    key={p.player_id}
                                    disabled={isAdded}
                                    onClick={() => {
                                        onAdd(p.player_id);
                                        setIsSearching(false);
                                        setQuery("");
                                    }}
                                    className={`w-full text-left px-3 py-2 rounded-lg flex justify-between items-center transition-colors group ${
                                        isAdded 
                                        ? 'opacity-50 cursor-not-allowed bg-slate-50 dark:bg-slate-700/50' 
                                        : 'hover:bg-blue-50 dark:hover:bg-blue-900/20'
                                    }`}
                                >
                                    <div>
                                        <div className={`font-bold text-xs ${isAdded ? 'text-slate-500' : 'text-slate-800 dark:text-slate-200 group-hover:text-blue-600'}`}>{p.player_name}</div>
                                        <div className="text-[10px] text-slate-400 font-semibold">{p.position} • {p.team}</div>
                                    </div>
                                    {isAdded ? (
                                        <Check size={12} className="text-green-500" />
                                    ) : (
                                        <Plus size={12} className="text-blue-500 opacity-0 group-hover:opacity-100" />
                                    )}
                                </button>
                            );
                        })}
                    </div>
                ) : (
                    query.length > 1 && (
                        <div className="p-4 text-center text-slate-400 text-xs font-bold">No players found.</div>
                    )
                )}
            </div>
        </div>
    );
};

// --- MAIN VIEW ---
export default function CompareView({ week, playerIds, onRemove, onViewHistory, onAdd }: CompareViewProps) {
  const [activeTab, setActiveTab] = useState<'RADAR' | 'LINE' | 'DEPENDENCY'>('RADAR');
  const [players, setPlayers] = useState<PlayerData[]>([]);
  const [histories, setHistories] = useState<HistoryEntry[][]>([]);
  const [loadingGraphs, setLoadingGraphs] = useState(false);

  // --- FETCH SCHEDULE FOR BACKUP ---
  const { games: schedule } = useSchedule(week);

  useEffect(() => {
    if (playerIds.length === 0) {
        setPlayers([]);
        setHistories([]);
        return;
    }
    setLoadingGraphs(true);

    const fetchAll = async () => {
        try {
            const api = await import('../lib/api');
            const pData = await Promise.all(playerIds.map(id => api.fetchPlayerById(id, week)));
            setPlayers(pData as PlayerData[]);
            const hData = await Promise.all(playerIds.map(id => api.fetchPlayerHistory(id)));
            setHistories(hData as HistoryEntry[][]);
        } catch (e) { console.error(e); } 
        finally { setLoadingGraphs(false); }
    };
    fetchAll();
  }, [playerIds, week]);

  const radarData = React.useMemo(() => {
      if (players.length === 0 || histories.length === 0) return [];
      
      const playerTotalTDs = histories.map(hist => 
          hist.reduce((acc: number, game: HistoryEntry) => acc + (game.touchdowns || 0), 0)
      );
      
      const playerRollingSnaps = histories.map(hist => {
          if (!hist || hist.length === 0) return 0;
          // Take last 4 games (hist is sorted desc by week)
          const recentGames = hist.slice(0, 4);
          const totalSnaps = recentGames.reduce((acc: number, game: HistoryEntry) => acc + (game.snap_count || 0), 0);
          return totalSnaps / recentGames.length;
      });

      const maxTDs = Math.max(...playerTotalTDs, 1); // ensure at least 1 to avoid division by zero
      const maxSnaps = Math.max(...playerRollingSnaps, 1);

      const metrics = [
          { key: 'prediction', label: 'Proj', cap: 25 },
          { key: 'average_points', label: 'Avg', cap: 25 },
          { key: 'ROLLING_SNAPS', label: 'Rolling Snaps (4wk)', cap: maxSnaps },
          { key: 'implied_total', label: 'Game Script', cap: 35 }, 
          { key: 'TOTAL_TDS', label: 'Total TDs', cap: maxTDs } 
      ];

      const getPlayerMetric = (p: PlayerData, key: string) => {
          switch (key) {
              case 'prediction': return p.prediction || 0;
              case 'average_points': return p.average_points || 0;
              case 'implied_total': return (p as any).implied_total || (p.overunder ? p.overunder / 2 : 0);
              // non-numeric or unsupported metrics default to 0
              default: return 0;
          }
      };

      return metrics.map(m => {
          const point: { subject: string; fullMark: number; [player: string]: number | string } = { subject: m.label, fullMark: 100 };
          players.forEach((p, idx) => {
              let val = 0;
              if (m.key === 'TOTAL_TDS') {
                  // Normalize by observed max TDS across selected players
                  const tds = playerTotalTDs[idx] || 0;
                  val = m.cap ? (tds / m.cap) * 100 : 0;
              } else if (m.key === 'ROLLING_SNAPS') {
                  val = m.cap ? (playerRollingSnaps[idx] / m.cap) * 100 : 0;
              } else {
                  const base = getPlayerMetric(p, m.key);
                  val = m.cap ? (base / m.cap) * 100 : 0;
              }
              point[`player_${idx}`] = Math.min(Math.max(val, 0), 100);
          });
          return point;
      });
  }, [players, histories]);

  const lineData = React.useMemo(() => {
      if (histories.length === 0) return [];
      const allWeeks = new Set<number>();
      histories.forEach(h => h.forEach((game: HistoryEntry) => allWeeks.add(game.week)));
      const sortedWeeks = Array.from(allWeeks).sort((a, b) => a - b);
      return sortedWeeks.map(week => {
          const point: Record<string, number | string | null> = { week: `Wk ${week}` };
          histories.forEach((hist, idx) => {
              const game = hist.find((g: HistoryEntry) => g.week === week);
              point[`player_${idx}`] = game ? game.points : null;
          });
          return point;
      });
  }, [histories]);

  const dependencyData = React.useMemo(() => {
      if (players.length === 0 || histories.length === 0) return [];
      
      return players.map((p, idx) => {
          const hist = histories[idx] || [];
          if (hist.length === 0) return { name: p.player_name, yards: 0, tds: 0, other: 0 };

          const totalGames = hist.length;
          const totalPts = hist.reduce((acc, g) => acc + (g.points || 0), 0);
          
          // Calculate components (Standard Scoring approx)
          const totalYardsPts = hist.reduce((acc, g) => 
              acc + ((g.passing_yds || 0) * 0.04) + ((g.rushing_yds || 0) * 0.1) + ((g.receiving_yds || 0) * 0.1), 0);
          
          const totalTDPts = hist.reduce((acc, g) => acc + ((g.touchdowns || 0) * 6), 0);
          
          const avgYards = totalYardsPts / totalGames;
          const avgTDs = totalTDPts / totalGames;
          const avgTotal = totalPts / totalGames;
          // "Other" captures PPR, Bonuses, or scoring diffs
          const avgOther = Math.max(0, avgTotal - avgYards - avgTDs);

          return {
              name: p.player_name,
              yards: parseFloat(avgYards.toFixed(1)),
              tds: parseFloat(avgTDs.toFixed(1)),
              other: parseFloat(avgOther.toFixed(1))
          };
      });
  }, [players, histories]);

  return (
    <div className="h-full flex flex-col px-4 pt-4 max-w-[1400px] mx-auto text-slate-900 dark:text-slate-100 pb-10 overflow-y-auto">
      
      {/* HEADER */}
      <div className="text-center mb-2 shrink-0">
        <h2 className="text-xl font-black flex items-center justify-center gap-2">
          <Swords className="text-blue-600 dark:text-blue-400" size={24} /> HEAD TO HEAD
        </h2>
        <p className="text-slate-500 dark:text-slate-400 text-xs">Comparing {playerIds.length} players</p>
      </div>

      {/* DYNAMIC PLAYER GRID */}
      <div className="flex gap-4 overflow-x-auto pb-0 snap-x mx-auto max-w-full items-start px-2 scrollbar-thin scrollbar-thumb-slate-300 dark:scrollbar-thumb-slate-700 shrink-0 min-h-[160px]">
        {playerIds.map((id, idx) => (
            <div key={id} className="snap-center">
                <LoadedPlayerCard 
                    id={id} 
                    week={week} 
                    schedule={schedule} // <-- Pass Schedule Down
                    onRemove={onRemove} 
                    onViewHistory={onViewHistory} 
                    colorIndex={idx}
                />
            </div>
        ))}
        <div className="snap-center pt-0">
            <AddPlayerCard onAdd={onAdd} existingIds={playerIds} />
        </div>
      </div>

      {/* VISUALIZATION SECTION */}
      {playerIds.length > 0 ? (
          <div className="mt-2 bg-white dark:bg-slate-800 rounded-3xl shadow-sm border border-slate-200 dark:border-slate-700 overflow-hidden flex-1 flex flex-col min-h-[320px] sm:min-h-[420px] md:min-h-[600px] mb-20">
            
            {/* TAB BAR */}
            <div className="flex border-b border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900/50 overflow-x-auto w-full">
                {[
                    { id: 'RADAR', label: 'Metric Radar', icon: Radar },
                    { id: 'LINE', label: 'Performance', icon: Activity },
                    { id: 'DEPENDENCY', label: 'Scoring Mix', icon: TrendingUp }
                ].map(tab => (
                    <button 
                        key={tab.id}
                        onClick={() => setActiveTab(tab.id as any)}
                        className={`flex-1 py-3 text-[10px] sm:text-xs font-black uppercase tracking-widest flex items-center justify-center gap-2 transition-colors whitespace-nowrap px-1 ${
                            activeTab === tab.id 
                            ? 'text-blue-600 dark:text-blue-400 bg-white dark:bg-slate-800 border-b-2 border-blue-600 dark:border-blue-400' 
                            : 'text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700'
                        }`}
                    >
                        <tab.icon size={14} className="shrink-0" /> 
                        <span>{tab.label}</span>
                    </button>
                ))}
            </div>

            {/* CHART AREA */}
            <div className="flex-1 w-full h-full min-h-[320px] sm:min-h-[500px] pt-4 sm:pt-12 px-2 sm:px-4">
                {loadingGraphs ? (
                    <div className="h-full flex items-center justify-center text-slate-400 font-bold animate-pulse">Running Simulation...</div>
                ) : (
                    <ResponsiveContainer width="100%" height="100%">
                        {activeTab === 'RADAR' ? (
                            <RadarChart cx="50%" cy="50%" outerRadius="70%" data={radarData}>
                                <PolarGrid stroke="#64748b" strokeOpacity={0.2} />
                                <PolarAngleAxis dataKey="subject" tick={{ fill: '#94a3b8', fontSize: 10, fontWeight: 'bold' }} />
                                <PolarRadiusAxis angle={30} domain={[0, 100]} tick={false} axisLine={false} />
                                <Tooltip contentStyle={{ backgroundColor: '#0f172a', border: 'none', borderRadius: '8px', color: '#fff' }} />
                                {players.map((_, idx) => (
                                    <RechartRadar 
                                        key={idx}
                                        name={players[idx]?.player_name}
                                        dataKey={`player_${idx}`}
                                        stroke={GRAPH_COLORS[idx % GRAPH_COLORS.length]}
                                        strokeWidth={3}
                                        fill={GRAPH_COLORS[idx % GRAPH_COLORS.length]}
                                        fillOpacity={0.2}
                                    />
                                ))}
                                <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }} />
                            </RadarChart>
                        ) : activeTab === 'LINE' ? (
                            <LineChart data={lineData} margin={{ top: 5, right: 10, left: -20, bottom: 5 }}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.2} />
                                <XAxis dataKey="week" tick={{ fill: '#94a3b8', fontSize: 10 }} axisLine={false} tickLine={false} />
                                <YAxis tick={{ fill: '#94a3b8', fontSize: 10 }} axisLine={false} tickLine={false} />
                                <Tooltip contentStyle={{ backgroundColor: '#0f172a', border: 'none', borderRadius: '8px', color: '#fff' }} />
                                <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }} />
                                {players.map((_, idx) => (
                                    <Line 
                                        key={idx}
                                        type="monotone" 
                                        dataKey={`player_${idx}`} 
                                        name={players[idx]?.player_name}
                                        stroke={GRAPH_COLORS[idx % GRAPH_COLORS.length]} 
                                        strokeWidth={3}
                                        dot={{ r: 3 }}
                                        connectNulls
                                    />
                                ))}
                            </LineChart>
                        ) : (
                            <BarChart data={dependencyData} layout="vertical" margin={{ top: 20, right: 10, left: 0, bottom: 5 }}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.2} horizontal={false} />
                                <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 10 }} axisLine={false} tickLine={false} />
                                <YAxis dataKey="name" type="category" tick={{ fill: '#94a3b8', fontSize: 10, fontWeight: 'bold' }} width={70} axisLine={false} tickLine={false} />
                                <Tooltip cursor={{fill: 'transparent'}} contentStyle={{ backgroundColor: '#0f172a', border: 'none', borderRadius: '8px', color: '#fff' }} />
                                <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }} />
                                <Bar dataKey="yards" name="Yards" stackId="a" fill="#3b82f6" radius={[0, 0, 0, 0]} />
                                <Bar dataKey="tds" name="TDs" stackId="a" fill="#10b981" radius={[0, 0, 0, 0]} />
                                <Bar dataKey="other" name="Other" stackId="a" fill="#64748b" radius={[0, 4, 4, 0]} />
                            </BarChart>
                        )}
                    </ResponsiveContainer>
                )}
            </div>
          </div>
      ) : (
        <div className="flex flex-col items-center justify-center h-64 text-slate-400 opacity-50 mt-10">
            <TrendingUp size={48} className="mb-4" />
            <p className="font-bold text-lg">Start Comparison</p>
            <p className="text-sm">Add players to analyze stats side-by-side.</p>
        </div>
      )}
    </div>
  );
};