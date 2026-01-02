import React, { useEffect, useState } from 'react';
import { ArrowLeft, Activity, Users } from 'lucide-react';
import MatchupBanner from './MatchupBanner';
import PlayerCard from './PlayerCard';
import PlayerModal from './PlayerModal';
import { getTeamColor } from '../utils/nflColors';
import type { MatchupData, InjuryData } from '../hooks/useNflData';
import type { PlayerData } from '../types';

interface MatchupViewProps {
  week: number;
  home: string;
  away: string;
  onBack: () => void;
  compareList: string[];
  onToggleCompare: (id: string) => void;
}

type PositionFilter = 'ALL' | 'QB' | 'RB' | 'WR' | 'TE';
type ViewTab = 'ROSTER' | 'INJURIES';
type InjuryFilter = 'ALL' | 'OFFENSE' | 'DEFENSE' | 'SKILL';

const InjuryCard = ({ player }: { player: InjuryData }) => {
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
};

const MatchupView: React.FC<MatchupViewProps> = ({ week, home, away, onBack, compareList, onToggleCompare }) => {
  const [data, setData] = useState<MatchupData | null>(null);
  const [selectedPlayer, setSelectedPlayer] = useState<PlayerData | null>(null);
  const [loading, setLoading] = useState(true);
  const [filterPos, setFilterPos] = useState<PositionFilter>('ALL');
  const [activeTab, setActiveTab] = useState<ViewTab>('ROSTER');
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

  if (loading) return <div className="flex h-full items-center justify-center"><div className="w-8 h-8 border-4 border-blue-600 border-t-transparent rounded-full animate-spin"></div></div>;
  if (!data) return <div className="p-10 text-center text-slate-400">Matchup Data Unavailable</div>;

  const processRoster = (roster: PlayerData[]) => {
    let processed = [...roster];
    if (filterPos !== 'ALL') processed = processed.filter(p => p.position === filterPos);
    return processed.sort((a, b) => (b.average_points || 0) - (a.average_points || 0));
  };

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

  const homeRoster = processRoster(data.home_roster);
  const awayRoster = processRoster(data.away_roster);
  const homeInjuries = filterInjuries(data.home_injuries);
  const awayInjuries = filterInjuries(data.away_injuries);

  return (
    <div className="animate-in fade-in slide-in-from-bottom-4 duration-500 h-full flex flex-col relative bg-slate-50 dark:bg-slate-950 transition-colors duration-300">
      
      <div className="flex items-center justify-between mb-4 shrink-0">
        <button onClick={onBack} className="flex items-center text-xs font-bold text-slate-500 dark:text-slate-400 hover:text-blue-600 dark:hover:text-blue-500 transition-colors">
          <ArrowLeft size={14} className="mr-1" /> Back to Schedule
        </button>
        
        {/* View Tabs */}
        <div className="flex bg-slate-200 dark:bg-slate-800 p-1 rounded-lg">
            <button 
                onClick={() => setActiveTab('ROSTER')}
                className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-xs font-bold transition-all ${activeTab === 'ROSTER' ? 'bg-white dark:bg-slate-700 shadow text-blue-600 dark:text-blue-400' : 'text-slate-500 hover:text-slate-700 dark:text-slate-400'}`}
            >
                <Users size={14} /> Roster
            </button>
            <button 
                onClick={() => setActiveTab('INJURIES')}
                className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-xs font-bold transition-all ${activeTab === 'INJURIES' ? 'bg-white dark:bg-slate-700 shadow text-red-600 dark:text-red-400' : 'text-slate-500 hover:text-slate-700 dark:text-slate-400'}`}
            >
                <Activity size={14} /> Injuries
            </button>
        </div>
      </div>

      <div className="mb-4 rounded-xl overflow-hidden shadow-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 shrink-0">
        <MatchupBanner 
          matchup={data.matchup} 
          overUnder={data.over_under || null}
          spread={data.spread || null}
          homeWinProb={data.home_win_prob || null}
          awayWinProb={data.away_win_prob || null}
        />
      </div>

      <div className="flex flex-1 min-h-0 relative overflow-visible">
        <div className="flex-1 overflow-y-auto pr-12 scrollbar-thin scrollbar-thumb-slate-300 dark:scrollbar-thumb-slate-700 pb-20">
          
          {activeTab === 'ROSTER' ? (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-x-8 gap-y-4">
                <div>
                <div className="flex items-center justify-between mb-2 border-b border-slate-200 dark:border-slate-700 pb-1 sticky top-0 bg-slate-50/95 dark:bg-slate-950/95 backdrop-blur z-20 pt-1">
                    <h3 className="text-xl font-black text-slate-800 dark:text-slate-100">{away}</h3>
                    <span className="text-[10px] font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest">{filterPos}</span>
                </div>
                <div className="space-y-3">
                    {awayRoster.map(p => (
                        <PlayerCard 
                            key={p.player_id} 
                            data={p} 
                            teamColor={getTeamColor(away)}
                            onClick={setSelectedPlayer}
                            isSelected={compareList.includes(p.player_id)}
                            onToggleCompare={onToggleCompare}
                        />
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
                        <PlayerCard 
                            key={p.player_id} 
                            data={p} 
                            teamColor={getTeamColor(home)}
                            onClick={setSelectedPlayer}
                            isSelected={compareList.includes(p.player_id)}
                            onToggleCompare={onToggleCompare}
                        />
                    ))}
                </div>
                </div>
            </div>
          ) : (
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
                                <InjuryCard key={p.player_id} player={p} />
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
                                <InjuryCard key={p.player_id} player={p} />
                            )) : (
                                <div className="text-center py-8 text-slate-400 text-sm italic">No injuries reported</div>
                            )}
                        </div>
                    </div>
                </div>
            </div>
          )}

        </div>

        {activeTab === 'ROSTER' && (
            <div className="absolute right-0 -mr-6 top-1/2 -translate-y-1/2 flex flex-col gap-0 z-50">
            {(['ALL', 'QB', 'RB', 'WR', 'TE'] as PositionFilter[]).map((pos) => (
                <button key={pos} onClick={() => setFilterPos(pos)} className={`h-16 w-10 text-[10px] font-black tracking-widest flex items-center justify-center transition-all duration-200 border-y border-l rounded-l-lg border-r-0 shadow-sm ${filterPos === pos ? 'bg-blue-600 text-white border-blue-500 w-12 shadow-lg z-20' : 'bg-white dark:bg-slate-800 text-slate-400 dark:text-slate-500 border-slate-200 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-700 z-10'} [writing-mode:vertical-rl] rotate-180 mb-[-1px]`}>{pos}</button>
            ))}
            </div>
        )}
      </div>

      {selectedPlayer && <PlayerModal player={selectedPlayer} onClose={() => setSelectedPlayer(null)} />}
    </div>
  );
};

export default MatchupView;