import React, { useEffect, useState } from 'react';
import { ArrowLeft } from 'lucide-react';
import MatchupBanner from './MatchupBanner';
import PlayerCard from './PlayerCard';
import PlayerModal from './PlayerModal';
import { getTeamColor } from '../utils/nflColors';
import type { MatchupData, PlayerData } from '../types';

interface MatchupViewProps {
  week: number;
  home: string;
  away: string;
  onBack: () => void;
  // Compare Props
  compareList: string[];
  onToggleCompare: (id: string) => void;
}

type PositionFilter = 'ALL' | 'QB' | 'RB' | 'WR' | 'TE';

const MatchupView: React.FC<MatchupViewProps> = ({ week, home, away, onBack, compareList, onToggleCompare }) => {
  const [data, setData] = useState<MatchupData | null>(null);
  const [selectedPlayer, setSelectedPlayer] = useState<PlayerData | null>(null);
  const [loading, setLoading] = useState(true);
  const [filterPos, setFilterPos] = useState<PositionFilter>('ALL');

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

  const homeRoster = processRoster(data.home_roster);
  const awayRoster = processRoster(data.away_roster);

  return (
    <div className="animate-in fade-in slide-in-from-bottom-4 duration-500 h-full flex flex-col relative bg-slate-50 dark:bg-slate-950 transition-colors duration-300">
      
      <div className="flex items-center justify-between mb-4 shrink-0">
        <button onClick={onBack} className="flex items-center text-xs font-bold text-slate-500 dark:text-slate-400 hover:text-blue-600 dark:hover:text-blue-500 transition-colors">
          <ArrowLeft size={14} className="mr-1" /> Back to Schedule
        </button>
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
        </div>

        <div className="absolute right-0 -mr-6 top-1/2 -translate-y-1/2 flex flex-col gap-0 z-50">
          {(['ALL', 'QB', 'RB', 'WR', 'TE'] as PositionFilter[]).map((pos) => (
            <button key={pos} onClick={() => setFilterPos(pos)} className={`h-16 w-10 text-[10px] font-black tracking-widest flex items-center justify-center transition-all duration-200 border-y border-l rounded-l-lg border-r-0 shadow-sm ${filterPos === pos ? 'bg-blue-600 text-white border-blue-500 w-12 shadow-lg z-20' : 'bg-white dark:bg-slate-800 text-slate-400 dark:text-slate-500 border-slate-200 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-700 z-10'} [writing-mode:vertical-rl] rotate-180 mb-[-1px]`}>{pos}</button>
          ))}
        </div>
      </div>

      {selectedPlayer && <PlayerModal player={selectedPlayer} onClose={() => setSelectedPlayer(null)} />}
    </div>
  );
};

export default MatchupView;