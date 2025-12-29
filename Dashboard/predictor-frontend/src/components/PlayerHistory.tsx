import { ArrowLeft } from 'lucide-react';
import { usePlayerHistory, usePlayerProfileById } from '../hooks/useNflData';
import { getTeamColor } from '../utils/nflColors';

interface Props {
  playerId: string;
  onBack: () => void;
}

export default function PlayerHistory({ playerId, onBack }: Props) {
  const { history, loadingHistory } = usePlayerHistory(playerId);
  const { cardData } = usePlayerProfileById(playerId);

  const teamColor = cardData ? getTeamColor(cardData.team) : "#1e293b"; 

  // Stats
  const totalPoints = history.reduce((acc, curr) => acc + (curr.points || 0), 0);
  const totalTDs = history.reduce((acc, curr) => acc + (curr.touchdowns || 0), 0);
  const avgPoints = history.length > 0 ? (totalPoints / history.length).toFixed(1) : "0.0";
  const avgSnapPct = history.length > 0 
    ? (history.reduce((acc, curr) => acc + (curr.snap_percentage || 0), 0) / history.length * 100).toFixed(0) 
    : "0";

  return (
    <div className="w-full animate-in fade-in slide-in-from-bottom-4 duration-500">
      <button onClick={onBack} className="mb-6 flex items-center text-sm font-bold text-slate-400 hover:text-blue-600 dark:hover:text-blue-400 transition-colors">
        <ArrowLeft size={16} className="mr-1"/> BACK TO DASHBOARD
      </button>

      {/* HEADER CARD WITH GRADIENT BLEND */}
      <div 
        className="rounded-xl shadow-lg border border-white/10 p-6 mb-8 text-white relative overflow-hidden transition-all duration-500"
        style={{ 
            background: `linear-gradient(135deg, ${teamColor} 0%, #0f172a 100%)` 
        }}
      >
        {/* Decorative Blur Effect */}
        <div className="absolute -right-20 -top-20 w-96 h-96 bg-white/5 rounded-full blur-[100px] pointer-events-none mix-blend-overlay"></div>
        
        <div className="flex flex-col xl:flex-row items-center justify-between gap-6 relative z-10">
           <div className="flex items-center gap-6 w-full xl:w-auto">
               <div className="h-24 w-24 rounded-full border-4 border-white/20 bg-black/20 overflow-hidden shadow-2xl shrink-0">
                   {cardData?.image ? <img src={cardData.image} alt={cardData.name} className="w-full h-full object-cover" /> : <div className="w-full h-full flex items-center justify-center text-white/50 font-bold">...</div>}
               </div>
               <div>
                   <h1 className="text-3xl md:text-4xl font-black tracking-tight uppercase drop-shadow-md">{cardData?.name || "Loading..."}</h1>
                   <div className="flex flex-wrap items-center gap-2 md:gap-3 text-white/90 font-bold text-sm mt-1">
                        <span className="bg-black/30 px-2 py-0.5 rounded text-white border border-white/10">{cardData?.position}</span>
                        <span>{cardData?.team}</span>
                        <span className="text-white/50 hidden md:inline">•</span>
                        <span className={`px-2 py-0.5 rounded font-black text-xs ${cardData?.injury_status === 'Active' ? 'bg-green-500 text-white' : 'bg-red-500 text-white'}`}>
                            {cardData?.injury_status || 'ACT'}
                        </span>
                   </div>
               </div>
           </div>

           {/* RESPONSIVE STATS GRID */}
           <div className="w-full xl:w-auto grid grid-cols-3 md:grid-cols-5 gap-4 gap-y-6 text-center bg-black/20 p-4 rounded-xl border border-white/10 backdrop-blur-md shadow-inner">
               <div>
                   <div className="text-[10px] font-bold text-white/60 uppercase tracking-widest">Wk Proj</div>
                   <div className="text-xl md:text-2xl font-black text-yellow-400 drop-shadow-sm">{cardData?.stats.projected.toFixed(1) || "-"}</div>
               </div>
               <div className="hidden md:block w-px bg-white/10 h-full mx-auto"></div>
               <div>
                   <div className="text-[10px] font-bold text-white/60 uppercase tracking-widest">Avg</div>
                   <div className="text-xl md:text-2xl font-black">{avgPoints}</div>
               </div>
               <div>
                   <div className="text-[10px] font-bold text-white/60 uppercase tracking-widest">TDs</div>
                   <div className="text-xl md:text-2xl font-black">{totalTDs}</div>
               </div>
               <div>
                   <div className="text-[10px] font-bold text-white/60 uppercase tracking-widest">Usage</div>
                   <div className="text-xl md:text-2xl font-black">{avgSnapPct}%</div>
               </div>
           </div>
        </div>
      </div>

      {/* HISTORY TABLE */}
      <div className="bg-white dark:bg-slate-800 rounded-xl shadow-sm border border-slate-200 dark:border-slate-700 overflow-hidden flex flex-col transition-colors duration-300">
        {loadingHistory ? <div className="p-20 text-center text-slate-400 dark:text-slate-500 animate-pulse font-bold">Loading Season History...</div> : (
          <div className="overflow-x-auto scrollbar-thin dark:scrollbar-thumb-slate-600 dark:scrollbar-track-slate-800">
            <table className="w-full text-sm text-left whitespace-nowrap">
              <thead className="bg-slate-50 dark:bg-slate-900/50 text-slate-500 dark:text-slate-400 font-bold uppercase text-xs tracking-wider border-b border-slate-100 dark:border-slate-700">
                  <tr>
                      <th className="px-6 py-4 sticky left-0 bg-slate-50 dark:bg-slate-900 z-10">Week</th>
                      <th className="px-6 py-4">Opponent</th>
                      <th className="px-6 py-4 text-center">Snaps</th>
                      <th className="px-6 py-4 text-center">Rec / Tgt</th>
                      {/* EXTENDED STATS */}
                      <th className="px-6 py-4 text-right">Rush Att</th> 
                      <th className="px-6 py-4 text-right">Rush Yds</th>
                      <th className="px-6 py-4 text-right">Pass Yds</th>
                      <th className="px-6 py-4 text-right">Rec Yds</th>
                      <th className="px-6 py-4 text-right">TDs</th>
                      <th className="px-6 py-4 text-right bg-slate-50/50 dark:bg-slate-800/50">Pts</th>
                  </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-700">
                  {history.map((game) => (
                  <tr key={game.week} className="hover:bg-slate-50/50 dark:hover:bg-slate-700/50 transition-colors">
                      <td className="px-6 py-4 font-bold text-slate-800 dark:text-slate-100 sticky left-0 bg-white dark:bg-slate-800 z-10 border-r border-slate-100 dark:border-slate-700">Week {game.week}</td>
                      <td className="px-6 py-4"><span className="px-2 py-1 bg-slate-100 dark:bg-slate-700 rounded text-slate-600 dark:text-slate-300 font-bold text-xs border border-slate-200 dark:border-slate-600">{game.opponent}</span></td>
                      <td className="px-6 py-4 text-center"><span className="font-bold text-slate-700 dark:text-slate-300">{(game.snap_percentage * 100).toFixed(0)}%</span></td>
                      <td className="px-6 py-4 text-center font-medium text-slate-600 dark:text-slate-400">{game.receptions} <span className="text-slate-300 dark:text-slate-600 mx-1">/</span> {game.targets}</td>
                      
                      <td className="px-6 py-4 text-right text-slate-600 dark:text-slate-400 font-medium">{game.carries !== undefined ? game.carries : "-"}</td> 
                      
                      <td className="px-6 py-4 text-right text-slate-600 dark:text-slate-400 font-medium">{game.rushing_yds || "-"}</td>
                      <td className="px-6 py-4 text-right text-slate-600 dark:text-slate-400 font-medium">{game.passing_yds || "-"}</td>
                      <td className="px-6 py-4 text-right text-slate-600 dark:text-slate-400 font-medium">{game.receiving_yds || "-"}</td>
                      <td className="px-6 py-4 text-right text-slate-600 dark:text-slate-400 font-medium">{game.touchdowns > 0 ? game.touchdowns : "-"}</td>
                      <td className="px-6 py-4 text-right bg-slate-50/30 dark:bg-slate-900/30"><span className={`font-black text-base ${game.points >= 15 ? 'text-green-600 dark:text-green-400' : 'text-slate-700 dark:text-slate-300'}`}>{game.points?.toFixed(1) || "0.0"}</span></td>
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