import React from 'react';
import { TrendingUp, ExternalLink, Plus, Check } from 'lucide-react';
import { getTeamColor } from '../utils/nflColors';
import type { BroadcastCardData } from '../hooks/useNflData';

interface BroadcastCardProps {
  data: BroadcastCardData | null;
  loading?: boolean;
  mini?: boolean;
  showProps?: boolean;
  onClick?: () => void;
  // Compare Logic
  isSelected?: boolean;
  onToggleCompare?: (id: string) => void;
}

const BroadcastCard: React.FC<BroadcastCardProps> = ({ 
  data, 
  loading, 
  mini, 
  showProps = true, 
  onClick,
  isSelected,
  onToggleCompare
}) => {
  if (loading) {
    return (
      <div className="w-full h-96 bg-white dark:bg-slate-800 rounded-3xl animate-pulse shadow-xl border border-slate-200 dark:border-slate-700 relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/10 to-transparent skew-x-12 animate-shimmer"></div>
      </div>
    );
  }

  if (!data) return null;

  // --- DATA MAPPING FIX ---
  // Removed 'spread' from destructuring as it was unused
  const { id, name, position, team, image, injury_status, stats, props } = data;
  
  // Stats
  const prediction = stats?.projected || 0;
  const floor = stats?.floor || 0;
  const average = stats?.average || 0;
  const snapPct = stats?.snap_percentage ? (stats.snap_percentage * 100).toFixed(0) : "0";
  // Removed unused 'snapCount' variable
  
  // Props Logic (Find specific props in the array)
  const findProp = (keywords: string[]) => {
    if (!props) return undefined;
    return props.find(p => {
        const type = p.prop_type.toLowerCase();
        return keywords.some(k => type.includes(k.toLowerCase()));
    });
  };

  const passProp = findProp(['Passing Yards', 'Pass Yards', 'Pass Yds']);
  const rushProp = findProp(['Rushing Yards', 'Rush Yards', 'Rush Yds']);
  const recProp = findProp(['Receiving Yards', 'Rec Yards', 'Rec Yds']);
  const tdProp = findProp(['Anytime TD', 'Anytime Touchdown']);

  // Determine Main Prop to Show
  let mainProp = recProp;
  let mainPropLabel = "Receiving Yards";
  if (position === 'QB') { mainProp = passProp; mainPropLabel = "Passing Yards"; }
  else if (position === 'RB') { mainProp = rushProp; mainPropLabel = "Rushing Yards"; }

  const teamColor = getTeamColor(team);
  
  // Status Logic
  const getStatusColor = (s: string) => {
    if (!s) return 'bg-slate-100 text-slate-500';
    const lower = s.toLowerCase();
    if (lower.includes('out') || lower.includes('ir')) return 'bg-red-100 text-red-700 border-red-200';
    if (lower.includes('questionable')) return 'bg-yellow-100 text-yellow-700 border-yellow-200';
    return 'bg-green-100 text-green-700 border-green-200';
  };

  return (
    <div 
      onClick={onClick}
      className={`group relative w-full bg-white dark:bg-slate-900 rounded-3xl shadow-2xl overflow-hidden border transition-all duration-500 ${
        isSelected 
          ? 'border-blue-500 ring-4 ring-blue-500/20' 
          : 'border-slate-200 dark:border-slate-800 hover:border-slate-300 dark:hover:border-slate-700'
      } ${mini ? '' : 'max-w-md mx-auto'}`}
    >
      
      {/* --- COMPARE BUTTON --- */}
      {onToggleCompare && (
        <button
          onClick={(e) => { e.stopPropagation(); onToggleCompare(id); }}
          className={`absolute top-4 right-4 z-30 p-2 rounded-full shadow-lg transition-all transform hover:scale-110 ${
            isSelected 
              ? 'bg-blue-600 text-white' 
              : 'bg-white/90 dark:bg-slate-800/90 text-slate-400 hover:text-blue-500 backdrop-blur-sm'
          }`}
        >
          {isSelected ? <Check size={18} strokeWidth={4} /> : <Plus size={18} strokeWidth={3} />}
        </button>
      )}

      {/* --- HERO HEADER --- */}
      <div className="relative h-48 overflow-hidden">
        <div className="absolute inset-0" style={{ background: `linear-gradient(135deg, ${teamColor} 0%, #0f172a 100%)` }}></div>
        <div className="absolute inset-0 opacity-30 bg-[url('https://www.transparenttextures.com/patterns/carbon-fibre.png')]"></div>
        
        {/* Big Team Text Background */}
        <h1 className="absolute -bottom-6 -left-4 text-9xl font-black text-white opacity-5 select-none tracking-tighter italic">
          {team}
        </h1>

        <div className="absolute inset-0 flex flex-col items-center justify-center pt-4 z-10">
          <div className="relative">
            <div className="w-28 h-28 rounded-full p-1 bg-gradient-to-br from-white/20 to-transparent shadow-2xl backdrop-blur-md">
               {image ? (
                 <img src={image} alt={name} className="w-full h-full rounded-full object-cover border-4 border-white/10 bg-slate-800" />
               ) : (
                 <div className="w-full h-full rounded-full bg-slate-700 flex items-center justify-center text-white/50 font-bold">IMG</div>
               )}
            </div>
            <div className={`absolute bottom-0 right-0 px-3 py-1 rounded-full text-[10px] font-black uppercase tracking-widest shadow-lg border border-white/20 ${getStatusColor(injury_status || 'Active')}`}>
              {injury_status || 'Active'}
            </div>
          </div>
          
          <div className="text-center mt-3">
            <h2 className="text-2xl font-black text-white tracking-tight leading-none drop-shadow-md">{name}</h2>
            <div className="flex items-center justify-center gap-2 mt-1 text-xs font-bold text-white/60 uppercase tracking-widest">
              <span>{position}</span>
              <span className="w-1 h-1 bg-white/40 rounded-full"></span>
              <span>{team}</span>
            </div>
          </div>
        </div>
      </div>

      {/* --- STATS GRID --- */}
      <div className="p-6">
        <div className="grid grid-cols-2 gap-4 mb-6">
          
          {/* Main Projection Box */}
          <div className="bg-slate-50 dark:bg-slate-800/50 p-4 rounded-2xl border border-slate-100 dark:border-slate-700/50 text-center group-hover:scale-[1.02] transition-transform duration-300 flex flex-col justify-center">
            <div className="text-[10px] uppercase font-black text-slate-400 dark:text-slate-500 tracking-widest mb-1">Projected</div>
            <div className="text-4xl font-black text-slate-800 dark:text-white tracking-tighter leading-none">
              {prediction.toFixed(1)}<span className="text-lg text-slate-400 font-bold ml-0.5">pts</span>
            </div>
          </div>

          {/* Secondary Stats */}
          <div className="space-y-2">
             <div className="flex justify-between items-center bg-slate-50 dark:bg-slate-800/30 px-3 py-2 rounded-xl border border-slate-100 dark:border-slate-700/30">
                <span className="text-xs font-bold text-slate-400">Floor</span>
                <span className="text-sm font-black text-slate-700 dark:text-slate-300">{floor.toFixed(1)}</span>
             </div>
             <div className="flex justify-between items-center bg-slate-50 dark:bg-slate-800/30 px-3 py-2 rounded-xl border border-slate-100 dark:border-slate-700/30">
                <span className="text-xs font-bold text-slate-400">Avg</span>
                <span className="text-sm font-black text-slate-700 dark:text-slate-300">{average.toFixed(1)}</span>
             </div>
             <div className="flex justify-between items-center bg-slate-50 dark:bg-slate-800/30 px-3 py-2 rounded-xl border border-slate-100 dark:border-slate-700/30">
                <span className="text-xs font-bold text-slate-400">Snap %</span>
                <span className="text-sm font-black text-slate-700 dark:text-slate-300">{snapPct}%</span>
             </div>
          </div>
        </div>

        {/* --- PROPS SECTION --- */}
        {showProps && (
          <div className="space-y-3">
            <div className="flex items-center gap-2 mb-2">
               <TrendingUp size={16} className="text-blue-500" />
               <h3 className="text-xs font-black uppercase tracking-widest text-slate-400">Market Props</h3>
            </div>

            {/* Main Yardage Prop */}
            <div className="flex items-center justify-between p-3 rounded-xl bg-slate-50 dark:bg-slate-800/50 border border-slate-100 dark:border-slate-700/50">
               <span className="text-xs font-bold text-slate-600 dark:text-slate-400">{mainPropLabel}</span>
               <div className="flex items-center gap-3">
                  <span className="font-mono font-bold text-slate-800 dark:text-white">{mainProp?.line || '-'}</span>
                  {mainProp?.implied_prob && (
                    <span className={`text-[10px] font-black px-1.5 py-0.5 rounded ${mainProp.implied_prob > 50 ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
                      {mainProp.implied_prob.toFixed(1)}%
                    </span>
                  )}
               </div>
            </div>

            {/* Anytime TD Prop */}
            <div className="flex items-center justify-between p-3 rounded-xl bg-slate-50 dark:bg-slate-800/50 border border-slate-100 dark:border-slate-700/50">
               <span className="text-xs font-bold text-slate-600 dark:text-slate-400">Anytime TD</span>
               <div className="flex items-center gap-3">
                  {tdProp?.implied_prob ? (
                    <>
                      <div className="w-16 h-1.5 bg-slate-200 dark:bg-slate-700 rounded-full overflow-hidden">
                        <div className="h-full bg-blue-500 rounded-full" style={{ width: `${tdProp.implied_prob}%` }}></div>
                      </div>
                      <span className="font-mono font-bold text-slate-800 dark:text-white">{tdProp.implied_prob.toFixed(1)}%</span>
                    </>
                  ) : (
                    <span className="text-xs text-slate-400">-</span>
                  )}
               </div>
            </div>
          </div>
        )}

        {/* --- FOOTER ACTION --- */}
        <div className="mt-6 pt-4 border-t border-slate-100 dark:border-slate-800 flex justify-between items-center text-[10px] font-bold text-slate-400 uppercase tracking-widest cursor-pointer hover:text-blue-500 transition-colors">
           <span>View Full History</span>
           <ExternalLink size={12} />
        </div>
      </div>
    </div>
  );
};

export default BroadcastCard;