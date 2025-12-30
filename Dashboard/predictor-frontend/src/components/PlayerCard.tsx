import React from 'react';
import { Plus, Check } from 'lucide-react';
import type { PlayerData } from '../types';

interface PlayerCardProps {
  data: PlayerData;
  teamColor?: string;
  onClick: (player: PlayerData) => void;
  // Compare Logic
  isSelected?: boolean;
  onToggleCompare?: (id: string) => void;
}

const PlayerCard: React.FC<PlayerCardProps> = ({ 
    data, 
    teamColor = '#3b82f6', 
    onClick,
    isSelected,
    onToggleCompare
}) => {
  const getProbColor = (prob: number | null) => {
    if (!prob) return "text-slate-400 dark:text-white/40";
    if (prob >= 55) return "text-green-600 dark:text-emerald-300 font-black";
    if (prob <= 45) return "text-red-500 dark:text-rose-400";
    return "text-slate-600 dark:text-white/70";
  };

  const getMainPropLabel = () => {
    if (data.position === 'QB') return 'Pass Yds';
    if (data.position === 'RB') return 'Rush Yds';
    return 'Rec Yds';
  };

  // --- INJURY BADGE LOGIC ---
  const getStatusBadge = (status?: string) => {
    if (!status || status === 'Active' || status === 'ACT' || status === 'Healthy') return null;
    
    const s = status.toLowerCase();
    let colorClass = 'bg-slate-200 text-slate-600 border-slate-300';
    
    if (s.includes('out') || s.includes('ir') || s.includes('inactive')) {
      colorClass = 'bg-red-100 text-red-700 border-red-200 dark:bg-red-900/40 dark:text-red-300 dark:border-red-800';
    } else if (s.includes('doubtful')) {
      colorClass = 'bg-orange-100 text-orange-700 border-orange-200 dark:bg-orange-900/40 dark:text-orange-300 dark:border-orange-800';
    } else if (s.includes('questionable')) {
      colorClass = 'bg-yellow-100 text-yellow-700 border-yellow-200 dark:bg-yellow-900/40 dark:text-yellow-300 dark:border-yellow-800';
    }

    return (
      <span className={`ml-2 text-[9px] font-black px-1.5 py-0.5 rounded border uppercase tracking-wide ${colorClass}`}>
        {status === 'Questionable' ? 'Q' : status}
      </span>
    );
  };

  // --- OPPORTUNITY BOOST BADGE ---
  const getBoostBadge = () => {
    if (!data.is_injury_boosted) return null;

    // FIX: Do not show boost if the player themselves is injured/out
    const status = data.injury_status?.toLowerCase() || '';
    if (status.includes('out') || status.includes('ir') || status.includes('inactive') || status.includes('doubtful')) {
        return null;
    }

    return (
      <span className="ml-2 text-[9px] font-black px-1.5 py-0.5 rounded border border-amber-200 bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300 dark:border-amber-800 uppercase tracking-wide animate-pulse shadow-[0_0_10px_rgba(251,191,36,0.2)]">
        Boost
      </span>
    );
  };

  return (
    <div 
      onClick={() => onClick(data)}
      className={`relative rounded-xl p-3 cursor-pointer group shadow-sm hover:shadow-lg transition-all duration-300 flex flex-col h-32 overflow-hidden border bg-white dark:bg-slate-900 ${
        isSelected 
          ? 'border-blue-500 ring-2 ring-blue-500/20 dark:ring-blue-500/40' 
          : 'border-slate-200 dark:border-white/10 hover:border-blue-300 dark:hover:border-blue-700'
      }`}
    >
      {/* --- DYNAMIC BACKGROUND FADE --- */}
      <div className="absolute inset-0 opacity-15 dark:opacity-40 transition-opacity">
        <div 
            className="absolute inset-0"
            style={{ background: `linear-gradient(135deg, ${teamColor} 0%, transparent 100%)` }}
        ></div>
      </div>
      
      {/* Dark Mode Overlay */}
      <div className="absolute inset-0 hidden dark:block bg-gradient-to-br from-transparent to-slate-950/90 pointer-events-none"></div>

      {/* --- COMPARE BUTTON --- */}
      {onToggleCompare && (
          <button 
            onClick={(e) => { e.stopPropagation(); onToggleCompare(data.player_id); }}
            className={`absolute top-2 right-2 z-20 p-1.5 rounded-full transition-all shadow-sm ${
              isSelected 
                ? 'bg-blue-600 text-white hover:bg-blue-700' 
                : 'bg-white/80 dark:bg-black/40 text-slate-400 hover:text-blue-500 hover:bg-white dark:hover:bg-slate-800'
            }`}
          >
            {isSelected ? <Check size={12} strokeWidth={4} /> : <Plus size={12} strokeWidth={3} />}
          </button>
      )}

      {/* --- HEADER GRID --- */}
      <div className="grid grid-cols-[auto_1fr_auto] gap-3 mb-1 items-center relative z-10">
        <img 
          src={data.image} 
          alt={data.player_name} 
          className="w-10 h-10 rounded-full object-cover bg-slate-100 dark:bg-black/30 border-2 border-white dark:border-white/20 shadow-md" 
        />
        
        <div className="min-w-0 flex flex-col justify-center">
          <div className="flex items-center">
             <h4 className="text-slate-900 dark:text-white font-black truncate text-sm leading-tight">
               {data.player_name}
             </h4>
             {/* INJURY STATUS */}
             {getStatusBadge(data.injury_status)}
             {/* BOOST STATUS */}
             {getBoostBadge()}
          </div>
          
          <div className="flex items-center gap-1.5 text-[10px] text-slate-500 dark:text-white/60 mt-0.5 truncate font-medium">
            <span className="bg-slate-100 dark:bg-white/10 border border-slate-200 dark:border-white/5 px-1.5 rounded font-bold font-mono text-slate-700 dark:text-slate-300">{data.position}</span>
            <span>•</span>
            <span className="truncate opacity-80">{data.team} vs {data.opponent}</span>
          </div>
        </div>

        <div className="flex gap-2 text-right pl-2 border-l border-slate-200 dark:border-white/10 mr-6">
          <div className="flex flex-col items-end justify-center min-w-[32px]">
            <span className="text-[9px] text-slate-400 dark:text-white/50 uppercase font-bold tracking-widest">Avg</span>
            <span className="text-xs font-bold text-slate-700 dark:text-white/80">{data.average_points}</span>
          </div>
          <div className="flex flex-col items-end justify-center min-w-[36px]">
            <span className="text-[9px] text-blue-500 dark:text-blue-400 uppercase font-bold tracking-wider">Proj</span>
            <span className={`text-base font-black leading-none ${data.is_injury_boosted ? 'text-amber-500 dark:text-amber-400' : 'text-slate-900 dark:text-white'}`}>
              {data.prediction}
            </span>
          </div>
        </div>
      </div>

      {/* --- VEGAS PROPS GRID --- */}
      <div className="bg-slate-50 dark:bg-black/20 rounded-lg px-2 py-1.5 space-y-1 border border-slate-100 dark:border-white/5 mt-auto relative z-10 backdrop-blur-sm">
        
        <div className="flex justify-between items-center text-[10px]">
          <span className="text-slate-500 dark:text-white/60 font-medium">{getMainPropLabel()}</span>
          <div className="flex items-center gap-2">
            <span className="text-slate-700 dark:text-white font-mono bg-white dark:bg-white/5 px-1.5 rounded shadow-sm border border-slate-100 dark:border-transparent">
              {data.prop_line ? `${data.prop_line}` : '-'}
            </span>
            <span className={`min-w-[28px] text-right ${getProbColor(data.prop_prob)}`}>
              {data.prop_prob ? `${data.prop_prob}%` : ''}
            </span>
          </div>
        </div>

        {data.position === 'QB' && (
          <div className="flex justify-between items-center text-[10px]">
            <span className="text-slate-500 dark:text-white/60 font-medium">Pass TDs</span>
            <div className="flex items-center gap-2">
              <span className="text-slate-700 dark:text-white font-mono bg-white dark:bg-white/5 px-1.5 rounded shadow-sm border border-slate-100 dark:border-transparent">
                {data.pass_td_line ? `${data.pass_td_line}` : '-'}
              </span>
              <span className={`min-w-[28px] text-right ${getProbColor(data.pass_td_prob)}`}>
                {data.pass_td_prob ? `${data.pass_td_prob}%` : ''}
              </span>
            </div>
          </div>
        )}

        <div className="flex justify-between items-center text-[10px] border-t border-slate-200 dark:border-white/10 pt-1">
          <span className="text-slate-500 dark:text-white/60 font-medium">Anytime TD</span>
          <span className={`font-mono ${getProbColor(data.anytime_td_prob)}`}>
            {data.anytime_td_prob ? `${data.anytime_td_prob}%` : '-'}
          </span>
        </div>

      </div>
    </div>
  );
};

export default PlayerCard;