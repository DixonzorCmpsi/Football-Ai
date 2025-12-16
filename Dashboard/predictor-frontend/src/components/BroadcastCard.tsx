import { Target, TrendingUp, Clock, MousePointerClick, TrendingDown } from 'lucide-react';
import type { BroadcastCardData } from '../hooks/useNflData';
import { getTeamColor } from '../utils/nflColors';

interface Props {
  data: BroadcastCardData;
  onClick?: (id: string) => void;
  mini?: boolean;
}

// --- HELPER FUNCTIONS (for Dark Mode and Status) ---

const getInjuryColor = (status: string) => {
    if (!status) return 'hidden';
    const s = status.toLowerCase();
    
    if (s.includes('out') || s.includes('ir')) return 'bg-red-600';
    if (s.includes('doubtful')) return 'bg-orange-600';
    if (s.includes('questionable')) return 'bg-yellow-500';
    if (s.includes('active')) return 'bg-emerald-500'; 
    return 'bg-slate-500'; 
};

const getInjuryLabel = (status: string) => {
    if (!status) return '';
    const s = status.toLowerCase();
    
    if (s.includes('out')) return 'OUT';
    if (s.includes('ir')) return 'IR';
    if (s.includes('doubtful')) return 'D';
    if (s.includes('questionable')) return 'Q';
    if (s.includes('active')) return 'ACT';
    return status.substring(0, 3).toUpperCase();
};

// --- NEW HELPER: Format Over/Under ---
const formatOverUnder = (overUnderValue: string | number | null) => {
    if (overUnderValue === null || overUnderValue === 'N/A' || overUnderValue === '') {
        return "N/A";
    }
    // We expect the backend to pass the 'total_line' as a number/string
    const value = parseFloat(overUnderValue as string);
    if (isNaN(value)) return "N/A";

    return (
        <span className="text-blue-600 dark:text-blue-400 font-black">
            {value.toFixed(1)}
        </span>
    );
}

// --- MAIN COMPONENT ---
export default function BroadcastCard({ data, onClick, mini = false }: Props) {
  if (!data) return null;
  const teamColor = getTeamColor(data.team);
  const injuryColor = getInjuryColor(data.injury_status);
  const injuryLabel = getInjuryLabel(data.injury_status);
  
  // Spread logic is kept here but the display section is changed to use Over/Under
  const isFavored = data.spread !== null && data.spread < 0;
  const overUnderDisplay = formatOverUnder(data.spread); // Reusing data.spread, but using the correct column should be used from the backend.

  // NOTE: Your backend needs to pass the 'total_line' value to the 'spread' field in BroadcastCardData 
  // or add a new 'total_line' field. Assuming your dataPrep script updates the DB 
  // and the server passes the Total Line value in 'spread' for this section.
  
  const spreadValue = data.spread;
  const isSpreadData = spreadValue !== null && !isNaN(spreadValue);


  return (
    <div 
      onClick={() => onClick && onClick(data.id)}
      className={`bg-white dark:bg-slate-800 rounded-2xl shadow-sm overflow-hidden border border-slate-200 dark:border-slate-700 cursor-pointer hover:ring-4 hover:ring-opacity-50 transition-all group relative ${mini ? 'max-w-md mx-auto mb-4' : 'max-w-2xl mx-auto mt-8'}`}
      style={{ '--ring-color': teamColor } as React.CSSProperties} 
    >
      {/* INJURY BADGE */}
      {injuryLabel && (
          <div className={`absolute top-0 right-0 z-50 px-3 py-1 text-[10px] font-black text-white rounded-bl-xl ${injuryColor} shadow-md border-b border-l border-white/20`}>
              {injuryLabel}
          </div>
      )}

      {/* Hover Hint */}
      <div className="absolute top-4 right-14 bg-white/20 backdrop-blur text-white px-3 py-1 rounded-full text-xs font-bold opacity-0 group-hover:opacity-100 transition-opacity flex items-center gap-2 z-20">
        <MousePointerClick size={14} /> View History
      </div>

      {/* 1. HERO HEADER */}
      <div 
        className={`${mini ? 'p-4' : 'p-6'} text-white relative overflow-hidden transition-colors duration-500`}
        style={{ backgroundColor: teamColor }}
      >
        <div className="absolute top-0 right-0 w-64 h-64 bg-white rounded-full blur-[80px] opacity-10 -translate-y-1/2 translate-x-1/2 pointer-events-none"></div>
        
        <div className="flex items-center gap-6 relative z-10">
          <div className={`${mini ? 'w-16 h-16' : 'w-24 h-24'} rounded-full border-4 border-white/20 overflow-hidden bg-slate-900/50 shadow-lg shrink-0`}>
            {data.image ? (
                <img src={data.image} alt={data.name} className="w-full h-full object-cover" />
            ) : (
                <div className="w-full h-full flex items-center justify-center text-white/50 text-xs">NO IMG</div>
            )}
          </div>
          <div>
            <h2 className={`${mini ? 'text-xl' : 'text-3xl'} font-black tracking-tighter uppercase drop-shadow-md`}>{data.name}</h2>
            <div className="flex items-center gap-3 mt-1">
              <span className="px-2 py-0.5 bg-white/20 backdrop-blur rounded text-xs font-bold border border-white/10">{data.position}</span>
              <span className="text-white/80 font-bold tracking-widest text-xs">{data.team}</span>
              {!mini && <span className="text-white/60 text-xs ml-2 border-l border-white/30 pl-3">{data.draft}</span>}
            </div>
          </div>
        </div>
      </div>

      {/* 2. KEY METRICS GRID */}
      <div className="grid grid-cols-3 divide-x divide-slate-100 dark:divide-slate-700 border-b border-slate-100 dark:border-slate-700">
        
        {/* Metric A: Prediction + Floor + Avg */}
        <div className={`${mini ? 'p-3' : 'p-6'} text-center`}>
          <div className="flex items-center justify-center gap-2 mb-1 text-slate-400 dark:text-slate-500 text-xs font-bold uppercase tracking-widest">
            <TrendingUp size={14} /> Proj
          </div>
          <div className={`${mini ? 'text-2xl' : 'text-4xl'} font-black text-slate-800 dark:text-slate-100`}>{data.stats.projected.toFixed(1)}</div>
          
          <div className="flex flex-wrap justify-center gap-1 mt-1">
              <div className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-700 text-slate-500 dark:text-slate-300 border border-slate-200 dark:border-slate-600">
                Floor: {data.stats.floor.toFixed(0)}
              </div>
              <div className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 border border-blue-100 dark:border-blue-800">
                Avg: {data.stats.average.toFixed(1)}
              </div>
          </div>
        </div>

        {/* Metric B: OVER/UNDER (Updated) */}
        <div className={`${mini ? 'p-3' : 'p-6'} text-center bg-slate-50/50 dark:bg-slate-800/50`}>
          <div className="flex items-center justify-center gap-2 mb-1 text-slate-400 dark:text-slate-500 text-xs font-bold uppercase tracking-widest">
            <Target size={14} /> O/U
          </div>
          <div className={`mt-2 ${mini ? 'text-2xl' : 'text-4xl'} font-black text-slate-800 dark:text-slate-100`}>
            {formatOverUnder(data.spread)}
          </div>
          <div className="text-[10px] text-slate-400 dark:text-slate-500 font-bold mt-1">
            {isSpreadData ? 'TOTAL POINTS' : 'NO LINE'}
          </div>
        </div>

        {/* Metric C: Snap Volume */}
        <div className={`${mini ? 'p-3' : 'p-6'} text-center`}>
          <div className="flex items-center justify-center gap-2 mb-1 text-slate-400 dark:text-slate-500 text-xs font-bold uppercase tracking-widest">
            <Clock size={14} /> Usage
          </div>
          <div className={`${mini ? 'text-2xl' : 'text-4xl'} font-black text-slate-800 dark:text-slate-100`}>{data.stats.snap_count}</div>
          <div className="text-[10px] text-slate-400 dark:text-slate-500 mt-1 font-bold">
            {(data.stats.snap_percentage * 100).toFixed(0)}% Share
          </div>
        </div>
      </div>
    </div>
  );
}