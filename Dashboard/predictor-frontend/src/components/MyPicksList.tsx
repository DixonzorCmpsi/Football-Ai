import { useEffect } from 'react';
import { usePicks } from '../contexts/PicksContext';
import { Trash2, Check, X, Minus, Clock, RefreshCw } from 'lucide-react';

interface MyPicksListProps {
  currentWeek?: number | null;
}

const MyPicksList = ({ currentWeek }: MyPicksListProps) => {
  const { picks, removePick, refreshPickStatuses } = usePicks();
  
  const week = currentWeek ?? 19; // Default to 19 if null

  // Refresh pick statuses when component mounts or week changes
  useEffect(() => {
    refreshPickStatuses(week);
  }, [week, refreshPickStatuses]);

  const handleRefresh = () => {
    refreshPickStatuses(week);
  };

  // Status badge component
  const StatusBadge = ({ status, actual }: { status?: string; actual?: number }) => {
    if (status === 'HIT') {
      return (
        <div className="flex items-center gap-1">
          <div className="bg-green-100 dark:bg-green-900/40 text-green-600 dark:text-green-400 p-1 rounded-full">
            <Check size={12} strokeWidth={3} />
          </div>
          {actual !== undefined && (
            <span className="text-[10px] font-bold text-green-600 dark:text-green-400">{actual}</span>
          )}
        </div>
      );
    }
    if (status === 'MISS') {
      return (
        <div className="flex items-center gap-1">
          <div className="bg-red-100 dark:bg-red-900/40 text-red-600 dark:text-red-400 p-1 rounded-full">
            <X size={12} strokeWidth={3} />
          </div>
          {actual !== undefined && (
            <span className="text-[10px] font-bold text-red-600 dark:text-red-400">{actual}</span>
          )}
        </div>
      );
    }
    if (status === 'PUSH') {
      return (
        <div className="flex items-center gap-1">
          <div className="bg-yellow-100 dark:bg-yellow-900/40 text-yellow-600 dark:text-yellow-400 p-1 rounded-full">
            <Minus size={12} strokeWidth={3} />
          </div>
          {actual !== undefined && (
            <span className="text-[10px] font-bold text-yellow-600 dark:text-yellow-400">{actual}</span>
          )}
        </div>
      );
    }
    // PENDING
    return (
      <div className="bg-slate-100 dark:bg-slate-700 text-slate-400 dark:text-slate-500 p-1 rounded-full">
        <Clock size={12} />
      </div>
    );
  };

  // Calculate stats
  const completedPicks = picks.filter(p => p.status === 'HIT' || p.status === 'MISS' || p.status === 'PUSH');
  const hits = picks.filter(p => p.status === 'HIT').length;
  const misses = picks.filter(p => p.status === 'MISS').length;
  const pushes = picks.filter(p => p.status === 'PUSH').length;
  const winRate = completedPicks.length > 0 ? ((hits / (completedPicks.length - pushes)) * 100).toFixed(0) : '0';

  if (picks.length === 0) {
    return <div className="text-center text-slate-400 text-xs mt-10">No picks saved yet.</div>;
  }

  return (
    <div className="space-y-3">
      {/* Stats Header */}
      {completedPicks.length > 0 && (
        <div className="bg-slate-50 dark:bg-slate-800 p-3 rounded-lg border border-slate-200 dark:border-slate-700">
          <div className="flex justify-between items-center mb-2">
            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Record</span>
            <button 
              onClick={handleRefresh}
              className="p-1 text-slate-400 hover:text-blue-500 hover:bg-slate-100 dark:hover:bg-slate-700 rounded transition-colors"
              title="Refresh pick results"
            >
              <RefreshCw size={12} />
            </button>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-1">
              <span className="text-lg font-black text-green-600 dark:text-green-400">{hits}</span>
              <span className="text-slate-400">-</span>
              <span className="text-lg font-black text-red-600 dark:text-red-400">{misses}</span>
              {pushes > 0 && (
                <>
                  <span className="text-slate-400">-</span>
                  <span className="text-lg font-black text-yellow-600 dark:text-yellow-400">{pushes}</span>
                </>
              )}
            </div>
            <div className="text-sm font-bold text-slate-600 dark:text-slate-300">
              ({winRate}%)
            </div>
          </div>
        </div>
      )}

      {/* Picks List */}
      {picks.map(pick => (
        <div 
          key={pick.id} 
          className={`p-3 rounded-lg border shadow-sm flex justify-between items-center group transition-all ${
            pick.status === 'HIT' 
              ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800' 
              : pick.status === 'MISS'
                ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800'
                : pick.status === 'PUSH'
                  ? 'bg-yellow-50 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-800'
                  : 'bg-white dark:bg-slate-700/50 border-slate-100 dark:border-slate-700'
          }`}
        >
          <div className="flex items-center gap-3">
            <StatusBadge status={pick.status} actual={pick.actual} />
            <div>
              <div className="font-bold text-slate-800 dark:text-slate-200 text-sm">{pick.playerName}</div>
              <div className="text-[10px] text-slate-500 dark:text-slate-400 font-mono">
                {pick.propType} <span className={`font-black ${pick.choice === 'OVER' ? 'text-green-500' : 'text-red-500'}`}>{pick.choice}</span> {pick.line}
              </div>
              <div className="text-[9px] text-slate-400">Wk {pick.week}</div>
            </div>
          </div>
          <button 
            onClick={() => removePick(pick.id)} 
            className="text-slate-400 hover:text-red-500 transition-colors opacity-0 group-hover:opacity-100"
          >
            <Trash2 size={14} />
          </button>
        </div>
      ))}
    </div>
  );
};

export default MyPicksList;
