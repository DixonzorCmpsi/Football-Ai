import { usePicks } from '../contexts/PicksContext';
import { Trash2 } from 'lucide-react';

const MyPicksList = () => {
  const { picks, removePick } = usePicks();

  if (picks.length === 0) {
    return <div className="text-center text-slate-400 text-xs mt-10">No picks saved yet.</div>;
  }

  return (
    <div className="space-y-2">
      {picks.map(pick => (
        <div key={pick.id} className="bg-white dark:bg-slate-700/50 p-3 rounded-lg border border-slate-100 dark:border-slate-700 shadow-sm flex justify-between items-center group">
          <div>
            <div className="font-bold text-slate-800 dark:text-slate-200 text-sm">{pick.playerName}</div>
            <div className="text-[10px] text-slate-500 dark:text-slate-400 font-mono">
              {pick.propType} <span className={`font-black ${pick.choice === 'OVER' ? 'text-green-500' : 'text-red-500'}`}>{pick.choice}</span> {pick.line}
            </div>
            <div className="text-[9px] text-slate-400">Wk {pick.week}</div>
          </div>
          <button onClick={() => removePick(pick.id)} className="text-slate-400 hover:text-red-500 transition-colors opacity-0 group-hover:opacity-100">
            <Trash2 size={14} />
          </button>
        </div>
      ))}
    </div>
  );
};

export default MyPicksList;
