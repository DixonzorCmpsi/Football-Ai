import { useState } from 'react'; // internal state removed for players
import { Search, X, Swords } from 'lucide-react';
import { usePlayerSearch, useBroadcastCard } from '../hooks/useNflData';
import { getTeamColor } from '../utils/nflColors';
import BroadcastCard from './BroadcastCard';
import ComparisonHistory from './ComparisonHistory';

// Define the Props interface to accept state from parent
interface CompareViewProps {
  onViewHistory: (playerId: string) => void;
  p1: string | null;
  p2: string | null;
  setP1: (name: string | null) => void;
  setP2: (name: string | null) => void;
}

// --- SUB-COMPONENT: Search Input (Unchanged) ---
const PlayerSelector = ({ onSelect, label }: { onSelect: (name: string) => void, label: string }) => {
  const [query, setQuery] = useState("");
  const results = usePlayerSearch(query);

  return (
    <div className="relative w-full h-full min-h-[280px] flex flex-col items-center justify-center border-2 border-dashed border-slate-200 dark:border-slate-700 rounded-xl bg-slate-50/50 dark:bg-slate-800/50 p-6 transition-all">
      <span className="text-xs font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4">{label}</span>
      
      <div className="relative w-full max-w-xs">
        <Search className="absolute left-3 top-3.5 h-4 w-4 text-slate-400 dark:text-slate-500" />
        <input
          type="text"
          className="w-full pl-9 pr-4 py-3 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-xl text-sm font-bold placeholder-slate-400 dark:placeholder-slate-500 text-slate-900 dark:text-slate-100 focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400 focus:border-transparent shadow-sm"
          placeholder="Search Player..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        
        {query.length > 1 && results.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-2 bg-white dark:bg-slate-700 rounded-xl shadow-xl border border-slate-100 dark:border-slate-600 z-50 max-h-60 overflow-y-auto">
            {results.map((p) => (
              <button
                key={p.player_id}
                onClick={() => { onSelect(p.player_name); setQuery(""); }}
                className="w-full text-left px-4 py-3 hover:bg-slate-50 dark:hover:bg-slate-600 border-b border-slate-50 dark:border-slate-600 last:border-0"
              >
                <div className="font-bold text-slate-800 dark:text-slate-100 text-sm">{p.player_name}</div>
                <div className="text-[10px] text-slate-400 dark:text-slate-500 uppercase">{p.position} • {p.team}</div>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default function CompareView({ onViewHistory, p1, p2, setP1, setP2 }: CompareViewProps) {
  // REMOVED: Internal useState for players. Now using props p1/p2.

  const { cardData: data1, loadingCard: loading1 } = useBroadcastCard(p1 || "");
  const { cardData: data2, loadingCard: loading2 } = useBroadcastCard(p2 || "");

  // Helper to render the card content
  const renderPlayerContent = (
    name: string | null, 
    setName: (n: string | null) => void, 
    data: any, 
    loading: boolean,
    label: string
  ) => {
    if (!name) return <PlayerSelector onSelect={setName} label={label} />;

    if (loading) {
        return (
            <div className="h-full min-h-[280px] w-full bg-slate-100 dark:bg-slate-800 rounded-xl animate-pulse border border-slate-200 dark:border-slate-700 flex items-center justify-center">
                <span className="text-xs font-bold text-slate-400 dark:text-slate-500">SCOUTING...</span>
            </div>
        );
    }

    if (!data) return <div className="text-red-500">Error loading player.</div>;

    return (
        <div className="relative group animate-in slide-in-from-bottom-4 duration-500 h-full cursor-pointer">
            <button 
                onClick={(e) => {
                    e.stopPropagation(); 
                    setName(null);
                }}
                className="absolute -top-3 -right-3 bg-red-500 text-white p-1.5 rounded-full z-20 shadow-md opacity-0 group-hover:opacity-100 transition-opacity"
            >
                <X size={14} />
            </button>
            
            {/* CLICKING CARD TRIGGERS HISTORY */}
            <BroadcastCard 
                data={data} 
                mini={true} 
                onClick={() => onViewHistory(data.id)} 
            /> 
        </div>
    );
  };

  return (
    <div className="h-full flex flex-col px-4 pt-6 max-w-6xl mx-auto text-slate-900 dark:text-slate-100 pb-20">
      
      {/* HEADER */}
      <div className="text-center mb-8">
        <h2 className="text-2xl font-black flex items-center justify-center gap-2">
          <Swords className="text-blue-600 dark:text-blue-400" /> HEAD TO HEAD
        </h2>
        <p className="text-slate-500 dark:text-slate-400 text-sm">Compare projections and market sentiment</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-[1fr_auto_1fr] gap-6 items-stretch">
        <div className="flex flex-col gap-4 relative flex-1">
             {renderPlayerContent(p1, setP1, data1, loading1, "Challenger 1")}
        </div>

        <div className="flex flex-col items-center justify-center pt-0 lg:pt-0">
          <div className="w-12 h-12 rounded-full bg-slate-200 dark:bg-slate-600 flex items-center justify-center text-slate-500 dark:text-slate-300 font-black text-xs border-4 border-white dark:border-slate-950 shadow-sm z-10">
            VS
          </div>
        </div>

        <div className="flex flex-col gap-4 relative flex-1">
             {renderPlayerContent(p2, setP2, data2, loading2, "Challenger 2")}
        </div>
      </div>

      {/* --- COMPARISON GRAPH --- */}
      {data1 && data2 && !loading1 && !loading2 && (
          <ComparisonHistory 
            p1Id={data1.id} 
            p2Id={data2.id} 
            p1Name={data1.name} 
            p2Name={data2.name} 
            p1Color={getTeamColor(data1.team)} 
            p2Color={getTeamColor(data2.team)} 
            p1OU={Number(data1.spread) || 40} 
            p2OU={Number(data2.spread) || 40}
            p1Proj={data1.stats.projected}
            p2Proj={data2.stats.projected}
          />
      )}

    </div>
  );
}