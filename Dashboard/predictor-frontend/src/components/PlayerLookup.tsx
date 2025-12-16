import { useState } from 'react';
import { Search, Target, TrendingUp, Clock, MousePointerClick } from 'lucide-react'; 
import { usePlayerSearch, useBroadcastCard } from '../hooks/useNflData';

// --- Types ---
interface Props {
  onViewHistory?: (playerId: string) => void;
}

// --- Sub-Component: The Clickable Card ---
const BroadcastCard = ({ playerName, onClick }: { playerName: string, onClick?: (id: string) => void }) => {
  const { cardData, loadingCard } = useBroadcastCard(playerName);

  if (loadingCard) return <div className="h-96 flex items-center justify-center text-slate-400 animate-pulse font-bold">Running ML Prediction Model...</div>;
  if (!cardData) return <div className="text-center text-slate-400 p-10">Player data not found.</div>;

  return (
    <div 
      // CLICK HANDLER WIRED HERE
      onClick={() => onClick && onClick(cardData.id)}
      className="bg-white rounded-2xl shadow-xl overflow-hidden border border-slate-200 max-w-2xl mx-auto mt-8 cursor-pointer hover:ring-4 hover:ring-blue-50 transition-all group relative"
    >
      {/* Hover Overlay Hint */}
      <div className="absolute top-4 right-4 bg-white/20 backdrop-blur text-white px-3 py-1 rounded-full text-xs font-bold opacity-0 group-hover:opacity-100 transition-opacity flex items-center gap-2 z-20">
        <MousePointerClick size={14} /> View History
      </div>

      {/* 1. HERO HEADER */}
      <div className="bg-slate-900 text-white p-6 relative overflow-hidden">
        <div className="absolute top-0 right-0 w-64 h-64 bg-blue-600 rounded-full blur-[80px] opacity-20 -translate-y-1/2 translate-x-1/2"></div>
        
        <div className="flex items-center gap-6 relative z-10">
          <div className="w-24 h-24 rounded-full border-4 border-white/20 overflow-hidden bg-slate-800 shadow-lg shrink-0">
            <img src={cardData.image} alt={cardData.name} className="w-full h-full object-cover" />
          </div>
          <div>
            <h2 className="text-3xl font-black tracking-tighter uppercase">{cardData.name}</h2>
            <div className="flex items-center gap-3 mt-1">
              <span className="px-2 py-0.5 bg-blue-600 rounded text-xs font-bold">{cardData.position}</span>
              <span className="text-slate-300 font-bold tracking-widest">{cardData.team}</span>
              <span className="text-slate-400 text-xs ml-2 border-l border-slate-600 pl-3">{cardData.draft}</span>
            </div>
          </div>
        </div>
      </div>

      {/* 2. KEY METRICS GRID */}
      <div className="grid grid-cols-3 divide-x divide-slate-100 border-b border-slate-100">
        
        {/* Metric A: Prediction */}
        <div className="p-6 text-center">
          <div className="flex items-center justify-center gap-2 mb-1 text-slate-400 text-xs font-bold uppercase tracking-widest">
            <TrendingUp size={14} /> Projection
          </div>
          <div className="text-4xl font-black text-slate-800">{cardData.stats.projected.toFixed(1)}</div>
          <div className="text-xs text-blue-600 font-bold mt-1 bg-blue-50 px-2 py-1 rounded-full inline-block">
             Floor: {cardData.stats.floor.toFixed(1)}
          </div>
        </div>

        {/* Metric B: Vegas Odds */}
        <div className="p-6 text-center bg-slate-50/50">
          <div className="flex items-center justify-center gap-2 mb-1 text-slate-400 text-xs font-bold uppercase tracking-widest">
            <Target size={14} /> Vegas Odds
          </div>
          <div className="text-sm font-bold text-slate-800 mt-3 break-words leading-tight px-2">
            {cardData.stats.over_under}
          </div>
        </div>

        {/* Metric C: Snap Volume */}
        <div className="p-6 text-center">
          <div className="flex items-center justify-center gap-2 mb-1 text-slate-400 text-xs font-bold uppercase tracking-widest">
            <Clock size={14} /> Usage
          </div>
          <div className="text-4xl font-black text-slate-800">{cardData.stats.snap_count}</div>
          <div className="text-xs text-slate-400 mt-1 font-bold">
            {(cardData.stats.snap_percentage * 100).toFixed(0)}% Share
          </div>
        </div>
      </div>
      
      {/* Footer / Context */}
      <div className="p-3 bg-slate-50 text-center text-[10px] text-slate-400 font-medium uppercase tracking-widest">
        Click card for full season history & stats
      </div>
    </div>
  );
};

// --- Main Component ---
export default function PlayerLookupView({ onViewHistory }: Props) {
  const [query, setQuery] = useState("");
  const [selectedName, setSelectedName] = useState<string | null>(null);
  const searchResults = usePlayerSearch(query);

  return (
    <div className="max-w-4xl mx-auto pt-8 px-4">
      <div className="relative mb-8 max-w-xl mx-auto">
        <div className="absolute inset-y-0 left-3 flex items-center pointer-events-none">
          <Search className="h-5 w-5 text-slate-400" />
        </div>
        <input
          type="text"
          className="block w-full pl-10 pr-3 py-4 border-2 border-slate-200 rounded-xl leading-5 bg-white placeholder-slate-400 focus:outline-none focus:border-blue-500 focus:ring-4 focus:ring-blue-50 transition-all font-bold text-lg"
          placeholder="Search player name (e.g. Mahomes)..."
          value={query}
          onChange={(e) => { setQuery(e.target.value); setSelectedName(null); }}
        />
        
        {/* Type-Ahead Dropdown */}
        {query.length > 1 && !selectedName && searchResults.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-2xl border border-slate-100 z-50 overflow-hidden">
            {searchResults.map((p) => (
              <div
                key={p.player_id}
                onClick={() => {
                   setQuery(p.player_name);
                   setSelectedName(p.player_name); 
                }}
                className="px-4 py-3 hover:bg-slate-50 cursor-pointer border-b border-slate-50 last:border-0 flex items-center justify-between group"
              >
                <div>
                    <div className="font-bold text-slate-800">{p.player_name}</div>
                    <div className="text-xs text-slate-400">{p.position} • {p.team}</div>
                </div>
                <div className="text-xs text-blue-600 opacity-0 group-hover:opacity-100 font-bold transition-opacity">Select &rarr;</div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Render Broadcast Card with Interaction */}
      {selectedName ? (
        <div className="animate-in fade-in slide-in-from-bottom-8 duration-500">
            <BroadcastCard 
                playerName={selectedName} 
                onClick={onViewHistory} // PASS IT DOWN
            />
        </div>
      ) : (
        <div className="text-center text-slate-300 mt-20">
            <Search className="w-16 h-16 mx-auto mb-4 opacity-20" />
            <p>Search for a player to generate their Broadcast Card</p>
        </div>
      )}
    </div>
  );
}