import { useState } from 'react';
import { Search } from 'lucide-react'; 
import { usePlayerSearch, useBroadcastCard } from '../hooks/useNflData';
// FIX: Import the global BroadcastCard that uses team colors
import BroadcastCard from './BroadcastCard';

// --- Types ---
interface Props {
  onViewHistory?: (playerId: string) => void;
}

// --- Wrapper Component to Fetch & Render ---
// This replaces the old local BroadcastCard definition
const LookupCardWrapper = ({ playerName, onClick }: { playerName: string, onClick?: (id: string) => void }) => {
  const { cardData, loadingCard } = useBroadcastCard(playerName);

  if (loadingCard) return <div className="h-96 flex items-center justify-center text-slate-400 dark:text-slate-500 animate-pulse font-bold">Running ML Prediction Model...</div>;
  if (!cardData) return <div className="text-center text-slate-400 dark:text-slate-500 p-10">Player data not found.</div>;

  return (
    <div className="animate-in fade-in slide-in-from-bottom-8 duration-500">
        {/* Uses the shared component which handles team colors and O/U formatting */}
        <BroadcastCard 
            data={cardData} 
            onClick={onClick} 
            mini={false} 
        />
    </div>
  );
};

// --- Main Component ---
export default function PlayerLookupView({ onViewHistory }: Props) {
  const [query, setQuery] = useState("");
  const [selectedName, setSelectedName] = useState<string | null>(null);
  const searchResults = usePlayerSearch(query);

  return (
    <div className="max-w-4xl mx-auto pt-8 px-4 text-slate-900 dark:text-slate-100">
      <div className="relative mb-8 max-w-xl mx-auto">
        <div className="absolute inset-y-0 left-3 flex items-center pointer-events-none">
          <Search className="h-5 w-5 text-slate-400 dark:text-slate-500" />
        </div>
        <input
          type="text"
          className="block w-full pl-10 pr-3 py-4 border-2 border-slate-200 dark:border-slate-700 rounded-xl leading-5 bg-white dark:bg-slate-800 placeholder-slate-400 dark:placeholder-slate-500 focus:outline-none focus:border-blue-500 dark:focus:border-blue-400 focus:ring-4 focus:ring-blue-50 dark:focus:ring-blue-900/30 transition-all font-bold text-lg"
          placeholder="Search player name (e.g. Mahomes)..."
          value={query}
          onChange={(e) => { setQuery(e.target.value); setSelectedName(null); }}
        />
        
        {/* Type-Ahead Dropdown */}
        {query.length > 1 && !selectedName && searchResults.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-2 bg-white dark:bg-slate-700 rounded-xl shadow-2xl border border-slate-100 dark:border-slate-600 z-50 overflow-hidden">
            {searchResults.map((p) => (
              <div
                key={p.player_id}
                onClick={() => {
                   setQuery(p.player_name);
                   setSelectedName(p.player_name); 
                }}
                className="px-4 py-3 hover:bg-slate-50 dark:hover:bg-slate-600 cursor-pointer border-b border-slate-50 dark:border-slate-600 last:border-0 flex items-center justify-between group"
              >
                <div>
                  <div className="font-bold text-slate-800 dark:text-slate-100">{p.player_name}</div>
                  <div className="text-xs text-slate-400 dark:text-slate-400">{p.position} • {p.team}</div>
                </div>
                <div className="text-xs text-blue-600 dark:text-blue-400 opacity-0 group-hover:opacity-100 font-bold transition-opacity">Select &rarr;</div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Render Broadcast Card Wrapper */}
      {selectedName ? (
        <LookupCardWrapper 
            playerName={selectedName} 
            onClick={onViewHistory} 
        />
      ) : (
        <div className="text-center text-slate-300 dark:text-slate-600 mt-20">
            <Search className="w-16 h-16 mx-auto mb-4 opacity-20" />
            <p>Search for a player to generate their Broadcast Card</p>
        </div>
      )}
    </div>
  );
}