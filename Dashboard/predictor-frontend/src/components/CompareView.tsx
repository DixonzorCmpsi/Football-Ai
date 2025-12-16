import { useState } from 'react';
import { Search, X, Swords } from 'lucide-react';
import { usePlayerSearch, useBroadcastCard } from '../hooks/useNflData';

// --- SUB-COMPONENT: Slimmer Broadcast Card for Split View ---
const MiniBroadcastCard = ({ playerName, onDelete }: { playerName: string, onDelete: () => void }) => {
  const { cardData, loadingCard } = useBroadcastCard(playerName);

  if (loadingCard) return <div className="h-64 flex items-center justify-center text-slate-400 animate-pulse bg-slate-50 rounded-xl border border-slate-200">Loading...</div>;
  if (!cardData) return null;

  return (
    <div className="bg-white rounded-xl shadow-lg border border-slate-200 overflow-hidden relative group">
      <button 
        onClick={onDelete}
        className="absolute top-2 right-2 bg-white/80 hover:bg-red-500 hover:text-white text-slate-400 p-1 rounded-full z-20 transition-all opacity-0 group-hover:opacity-100"
      >
        <X size={16} />
      </button>

      {/* Hero Section */}
      <div className="bg-slate-900 text-white p-4 relative overflow-hidden">
        <div className="absolute top-0 right-0 w-32 h-32 bg-blue-600 rounded-full blur-[50px] opacity-20 -translate-y-1/2 translate-x-1/2"></div>
        <div className="flex items-center gap-4 relative z-10">
          <div className="w-16 h-16 rounded-full border-2 border-white/20 overflow-hidden bg-slate-800">
            <img src={cardData.image} alt={cardData.name} className="w-full h-full object-cover" />
          </div>
          <div>
            <h3 className="text-xl font-black tracking-tighter uppercase leading-none">{cardData.name}</h3>
            <p className="text-slate-400 text-xs font-bold mt-1">{cardData.position} • {cardData.team}</p>
          </div>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-2 divide-x divide-slate-100 border-b border-slate-100">
        <div className="p-4 text-center">
          <div className="text-[10px] text-slate-400 font-bold uppercase tracking-widest mb-1">Projection</div>
          <div className="text-3xl font-black text-slate-800">{cardData.stats.projected.toFixed(1)}</div>
          <div className="text-[10px] font-bold text-blue-600 bg-blue-50 rounded-full px-2 py-0.5 inline-block mt-1">
            Floor: {cardData.stats.floor.toFixed(1)}
          </div>
        </div>
        <div className="p-4 text-center bg-slate-50/50">
          <div className="text-[10px] text-slate-400 font-bold uppercase tracking-widest mb-1">Vegas</div>
          <div className="text-xs font-bold text-slate-800 mt-2 leading-tight">
            {cardData.stats.over_under}
          </div>
        </div>
      </div>

      {/* Snap Context */}
      <div className="p-4 bg-slate-50 flex justify-between items-center">
        <span className="text-xs font-bold text-slate-400 uppercase">Usage Rate</span>
        <span className="text-lg font-black text-slate-700">
            {(cardData.stats.snap_percentage * 100).toFixed(0)}%
        </span>
      </div>
    </div>
  );
};

// --- SUB-COMPONENT: Search Input ---
const PlayerSelector = ({ onSelect }: { onSelect: (name: string) => void }) => {
  const [query, setQuery] = useState("");
  const results = usePlayerSearch(query);

  return (
    <div className="relative w-full">
      <div className="relative">
        <Search className="absolute left-3 top-3.5 h-4 w-4 text-slate-400" />
        <input
          type="text"
          className="w-full pl-9 pr-4 py-3 bg-white border border-slate-200 rounded-xl text-sm font-bold placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent shadow-sm"
          placeholder="Add Player..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
      </div>
      {query.length > 1 && results.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-xl border border-slate-100 z-50 max-h-60 overflow-y-auto">
          {results.map((p) => (
            <button
              key={p.player_id}
              onClick={() => { onSelect(p.player_name); setQuery(""); }}
              className="w-full text-left px-4 py-3 hover:bg-slate-50 border-b border-slate-50 last:border-0"
            >
              <div className="font-bold text-slate-800 text-sm">{p.player_name}</div>
              <div className="text-[10px] text-slate-400 uppercase">{p.position} • {p.team}</div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
};

export default function CompareView() {
  const [player1Name, setPlayer1Name] = useState<string | null>(null);
  const [player2Name, setPlayer2Name] = useState<string | null>(null);

  return (
    <div className="h-full flex flex-col px-4 pt-6 max-w-5xl mx-auto">
      <div className="text-center mb-8">
        <h2 className="text-2xl font-black text-slate-900 flex items-center justify-center gap-2">
          <Swords className="text-blue-600" /> HEAD TO HEAD
        </h2>
        <p className="text-slate-500 text-sm">Compare projections and market sentiment</p>
      </div>

      <div className="flex-1 grid grid-cols-[1fr_auto_1fr] gap-6 items-start">
        
        {/* LEFT CORNER */}
        <div className="flex flex-col gap-4">
          {!player1Name ? (
            <div className="h-64 border-2 border-dashed border-slate-200 rounded-xl flex flex-col items-center justify-center p-6 bg-slate-50/50">
              <span className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-4">Challenger 1</span>
              <PlayerSelector onSelect={setPlayer1Name} />
            </div>
          ) : (
            <MiniBroadcastCard playerName={player1Name} onDelete={() => setPlayer1Name(null)} />
          )}
        </div>

        {/* VS BADGE */}
        <div className="flex flex-col items-center justify-center pt-24">
          <div className="w-12 h-12 rounded-full bg-slate-200 flex items-center justify-center text-slate-500 font-black text-xs border-4 border-white shadow-sm">
            VS
          </div>
        </div>

        {/* RIGHT CORNER */}
        <div className="flex flex-col gap-4">
          {!player2Name ? (
            <div className="h-64 border-2 border-dashed border-slate-200 rounded-xl flex flex-col items-center justify-center p-6 bg-slate-50/50">
              <span className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-4">Challenger 2</span>
              <PlayerSelector onSelect={setPlayer2Name} />
            </div>
          ) : (
            <MiniBroadcastCard playerName={player2Name} onDelete={() => setPlayer2Name(null)} />
          )}
        </div>
      </div>
    </div>
  );
}