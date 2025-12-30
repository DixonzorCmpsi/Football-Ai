import React, { useState, useEffect } from 'react';
import { Search } from 'lucide-react';
import { useBroadcastCard } from '../hooks/useNflData';
import PlayerCard from './PlayerCard'; // Using the consistent PlayerCard
import { getTeamColor } from '../utils/nflColors'; // Import color utility
import type { PlayerData } from '../types';

interface PlayerLookupProps {
  onViewHistory: (playerId: string) => void;
  compareList: string[]; 
  onToggleCompare: (id: string) => void; 
}

const PlayerLookupView: React.FC<PlayerLookupProps> = ({ 
  onViewHistory, 
  compareList, 
  onToggleCompare 
}) => {
  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [results, setResults] = useState<any[]>([]); 
  
  const [targetPlayer, setTargetPlayer] = useState<string | null>(null);

  const { cardData, loadingCard } = useBroadcastCard(targetPlayer);

  // Debounce
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQuery(query), 500);
    return () => clearTimeout(timer);
  }, [query]);

  // Search API
  useEffect(() => {
    if (!debouncedQuery || debouncedQuery.length < 2) {
      setResults([]);
      return;
    }
    // Don't search if we already selected this player
    if (debouncedQuery !== targetPlayer) {
        fetch(`http://127.0.0.1:8000/players/search?q=${debouncedQuery}`)
        .then(res => res.json())
        .then(setResults)
        .catch(console.error);
    }
  }, [debouncedQuery, targetPlayer]);

  const handleSelect = (player: any) => {
    setQuery(player.player_name); 
    setResults([]); // Clear results immediately
    setTargetPlayer(player.player_name); // Set target
  };

  const isSelected = cardData ? compareList.includes(cardData.id) : false;

  // --- MAP HOOK DATA TO CARD DATA ---
  const playerData: PlayerData | null = cardData ? {
      player_id: cardData.id,
      player_name: cardData.name,
      position: cardData.position as any,
      team: cardData.team,
      opponent: cardData.opponent,
      image: cardData.image,
      week: 18, // Default context
      prediction: cardData.stats.projected,
      floor_prediction: cardData.stats.floor,
      average_points: cardData.stats.average,
      injury_status: cardData.injury_status,
      is_injury_boosted: cardData.is_injury_boosted,
      // Props
      overunder: null,
      spread: cardData.spread,
      prop_line: cardData.props?.[0]?.line || null,
      prop_prob: cardData.props?.[0]?.implied_prob || null,
      pass_td_line: cardData.pass_td_line || null,
      pass_td_prob: cardData.pass_td_prob || null,
      anytime_td_prob: cardData.anytime_td_prob || null
  } : null;

  return (
    <div className="h-full flex flex-col items-center pt-8 px-4 max-w-4xl mx-auto">
      
      {/* --- SEARCH BAR --- */}
      <div className="w-full max-w-lg relative mb-10 z-50">
        <div className="relative group">
          <div className="absolute -inset-1 bg-gradient-to-r from-blue-500 to-purple-600 rounded-2xl opacity-20 group-hover:opacity-40 transition duration-500 blur-md"></div>
          <div className="relative flex items-center bg-white dark:bg-slate-900 rounded-2xl shadow-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
            <Search className="ml-4 text-slate-400" size={20} />
            <input 
              type="text" 
              className="w-full py-4 px-4 bg-transparent outline-none text-slate-800 dark:text-slate-100 font-bold placeholder-slate-400 dark:placeholder-slate-500 text-lg"
              placeholder="Search Player..." 
              value={query}
              onChange={(e) => { 
                  setQuery(e.target.value); 
                  if (e.target.value === "") setTargetPlayer(null);
                  if (targetPlayer && e.target.value !== targetPlayer) setTargetPlayer(null);
              }}
            />
            {loadingCard && <div className="mr-4 w-5 h-5 border-2 border-blue-500 border-t-transparent rounded-full animate-spin"></div>}
          </div>
        </div>

        {/* DROPDOWN RESULTS */}
        {results.length > 0 && !targetPlayer && (
          <div className="absolute top-full left-0 right-0 mt-2 bg-white dark:bg-slate-800 rounded-xl shadow-2xl border border-slate-100 dark:border-slate-700 overflow-hidden z-50 animate-in fade-in slide-in-from-top-2 duration-200">
            {results.map((p: any) => (
              <div 
                key={p.player_id}
                onClick={() => handleSelect(p)}
                className="px-5 py-3 hover:bg-slate-50 dark:hover:bg-slate-700 cursor-pointer border-b border-slate-50 dark:border-slate-700 last:border-0 flex items-center justify-between group"
              >
                <div>
                  <div className="font-bold text-slate-800 dark:text-slate-200 text-sm group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">{p.player_name}</div>
                  <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">{p.position} • {p.team_abbr || p.team}</div>
                </div>
                {p.headshot && <img src={p.headshot} alt="headshot" className="w-8 h-8 rounded-full bg-slate-100 dark:bg-slate-600" />}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* --- RESULT CARD AREA --- */}
      <div className="w-full flex-1 flex flex-col items-center justify-start pb-20">
        {playerData ? (
          <div className="animate-in slide-in-from-bottom-8 duration-700 w-full max-w-md relative">
            <PlayerCard 
              data={playerData}
              // FIX: Explicitly pass the team color here
              teamColor={getTeamColor(playerData.team)}
              onClick={() => onViewHistory(playerData.player_id)}
              isSelected={isSelected}
              onToggleCompare={onToggleCompare}
            />
          </div>
        ) : (
          !loadingCard && (
            <div className="text-center mt-10 opacity-30">
               <div className="text-9xl font-black text-slate-200 dark:text-slate-800 select-none">NFL</div>
               <p className="text-slate-400 font-bold mt-4">Search for a player to analyze</p>
            </div>
          )
        )}
      </div>

    </div>
  );
};

export default PlayerLookupView;