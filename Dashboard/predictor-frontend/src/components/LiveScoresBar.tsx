import React, { useState, useEffect, useCallback } from 'react';
import { Radio, RefreshCw, ChevronLeft, ChevronRight, Circle } from 'lucide-react';
import { getTeamColor } from '../utils/nflColors';

interface LiveGame {
  home_team: string;
  away_team: string;
  home_score: number | null;
  away_score: number | null;
  status: string;
  status_detail: string;
  game_date: string;
  venue?: string;
}

interface LiveScoresBarProps {
  week: number;
  onGameClick?: (homeTeam: string, awayTeam: string) => void;
}

// Polling intervals
const LIVE_POLL_INTERVAL = 30000;  // 30 seconds during live games
const IDLE_POLL_INTERVAL = 300000; // 5 minutes when no live games

const API_BASE_URL = (typeof window !== 'undefined' && window.__env?.API_BASE_URL) || '/api';

const LiveScoresBar: React.FC<LiveScoresBarProps> = ({ week, onGameClick }) => {
  const [games, setGames] = useState<LiveGame[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [scrollIndex, setScrollIndex] = useState(0);
  const [isRefreshing, setIsRefreshing] = useState(false);

  const fetchScores = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE_URL}/live-scores/${week}`);
      if (!res.ok) throw new Error('Failed to fetch');
      const data = await res.json();
      setGames(data.games || []);
      setLastUpdated(new Date());
      setError(null);
    } catch (err) {
      setError('Could not load scores');
      console.error('Live scores error:', err);
    } finally {
      setLoading(false);
      setIsRefreshing(false);
    }
  }, [week]);

  // Initial fetch
  useEffect(() => {
    setLoading(true);
    fetchScores();
  }, [fetchScores]);

  // Smart polling - faster when games are live
  useEffect(() => {
    const hasLiveGames = games.some(g => g.status === 'STATUS_IN_PROGRESS');
    const interval = hasLiveGames ? LIVE_POLL_INTERVAL : IDLE_POLL_INTERVAL;
    
    const timer = setInterval(fetchScores, interval);
    return () => clearInterval(timer);
  }, [games, fetchScores]);

  const handleRefresh = () => {
    setIsRefreshing(true);
    fetchScores();
  };

  const getStatusDisplay = (game: LiveGame) => {
    if (game.status === 'STATUS_IN_PROGRESS') {
      return { label: game.status_detail || 'LIVE', isLive: true };
    }
    if (game.status === 'STATUS_FINAL') {
      return { label: 'FINAL', isLive: false };
    }
    // Scheduled
    const date = new Date(game.game_date);
    const day = date.toLocaleDateString('en-US', { weekday: 'short' });
    const time = date.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
    return { label: `${day} ${time}`, isLive: false };
  };

  const visibleGames = games.slice(scrollIndex, scrollIndex + 4);
  const canScrollLeft = scrollIndex > 0;
  const canScrollRight = scrollIndex + 4 < games.length;

  if (loading && games.length === 0) {
    return (
      <div className="bg-slate-900 text-white px-4 py-2 flex items-center justify-center gap-2">
        <div className="w-4 h-4 border-2 border-blue-400 border-t-transparent rounded-full animate-spin"></div>
        <span className="text-xs text-slate-400">Loading live scores...</span>
      </div>
    );
  }

  if (error && games.length === 0) {
    return (
      <div className="bg-slate-900 text-white px-4 py-2 flex items-center justify-center gap-2">
        <span className="text-xs text-red-400">{error}</span>
        <button onClick={handleRefresh} className="p-1 hover:bg-slate-800 rounded">
          <RefreshCw size={12} className="text-slate-400" />
        </button>
      </div>
    );
  }

  const hasLiveGames = games.some(g => g.status === 'STATUS_IN_PROGRESS');

  return (
    <div className="bg-gradient-to-r from-slate-900 via-slate-800 to-slate-900 text-white border-b border-slate-700/50">
      <div className="flex items-center gap-2 px-3 py-1.5">
        {/* Live indicator */}
        <div className="flex items-center gap-1.5 pr-3 border-r border-slate-700">
          {hasLiveGames ? (
            <>
              <Circle size={8} className="text-red-500 fill-red-500 animate-pulse" />
              <span className="text-[10px] font-bold uppercase tracking-wider text-red-400">LIVE</span>
            </>
          ) : (
            <>
              <Radio size={12} className="text-slate-500" />
              <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500">Wk {week}</span>
            </>
          )}
        </div>

        {/* Scroll left */}
        <button 
          onClick={() => setScrollIndex(Math.max(0, scrollIndex - 1))}
          disabled={!canScrollLeft}
          className={`p-1 rounded transition-colors ${canScrollLeft ? 'hover:bg-slate-700 text-slate-300' : 'text-slate-700 cursor-not-allowed'}`}
        >
          <ChevronLeft size={16} />
        </button>

        {/* Games ticker */}
        <div className="flex-1 flex items-center gap-3 overflow-hidden">
          {visibleGames.map((game, idx) => {
            const status = getStatusDisplay(game);
            const homeColor = getTeamColor(game.home_team);
            const awayColor = getTeamColor(game.away_team);
            const homeWinning = (game.home_score ?? 0) > (game.away_score ?? 0);
            const awayWinning = (game.away_score ?? 0) > (game.home_score ?? 0);

            return (
              <button
                key={`${game.home_team}-${game.away_team}-${idx}`}
                onClick={() => onGameClick?.(game.home_team, game.away_team)}
                className="flex items-center gap-2 px-3 py-1 rounded-lg bg-slate-800/50 hover:bg-slate-700/50 transition-all border border-slate-700/50 hover:border-slate-600 min-w-[140px]"
              >
                {/* Away Team */}
                <div className="flex items-center gap-1.5">
                  <div 
                    className="w-1.5 h-4 rounded-full" 
                    style={{ backgroundColor: awayColor }}
                  />
                  <span className={`text-xs font-bold ${awayWinning ? 'text-white' : 'text-slate-400'}`}>
                    {game.away_team}
                  </span>
                  {game.away_score !== null && (
                    <span className={`text-sm font-black tabular-nums ${awayWinning ? 'text-white' : 'text-slate-500'}`}>
                      {game.away_score}
                    </span>
                  )}
                </div>

                {/* @ symbol */}
                <span className="text-slate-600 text-[10px]">@</span>

                {/* Home Team */}
                <div className="flex items-center gap-1.5">
                  <div 
                    className="w-1.5 h-4 rounded-full" 
                    style={{ backgroundColor: homeColor }}
                  />
                  <span className={`text-xs font-bold ${homeWinning ? 'text-white' : 'text-slate-400'}`}>
                    {game.home_team}
                  </span>
                  {game.home_score !== null && (
                    <span className={`text-sm font-black tabular-nums ${homeWinning ? 'text-white' : 'text-slate-500'}`}>
                      {game.home_score}
                    </span>
                  )}
                </div>

                {/* Status */}
                <div className={`ml-auto text-[9px] font-bold uppercase px-1.5 py-0.5 rounded ${
                  status.isLive 
                    ? 'bg-red-500/20 text-red-400 animate-pulse' 
                    : game.status === 'STATUS_FINAL'
                      ? 'bg-slate-700 text-slate-400'
                      : 'text-slate-500'
                }`}>
                  {status.label}
                </div>
              </button>
            );
          })}

          {games.length === 0 && (
            <span className="text-xs text-slate-500 italic">No games scheduled</span>
          )}
        </div>

        {/* Scroll right */}
        <button 
          onClick={() => setScrollIndex(Math.min(games.length - 4, scrollIndex + 1))}
          disabled={!canScrollRight}
          className={`p-1 rounded transition-colors ${canScrollRight ? 'hover:bg-slate-700 text-slate-300' : 'text-slate-700 cursor-not-allowed'}`}
        >
          <ChevronRight size={16} />
        </button>

        {/* Refresh button */}
        <button 
          onClick={handleRefresh}
          disabled={isRefreshing}
          className="p-1.5 hover:bg-slate-700 rounded transition-colors border-l border-slate-700 ml-2 pl-3"
          title={lastUpdated ? `Last updated: ${lastUpdated.toLocaleTimeString()}` : 'Refresh'}
        >
          <RefreshCw 
            size={14} 
            className={`text-slate-400 ${isRefreshing ? 'animate-spin' : ''}`} 
          />
        </button>
      </div>
    </div>
  );
};

export default LiveScoresBar;
