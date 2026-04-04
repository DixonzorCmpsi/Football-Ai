import React, { useEffect, useState } from 'react';
import { TrendingUp, TrendingDown, DollarSign, Target, AlertCircle } from 'lucide-react';
import { API_BASE_URL } from '../lib/api';

interface InsightsProps {
  week: number;
  home: string;
  away: string;
}

interface PropLeg {
  player_name: string;
  position: string;
  prop_type: string;
  line: number;
  odds: string;
  side: string;
  correlation_score: number;
}

interface Parlay {
  name: string;
  legs: PropLeg[];
  leg_count: number;
  combined_odds: string;
  confidence: number;
}

interface ScriptInsights {
  script_type: string;
  description: string;
  parlays: Parlay[];
  top_individual_props: PropLeg[];
}

interface InsightsData {
  status: string;
  message?: string;
  game?: {
    home_team: string;
    away_team: string;
    week: number;
    total: number;
    spread: number;
  };
  over_script?: ScriptInsights;
  under_script?: ScriptInsights;
}

const MatchupInsights: React.FC<InsightsProps> = ({ week, home, away }) => {
  const [insights, setInsights] = useState<InsightsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedScript, setSelectedScript] = useState<'over' | 'under'>('over');

  useEffect(() => {
    const fetchInsights = async () => {
      setLoading(true);
      try {
        const response = await fetch(`${API_BASE_URL}/matchup/${week}/${home}/${away}/insights`);
        const data = await response.json();
        setInsights(data);
      } catch (error) {
        console.error('Failed to fetch insights:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchInsights();
  }, [week, home, away]);

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full animate-spin"></div>
      </div>
    );
  }

  if (!insights || insights.status === 'no_lines') {
    return (
      <div className="flex flex-col items-center justify-center h-full p-8 text-center">
        <AlertCircle size={48} className="text-slate-400 dark:text-slate-600 mb-4" />
        <h3 className="text-lg font-bold text-slate-700 dark:text-slate-300 mb-2">
          No Lines Available Yet
        </h3>
        <p className="text-sm text-slate-500 dark:text-slate-400 max-w-md">
          {insights?.message || 'Betting lines have not been released for this game yet. Check back closer to game time.'}
        </p>
      </div>
    );
  }

  if (insights.status === 'error') {
    return (
      <div className="flex flex-col items-center justify-center h-full p-8 text-center">
        <AlertCircle size={48} className="text-red-400 mb-4" />
        <p className="text-sm text-slate-500 dark:text-slate-400">{insights.message}</p>
      </div>
    );
  }

  const currentScript = selectedScript === 'over' ? insights.over_script : insights.under_script;

  return (
    <div className="animate-in fade-in slide-in-from-bottom-4 duration-500 space-y-4">
      {/* Header with game info */}
      <div className="bg-gradient-to-r from-green-500 to-blue-500 dark:from-green-700 dark:to-blue-700 rounded-lg p-4 text-white">
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-lg font-black">Betting Insights</h2>
          <div className="flex items-center gap-2 text-sm">
            <span className="font-bold">O/U:</span>
            <span className="text-2xl font-black">{insights.game?.total}</span>
          </div>
        </div>
        <p className="text-xs opacity-90">
          AI-powered prop combinations based on game script analysis
        </p>
      </div>

      {/* Disclaimer */}
      <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-3">
        <p className="text-[11px] text-yellow-800 dark:text-yellow-200 leading-relaxed">
          <span className="font-bold">⚠️ For entertainment purposes only.</span> These insights are AI-generated predictions based on historical data and game context. Past performance does not guarantee future results. Please gamble responsibly.
        </p>
      </div>

      {/* Script Toggle */}
      <div className="flex gap-2 sticky top-0 bg-slate-50/95 dark:bg-slate-950/95 backdrop-blur z-20 py-2">
        <button
          onClick={() => setSelectedScript('over')}
          className={`flex-1 flex flex-col sm:flex-row items-center justify-center gap-1 sm:gap-2 px-3 sm:px-4 py-2 sm:py-3 rounded-lg font-bold transition-all ${
            selectedScript === 'over'
              ? 'bg-gradient-to-r from-orange-500 to-red-500 text-white shadow-lg scale-105'
              : 'bg-slate-200 dark:bg-slate-800 text-slate-600 dark:text-slate-400 hover:bg-slate-300 dark:hover:bg-slate-700'
          }`}
        >
          <TrendingUp size={18} />
          <div className="text-center sm:text-left">
            <div className="text-xs sm:text-sm">OVER Script</div>
            <div className="text-[10px] sm:text-xs opacity-75">High-Scoring</div>
          </div>
        </button>
        <button
          onClick={() => setSelectedScript('under')}
          className={`flex-1 flex flex-col sm:flex-row items-center justify-center gap-1 sm:gap-2 px-3 sm:px-4 py-2 sm:py-3 rounded-lg font-bold transition-all ${
            selectedScript === 'under'
              ? 'bg-gradient-to-r from-blue-500 to-purple-500 text-white shadow-lg scale-105'
              : 'bg-slate-200 dark:bg-slate-800 text-slate-600 dark:text-slate-400 hover:bg-slate-300 dark:hover:bg-slate-700'
          }`}
        >
          <TrendingDown size={18} />
          <div className="text-center sm:text-left">
            <div className="text-xs sm:text-sm">UNDER Script</div>
            <div className="text-[10px] sm:text-xs opacity-75">Low-Scoring</div>
          </div>
        </button>
      </div>

      {/* Script Description */}
      {currentScript && (
        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
          <p className="text-sm text-slate-700 dark:text-slate-300">{currentScript.description}</p>
        </div>
      )}

      {/* Recommended Parlays */}
      {currentScript && currentScript.parlays && currentScript.parlays.length > 0 && (
        <div className="space-y-3">
          <h3 className="text-lg font-black text-slate-800 dark:text-slate-100 flex items-center gap-2">
            <Target size={20} className="text-green-600" />
            Recommended Parlays
          </h3>
          {currentScript.parlays.map((parlay, idx) => (
            <ParlayCard key={idx} parlay={parlay} scriptType={selectedScript} />
          ))}
        </div>
      )}

      {/* Top Individual Props */}
      {currentScript && currentScript.top_individual_props && currentScript.top_individual_props.length > 0 && (
        <div className="space-y-3">
          <h3 className="text-sm font-bold text-slate-600 dark:text-slate-400 uppercase tracking-wider">
            Top Individual Props
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
            {currentScript.top_individual_props.slice(0, 6).map((prop, idx) => (
              <PropCard key={idx} prop={prop} />
            ))}
          </div>
        </div>
      )}

      {/* How to Read Section */}
      <div className="bg-slate-100 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg p-4 mt-6">
        <h4 className="text-sm font-bold text-slate-700 dark:text-slate-300 mb-2 flex items-center gap-2">
          <Target size={16} className="text-blue-600" />
          How to Read These Insights
        </h4>
        <div className="space-y-2 text-xs text-slate-600 dark:text-slate-400">
          <p>
            <span className="font-bold text-slate-800 dark:text-slate-200">Confidence:</span> Higher percentage = stronger AI correlation with the game script. 75%+ is considered high confidence.
          </p>
          <p>
            <span className="font-bold text-slate-800 dark:text-slate-200">Parlays:</span> Multiple legs combined for higher payouts. Each leg must hit for the parlay to win.
          </p>
          <p>
            <span className="font-bold text-slate-800 dark:text-slate-200">Game Script:</span> OVER = pass-heavy offense expected. UNDER = run-heavy, defensive game expected.
          </p>
        </div>
      </div>
    </div>
  );
};

const ParlayCard: React.FC<{ parlay: Parlay; scriptType: 'over' | 'under' }> = ({ parlay, scriptType }) => {
  const confidenceColor = parlay.confidence >= 0.75 ? 'text-green-600 dark:text-green-400' : 'text-yellow-600 dark:text-yellow-400';
  const bgGradient = scriptType === 'over' 
    ? 'from-orange-500/10 to-red-500/10 dark:from-orange-900/20 dark:to-red-900/20'
    : 'from-blue-500/10 to-purple-500/10 dark:from-blue-900/20 dark:to-purple-900/20';

  // Calculate confidence bar width
  const confidenceWidth = (parlay.confidence * 100).toFixed(0);

  return (
    <div className={`bg-gradient-to-br ${bgGradient} border border-slate-300 dark:border-slate-700 rounded-lg p-4 space-y-3 hover:shadow-lg transition-all duration-200`}>
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <h4 className="font-black text-slate-800 dark:text-slate-100">{parlay.name}</h4>
          <div className="flex items-center gap-2 mt-1">
            <p className="text-xs text-slate-500 dark:text-slate-400">{parlay.leg_count} legs</p>
            {/* Confidence bar */}
            <div className="flex-1 h-2 bg-slate-200 dark:bg-slate-700 rounded-full overflow-hidden">
              <div 
                className={`h-full ${parlay.confidence >= 0.75 ? 'bg-green-500' : 'bg-yellow-500'} transition-all duration-500`}
                style={{ width: `${confidenceWidth}%` }}
              />
            </div>
            <span className={`text-[10px] font-bold ${confidenceColor} min-w-[40px] text-right`}>
              {confidenceWidth}%
            </span>
          </div>
        </div>
        <div className="text-right ml-4">
          <div className="flex items-center gap-1 text-lg font-black text-green-600 dark:text-green-400">
            <DollarSign size={16} />
            {parlay.combined_odds}
          </div>
          <div className="text-[10px] text-slate-500 dark:text-slate-400">payout</div>
        </div>
      </div>
      
      <div className="space-y-2">
        {parlay.legs.map((leg, idx) => (
          <div key={idx} className="flex items-center justify-between text-xs bg-white/50 dark:bg-slate-800/50 rounded p-2 hover:bg-white/80 dark:hover:bg-slate-800/80 transition-colors">
            <div className="flex items-center gap-2">
              <span className="font-mono font-bold bg-slate-700 dark:bg-slate-600 text-white px-1.5 py-0.5 rounded text-[10px]">
                {leg.position}
              </span>
              <span className="font-bold text-slate-800 dark:text-slate-100">{leg.player_name}</span>
            </div>
            <div className="text-right">
              <div className="font-bold text-slate-700 dark:text-slate-300">
                {leg.side === 'over' ? 'O' : 'U'} {leg.line} {leg.prop_type}
              </div>
              <div className="text-[10px] text-slate-500">{leg.odds}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

const PropCard: React.FC<{ prop: PropLeg }> = ({ prop }) => {
  // Determine side indicator color
  const sideColor = prop.side === 'over' ? 'text-orange-600 dark:text-orange-400' : 'text-blue-600 dark:text-blue-400';
  const sideBg = prop.side === 'over' ? 'bg-orange-100 dark:bg-orange-900/20' : 'bg-blue-100 dark:bg-blue-900/20';

  return (
    <div className="flex items-center justify-between bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg p-3 hover:shadow-md hover:border-slate-300 dark:hover:border-slate-600 transition-all duration-200">
      <div className="flex items-center gap-2 flex-1">
        <span className="font-mono font-bold bg-slate-700 dark:bg-slate-600 text-white px-1.5 py-0.5 rounded text-[10px]">
          {prop.position}
        </span>
        <div className="flex-1 min-w-0">
          <div className="font-bold text-xs text-slate-800 dark:text-slate-100 truncate">{prop.player_name}</div>
          <div className="text-[10px] text-slate-500 truncate">{prop.prop_type}</div>
        </div>
      </div>
      <div className="text-right ml-2">
        <div className={`font-bold text-xs ${sideBg} ${sideColor} px-2 py-1 rounded`}>
          {prop.side === 'over' ? 'O' : 'U'} {prop.line}
        </div>
        <div className="text-[10px] text-green-600 dark:text-green-400 font-bold mt-0.5">{prop.odds}</div>
      </div>
    </div>
  );
};

export default MatchupInsights;
