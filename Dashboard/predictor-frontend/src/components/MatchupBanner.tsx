import React from 'react';

interface MatchupBannerProps {
  matchup: string;
  overUnder: number | null;
  spread?: number | null;
  homeWinProb: number | null;
  awayWinProb: number | null;
}

const MatchupBanner: React.FC<MatchupBannerProps> = ({ matchup, overUnder, spread, homeWinProb, awayWinProb }) => {
  const [away, home] = matchup.split(' @ ');

  // Calculate implied scores based on spread when available
  // spread > 0 means home is favored
  // total = home_score + away_score
  // home_score = (total + spread) / 2
  // away_score = (total - spread) / 2
  // TODO: Spread data integration pending - currently returning null from backend
  const calculateImpliedScores = () => {
    if (!overUnder) return { home: null, away: null };
    // If spread is available, use it for accurate implied scores
    if (spread !== null && spread !== undefined) {
      const homeScore = (overUnder + spread) / 2;
      const awayScore = (overUnder - spread) / 2;
      return { home: homeScore.toFixed(1), away: awayScore.toFixed(1) };
    }
    // Fallback: if spread unavailable, show total with equal split hint
    return { home: null, away: null };
  };

  const impliedScores = calculateImpliedScores();

  return (
    <div className="w-full relative overflow-hidden p-4">
      {/* Background with Theme Support */}
      <div className="absolute inset-0 bg-white dark:bg-slate-900"></div>
      <div className="absolute inset-0 bg-gradient-to-r from-slate-50 via-white to-slate-50 dark:from-slate-900 dark:via-slate-800 dark:to-slate-900 opacity-50"></div>
      
      <div className="max-w-7xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4 relative z-10">
        
        {/* Teams & Odds with logos */}
        <div className="flex items-center gap-6 flex-1 justify-center md:justify-start">
          <div className="text-center flex items-center gap-3">
            <div className="w-12 h-12 rounded-full bg-slate-100 dark:bg-slate-700 overflow-hidden flex items-center justify-center border border-slate-200 dark:border-slate-700">
              <img src={`https://a.espncdn.com/i/teamlogos/nfl/500/${away.toLowerCase()}.png`} alt={`${away} logo`} className="w-10 h-10 object-contain" onError={(e)=>{(e.target as HTMLImageElement).style.display='none'}} />
            </div>
            <div>
              <h2 className="text-2xl font-black text-slate-800 dark:text-white tracking-tighter leading-none">{away}</h2>
              {awayWinProb && (
                <span className={`text-[10px] font-bold uppercase tracking-wider ${awayWinProb > 50 ? 'text-green-600 dark:text-green-400' : 'text-slate-400 dark:text-slate-500'}`}>
                  {awayWinProb}% Win
                </span>
              )}
            </div>
          </div>

          <div className="text-slate-300 dark:text-slate-600 font-mono text-xl font-thin">@</div>

          <div className="text-center flex items-center gap-3">
            <div>
              <h2 className="text-2xl font-black text-slate-800 dark:text-white tracking-tighter leading-none">{home}</h2>
              {homeWinProb && (
                <span className={`text-[10px] font-bold uppercase tracking-wider ${homeWinProb > 50 ? 'text-green-600 dark:text-green-400' : 'text-slate-400 dark:text-slate-500'}`}>
                  {homeWinProb}% Win
                </span>
              )}
            </div>
            <div className="w-12 h-12 rounded-full bg-slate-100 dark:bg-slate-700 overflow-hidden flex items-center justify-center border border-slate-200 dark:border-slate-700">
              <img src={`https://a.espncdn.com/i/teamlogos/nfl/500/${home.toLowerCase()}.png`} alt={`${home} logo`} className="w-10 h-10 object-contain" onError={(e)=>{(e.target as HTMLImageElement).style.display='none'}} />
            </div>
          </div>
        </div>

        {/* Vegas Context */}
        <div className="flex items-center gap-4 bg-slate-100 dark:bg-slate-950/50 rounded-lg px-4 py-2 border border-slate-200 dark:border-slate-800/50 backdrop-blur-sm">
            <div className="flex flex-col items-center">
              <span className="text-slate-400 dark:text-slate-500 uppercase text-[9px] font-bold tracking-widest">Total</span>
              <span className="text-slate-800 dark:text-slate-200 font-mono text-sm font-bold">{overUnder || '-'}</span>
            </div>
            <div className="h-6 w-px bg-slate-200 dark:bg-slate-800"></div>
            <div className="flex flex-col items-center">
              <span className="text-slate-400 dark:text-slate-500 uppercase text-[9px] font-bold tracking-widest">Implied Score</span>
              <div className="flex gap-3 text-xs">
                <span className="text-slate-600 dark:text-slate-300 font-mono"><span className="text-slate-400 dark:text-slate-500 mr-1">{away}</span>{impliedScores.away !== null ? impliedScores.away : '-'}</span>
                <span className="text-slate-600 dark:text-slate-300 font-mono"><span className="text-slate-400 dark:text-slate-500 mr-1">{home}</span>{impliedScores.home !== null ? impliedScores.home : '-'}</span>
              </div>
            </div>
        </div>

      </div>
    </div>
  );
};

export default MatchupBanner;