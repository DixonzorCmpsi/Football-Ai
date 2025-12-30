import React from 'react';

interface MatchupBannerProps {
  matchup: string;
  overUnder: number | null;
  homeWinProb: number | null;
  awayWinProb: number | null;
}

const MatchupBanner: React.FC<MatchupBannerProps> = ({ matchup, overUnder, homeWinProb, awayWinProb }) => {
  const [away, home] = matchup.split(' @ ');

  return (
    <div className="w-full relative overflow-hidden p-4">
      {/* Background with Theme Support */}
      <div className="absolute inset-0 bg-white dark:bg-slate-900"></div>
      <div className="absolute inset-0 bg-gradient-to-r from-slate-50 via-white to-slate-50 dark:from-slate-900 dark:via-slate-800 dark:to-slate-900 opacity-50"></div>
      
      <div className="max-w-7xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4 relative z-10">
        
        {/* Teams & Odds */}
        <div className="flex items-center gap-6 flex-1 justify-center md:justify-start">
          <div className="text-center">
            <h2 className="text-3xl font-black text-slate-900 dark:text-white tracking-tighter leading-none">{away}</h2>
            {awayWinProb && (
              <span className={`text-[10px] font-bold uppercase tracking-wider ${awayWinProb > 50 ? 'text-green-600 dark:text-green-400' : 'text-slate-400 dark:text-slate-500'}`}>
                {awayWinProb}% Win
              </span>
            )}
          </div>

          <div className="text-slate-300 dark:text-slate-600 font-mono text-xl font-thin">@</div>

          <div className="text-center">
            <h2 className="text-3xl font-black text-slate-900 dark:text-white tracking-tighter leading-none">{home}</h2>
            {homeWinProb && (
              <span className={`text-[10px] font-bold uppercase tracking-wider ${homeWinProb > 50 ? 'text-green-600 dark:text-green-400' : 'text-slate-400 dark:text-slate-500'}`}>
                {homeWinProb}% Win
              </span>
            )}
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
                <span className="text-slate-600 dark:text-slate-300 font-mono"><span className="text-slate-400 dark:text-slate-500 mr-1">{away}</span>{overUnder && awayWinProb ? ((overUnder / 2) - (awayWinProb > 50 ? -1.5 : 1.5)).toFixed(1) : '-'}</span>
                <span className="text-slate-600 dark:text-slate-300 font-mono"><span className="text-slate-400 dark:text-slate-500 mr-1">{home}</span>{overUnder && homeWinProb ? ((overUnder / 2) - (homeWinProb > 50 ? -1.5 : 1.5)).toFixed(1) : '-'}</span>
              </div>
            </div>
        </div>

      </div>
    </div>
  );
};

export default MatchupBanner;