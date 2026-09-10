import React, { useState } from 'react';
import { CloudSun, ChevronDown } from 'lucide-react';

export interface BannerWeather {
  is_dome: boolean;
  roof: string;
  temp_f: number | null;
  wind_mph: number | null;
  condition: string;
  precip_chance: number | null;
}

export interface BannerGameScript {
  tag: 'SHOOTOUT' | 'GRIND_IT_OUT' | 'BLOWOUT_RISK' | 'BALANCED';
  label: string;
  summary: string;
  away_strength_note: string | null;
  home_strength_note: string | null;
}

interface MatchupBannerProps {
  matchup: string;
  gameTime?: string;
  gameDay?: string;
  overUnder: number | null;
  spread?: number | null;
  homeWinProb: number | null;
  awayWinProb: number | null;
  // Optional real-data game intelligence — weather, derived script tag, and
  // last-season offense/defense strength — shown as one compact row inside
  // this same banner instead of separate stacked boxes elsewhere on the page.
  weather?: BannerWeather | null;
  gameScript?: BannerGameScript | null;
  // View-switcher (Roster/Injuries/Insights/Rank) rendered inside this same
  // banner instead of its own row above it, to reclaim that row's height.
  tabs?: React.ReactNode;
}

function weatherOutlook(w: BannerWeather | null | undefined): { level: 'GREEN' | 'YELLOW' | 'RED'; summary: string } | null {
  if (!w) return null;
  if (w.is_dome) return { level: 'GREEN', summary: 'Dome · controlled conditions' };
  const wind = w.wind_mph ?? 0;
  const precip = w.precip_chance ?? 0;
  let level: 'GREEN' | 'YELLOW' | 'RED' = 'GREEN';
  if (wind >= 25 || precip >= 60) level = 'RED';
  else if (wind >= 15 || precip >= 30) level = 'YELLOW';
  const parts = [`${w.condition}`];
  if (w.temp_f != null) parts.push(`${w.temp_f}°F`);
  if (w.wind_mph != null) parts.push(`wind ${w.wind_mph} mph`);
  return { level, summary: parts.join(' · ') };
}

const SCRIPT_STYLES: Record<BannerGameScript['tag'], string> = {
  SHOOTOUT: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300',
  GRIND_IT_OUT: 'bg-sky-100 text-sky-700 dark:bg-sky-900/30 dark:text-sky-300',
  BLOWOUT_RISK: 'bg-rose-100 text-rose-700 dark:bg-rose-900/30 dark:text-rose-300',
  BALANCED: 'bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300',
};

const MatchupBanner: React.FC<MatchupBannerProps> = ({ matchup, gameTime, overUnder, spread, homeWinProb, awayWinProb, weather, gameScript, tabs }) => {
  const [away, home] = matchup.split(' @ ');
  const outlook = weatherOutlook(weather);
  const [intelOpen, setIntelOpen] = useState(true);
  // Collapses the WHOLE banner (teams, logos, odds, intel) down to one thin
  // row — frees the vertical space a fixed-height tier board needs to show
  // all five tiers without scrolling.
  const [bannerOpen, setBannerOpen] = useState(true);

  // Convert 24-hour time to 12-hour format (e.g., "16:25" -> "4:25 PM")
  const formatGameTime = (time: string | undefined): string => {
    if (!time) return '';
    const [hourStr, minute] = time.split(':');
    let hour = parseInt(hourStr, 10);
    if (isNaN(hour)) return time;
    const ampm = hour >= 12 ? 'PM' : 'AM';
    hour = hour % 12 || 12;
    return `${hour}:${minute} ${ampm}`;
  };

  // Calculate implied scores based on spread when available
  // Standard convention: Negative spread means favored (e.g. -6.5)
  // Home Score = (Total - Spread) / 2  => (Total - (-6.5))/2 = (Total + 6.5)/2 (Higher)
  // Away Score = (Total + Spread) / 2  => (Total + (-6.5))/2 = (Total - 6.5)/2 (Lower)
  const calculateImpliedScores = () => {
    if (!overUnder) return { home: null, away: null };
    // If spread is available, use it for accurate implied scores
    if (spread !== null && spread !== undefined) {
      const homeScore = (overUnder - spread) / 2;
      const awayScore = (overUnder + spread) / 2;
      return { home: homeScore.toFixed(1), away: awayScore.toFixed(1) };
    }
    // Fallback: if spread unavailable, show total with equal split hint
    return { home: null, away: null };
  };

  const impliedScores = calculateImpliedScores();
  const formattedTime = formatGameTime(gameTime);

  if (!bannerOpen) {
    return (
      <div className="w-full relative overflow-hidden px-3 py-2">
        <div className="absolute inset-0 bg-white dark:bg-slate-900"></div>
        <div className="flex items-center gap-2 relative z-10">
          <button
            onClick={() => setBannerOpen(true)}
            className="flex items-center gap-2 text-left flex-1 min-w-0"
          >
            <ChevronDown size={13} className="text-slate-400 dark:text-slate-500 -rotate-90 shrink-0" />
            <span className="text-sm font-black text-slate-800 dark:text-white tracking-tight whitespace-nowrap">{away} @ {home}</span>
            {overUnder != null && (
              <span className="text-[11px] font-mono text-slate-500 dark:text-slate-400 whitespace-nowrap">O/U {overUnder}</span>
            )}
            {gameScript && (
              <span className={`px-1.5 py-0.5 rounded text-[9px] font-black uppercase tracking-wider whitespace-nowrap ${SCRIPT_STYLES[gameScript.tag]}`}>
                {gameScript.label}
              </span>
            )}
          </button>
          {tabs && (
            <div className="shrink-0" onClick={(e) => e.stopPropagation()}>
              {tabs}
            </div>
          )}
          <button
            onClick={() => setBannerOpen(true)}
            className="shrink-0 text-[10px] font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest hover:text-slate-600 dark:hover:text-slate-300 transition-colors"
          >
            Expand
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="w-full relative overflow-hidden p-3 lg:p-4">
      {/* Background with Theme Support */}
      <div className="absolute inset-0 bg-white dark:bg-slate-900"></div>
      <div className="absolute inset-0 bg-gradient-to-r from-slate-50 via-white to-slate-50 dark:from-slate-900 dark:via-slate-800 dark:to-slate-900 opacity-50"></div>

      <div
        className="flex flex-col md:flex-row justify-between items-center gap-2 lg:gap-4 relative z-10 cursor-pointer rounded-lg -m-1 p-1 transition-colors hover:bg-slate-50/70 dark:hover:bg-slate-800/40"
        onClick={() => setBannerOpen(false)}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setBannerOpen(false); } }}
        title="Click to collapse"
      >

        {/* Teams & Odds with logos */}
        <div className="flex items-center gap-4 lg:gap-6 flex-1 justify-center md:justify-start">
          <div className="text-center flex items-center gap-2 lg:gap-3">
            <div className="w-10 h-10 lg:w-12 lg:h-12 rounded-full bg-slate-100 dark:bg-slate-700 overflow-hidden flex items-center justify-center border border-slate-200 dark:border-slate-700">
              <img src={`https://a.espncdn.com/i/teamlogos/nfl/500/${away.toLowerCase()}.png`} alt={`${away} logo`} className="w-8 h-8 lg:w-10 lg:h-10 object-contain" onError={(e)=>{(e.target as HTMLImageElement).style.display='none'}} />
            </div>
            <div>
              <h2 className="text-xl lg:text-2xl font-black text-slate-800 dark:text-white tracking-tighter leading-none">{away}</h2>
              {awayWinProb && (
                <span className={`text-[10px] font-bold uppercase tracking-wider ${awayWinProb > 50 ? 'text-green-600 dark:text-green-400' : 'text-slate-400 dark:text-slate-500'}`}>
                  {awayWinProb}% Win
                </span>
              )}
            </div>
          </div>

          <div className="flex flex-col items-center">
            <div className="text-slate-300 dark:text-slate-600 font-mono text-xl font-thin">@</div>
            {formattedTime && (
              <span className="text-[10px] font-bold text-slate-500 dark:text-slate-400 whitespace-nowrap">
                {formattedTime}
              </span>
            )}
          </div>

          <div className="text-center flex items-center gap-2 lg:gap-3">
            <div>
              <h2 className="text-xl lg:text-2xl font-black text-slate-800 dark:text-white tracking-tighter leading-none">{home}</h2>
              {homeWinProb && (
                <span className={`text-[10px] font-bold uppercase tracking-wider ${homeWinProb > 50 ? 'text-green-600 dark:text-green-400' : 'text-slate-400 dark:text-slate-500'}`}>
                  {homeWinProb}% Win
                </span>
              )}
            </div>
            <div className="w-10 h-10 lg:w-12 lg:h-12 rounded-full bg-slate-100 dark:bg-slate-700 overflow-hidden flex items-center justify-center border border-slate-200 dark:border-slate-700">
              <img src={`https://a.espncdn.com/i/teamlogos/nfl/500/${home.toLowerCase()}.png`} alt={`${home} logo`} className="w-8 h-8 lg:w-10 lg:h-10 object-contain" onError={(e)=>{(e.target as HTMLImageElement).style.display='none'}} />
            </div>
          </div>
        </div>

        {/* Vegas Context */}
        <div className="flex items-center gap-3 lg:gap-4 bg-slate-100 dark:bg-slate-950/50 rounded-lg px-3 lg:px-4 py-1.5 lg:py-2 border border-slate-200 dark:border-slate-800/50 backdrop-blur-sm">
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

      {/* No weather/script data for this game — the tab switcher still needs
          a home, since it normally lives inside the Game intel row below. */}
      {!(outlook || gameScript) && tabs && (
        <div className="mt-2 pt-2 border-t border-slate-200/70 dark:border-slate-800/70 flex justify-end relative z-10">
          {tabs}
        </div>
      )}

      {(outlook || gameScript) && (
        <div className="mt-2 pt-2 border-t border-slate-200/70 dark:border-slate-800/70 relative z-10">
          <div className="flex items-center justify-between gap-2">
            <button
              onClick={() => setIntelOpen((v) => !v)}
              className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-widest text-slate-400 dark:text-slate-500 hover:text-slate-600 dark:hover:text-slate-300 transition-colors"
            >
              <ChevronDown size={12} className={`transition-transform ${intelOpen ? 'rotate-180' : ''}`} />
              Game intel
            </button>
            {intelOpen && tabs && <div className="shrink-0">{tabs}</div>}
          </div>

          {intelOpen && (
            <div className="mt-1.5 space-y-1 text-[11px]">
              <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                {outlook && (
                  <span
                    className={`flex items-center gap-1 px-1.5 py-0.5 rounded font-bold whitespace-nowrap shrink-0 ${
                      outlook.level === 'GREEN'
                        ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
                        : outlook.level === 'YELLOW'
                        ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
                        : 'bg-rose-100 text-rose-700 dark:bg-rose-900/30 dark:text-rose-300'
                    }`}
                  >
                    <CloudSun size={11} /> {outlook.level} · {outlook.summary}
                  </span>
                )}
                {gameScript && (
                  <>
                    <span
                      className={`px-1.5 py-0.5 rounded text-[10px] font-black uppercase tracking-wider whitespace-nowrap shrink-0 ${SCRIPT_STYLES[gameScript.tag]}`}
                    >
                      {gameScript.label}
                    </span>
                    <span className="text-slate-500 dark:text-slate-400">{gameScript.summary}</span>
                  </>
                )}
              </div>
              {gameScript && (gameScript.away_strength_note || gameScript.home_strength_note) && (
                <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-slate-500 dark:text-slate-400">
                  {gameScript.away_strength_note && (
                    <span><span className="font-bold text-slate-700 dark:text-slate-300">{away}</span> {gameScript.away_strength_note}</span>
                  )}
                  {gameScript.home_strength_note && (
                    <span><span className="font-bold text-slate-700 dark:text-slate-300">{home}</span> {gameScript.home_strength_note}</span>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default MatchupBanner;
