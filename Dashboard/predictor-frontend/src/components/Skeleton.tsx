import React from 'react';

export const SkeletonBlock: React.FC<{ className?: string }> = ({ className = '' }) => (
  <div className={`animate-pulse rounded bg-slate-200 dark:bg-slate-700/60 ${className}`} />
);

// One fake player-card row — mirrors PlayerCard's rough shape so the swap-in
// from skeleton to real content doesn't jump the layout around.
export const PlayerCardSkeleton: React.FC = () => (
  <div className="flex items-center gap-3 p-3 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900">
    <SkeletonBlock className="w-10 h-10 rounded-full shrink-0" />
    <div className="flex-1 space-y-2">
      <SkeletonBlock className="h-3 w-2/5" />
      <SkeletonBlock className="h-2.5 w-1/4" />
    </div>
    <SkeletonBlock className="h-6 w-10 rounded" />
  </div>
);

// Matches MatchupView's banner + two-column roster layout so the drill-through
// page doesn't collapse to a bare spinner while the fetch is in flight.
export const MatchupSkeleton: React.FC = () => (
  <div className="animate-in fade-in duration-300">
    <div className="mb-1 rounded-xl overflow-hidden border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 p-4">
      <div className="flex items-center justify-center gap-6">
        <SkeletonBlock className="w-12 h-12 rounded-full" />
        <SkeletonBlock className="h-6 w-24" />
        <SkeletonBlock className="w-12 h-12 rounded-full" />
      </div>
    </div>
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-x-8 gap-y-4 mt-4">
      {[0, 1].map((col) => (
        <div key={col}>
          <SkeletonBlock className="h-5 w-20 mb-3" />
          <div className="space-y-3">
            {Array.from({ length: 6 }).map((_, i) => (
              <PlayerCardSkeleton key={i} />
            ))}
          </div>
        </div>
      ))}
    </div>
  </div>
);

// Matches the Rank tab / Ranks page board: pool column + tier rows.
export const RankBoardSkeleton: React.FC = () => (
  <div className="animate-in fade-in duration-300 grid gap-3 lg:grid-cols-[260px_1fr]">
    <div className="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 p-2 space-y-2 lg:h-[70vh]">
      {Array.from({ length: 7 }).map((_, i) => (
        <PlayerCardSkeleton key={i} />
      ))}
    </div>
    <div className="space-y-2">
      {Array.from({ length: 5 }).map((_, i) => (
        <div
          key={i}
          className="flex items-stretch gap-2 rounded-xl border-2 border-slate-200 dark:border-slate-700 p-2 min-h-[9rem]"
        >
          <SkeletonBlock className="w-16 sm:w-20 shrink-0 rounded-lg" />
          <div className="flex-1" />
        </div>
      ))}
    </div>
  </div>
);
