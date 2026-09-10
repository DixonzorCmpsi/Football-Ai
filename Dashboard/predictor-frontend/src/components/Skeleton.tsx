import React from 'react';

export const SkeletonBlock: React.FC<{ className?: string }> = ({ className = '' }) => (
  <div className={`animate-pulse rounded bg-slate-200 dark:bg-slate-700/60 ${className}`} />
);

// One fake player card. This mirrors PlayerCard's real structure — a header row
// (avatar / name / Avg+Proj) above the Vegas props grid, inside a min-h-[9rem]
// shell. An earlier version was a single ~64px row against a real card of
// ~144-260px, so the swap-in jumped the layout by 2-4x per card: exactly what a
// skeleton is supposed to prevent.
export const PlayerCardSkeleton: React.FC = () => (
  <div className="rounded-xl p-3 border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 flex flex-col min-h-[9rem]">
    {/* header: avatar | name+meta | avg/proj */}
    <div className="grid grid-cols-[auto_1fr_auto] gap-3 mb-1 items-center">
      <SkeletonBlock className="w-10 h-10 rounded-full shrink-0" />
      <div className="min-w-0 space-y-1.5">
        <SkeletonBlock className="h-3 w-2/5" />
        <SkeletonBlock className="h-2.5 w-1/4" />
      </div>
      <div className="flex gap-2 pl-2">
        <SkeletonBlock className="h-6 w-8 rounded" />
        <SkeletonBlock className="h-6 w-9 rounded" />
      </div>
    </div>

    {/* Vegas props grid: implied total + 2 prop rows + anytime TD */}
    <div className="bg-slate-50 dark:bg-black/20 rounded-lg px-2 py-1.5 space-y-1.5 border border-slate-100 dark:border-white/5 mt-auto">
      {Array.from({ length: 4 }).map((_, i) => (
        <div key={i} className="flex justify-between items-center gap-2">
          <SkeletonBlock className="h-2 w-1/3" />
          <SkeletonBlock className="h-2 w-8" />
        </div>
      ))}
    </div>
  </div>
);

// The pool/tier rails render compact one-line rows (~46px), not full cards, so
// they get their own skeleton rather than reusing PlayerCardSkeleton.
export const PlayerRowSkeleton: React.FC = () => (
  <div className="flex items-center gap-2 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 px-2 py-1.5">
    <SkeletonBlock className="w-7 h-7 rounded-full shrink-0" />
    <div className="flex-1 space-y-1">
      <SkeletonBlock className="h-2.5 w-1/2" />
      <SkeletonBlock className="h-2 w-1/3" />
    </div>
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
    <div className="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 lg:h-[70vh] flex flex-col overflow-hidden">
      {/* mirrors the real panel's "Unranked Players" header bar */}
      <div className="px-3 py-2 border-b border-slate-100 dark:border-slate-800 bg-slate-50/60 dark:bg-slate-800/40">
        <SkeletonBlock className="h-2.5 w-24" />
      </div>
      <div className="p-2 space-y-2">
        {Array.from({ length: 10 }).map((_, i) => (
          <PlayerRowSkeleton key={i} />
        ))}
      </div>
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
