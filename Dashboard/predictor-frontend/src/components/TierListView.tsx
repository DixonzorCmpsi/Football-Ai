import React, { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Save,
  RotateCcw,
  RefreshCw,
  Sparkles,
  TrendingUp,
  Plus,
  Check,
  History,
  X,
  Search,
} from 'lucide-react';
import {
  TIERS,
  usePositionPool,
  saveTierList,
  fetchTierList,
  refreshRookies,
  type Tier,
  type PoolPlayer,
  type TierAssignment,
} from '../hooks/useNflData';
import { getTeamColor } from '../utils/nflColors';

type Position = 'ALL' | 'QB' | 'RB' | 'WR' | 'TE';
const POSITIONS: Position[] = ['ALL', 'QB', 'RB', 'WR', 'TE'];

const TIER_STYLES: Record<Tier, { ring: string; chip: string; label: string }> = {
  UNRANKED: {
    ring: 'border-slate-300 dark:border-slate-700',
    chip: 'bg-slate-200 text-slate-600 dark:bg-slate-700 dark:text-slate-300',
    label: 'Unranked',
  },
  S: {
    ring: 'border-amber-400 dark:border-amber-500',
    chip: 'bg-gradient-to-br from-amber-400 to-yellow-500 text-white',
    label: 'S — Elite',
  },
  A: {
    ring: 'border-rose-400 dark:border-rose-500',
    chip: 'bg-gradient-to-br from-rose-400 to-red-500 text-white',
    label: 'A — Stud',
  },
  B: {
    ring: 'border-orange-400 dark:border-orange-500',
    chip: 'bg-gradient-to-br from-orange-400 to-amber-500 text-white',
    label: 'B — Solid Starter',
  },
  C: {
    ring: 'border-emerald-400 dark:border-emerald-500',
    chip: 'bg-gradient-to-br from-emerald-400 to-green-500 text-white',
    label: 'C — Flex',
  },
  D: {
    ring: 'border-sky-400 dark:border-sky-500',
    chip: 'bg-gradient-to-br from-sky-400 to-blue-500 text-white',
    label: 'D — Bench',
  },
  F: {
    ring: 'border-slate-500 dark:border-slate-600',
    chip: 'bg-gradient-to-br from-slate-500 to-slate-700 text-white',
    label: 'F — Avoid',
  },
};

// ───────────────────────────────────────── Pool side-panel card

interface PoolCardProps {
  player: PoolPlayer;
  rank: number;
  tier: Tier | null;
  isComparing: boolean;
  onDragStart: (id: string) => void;
  onDragEnd: () => void;
  onClickHistory: (id: string) => void;
  onToggleCompare: (id: string) => void;
  onClickName: (player: PoolPlayer) => void;
}

const PoolCard: React.FC<PoolCardProps> = memo(({
  player,
  rank,
  tier,
  isComparing,
  onDragStart,
  onDragEnd,
  onClickHistory,
  onToggleCompare,
  onClickName,
}) => {
  const teamColor = getTeamColor(player.team);
  const tiered = tier && tier !== 'UNRANKED';
  return (
    <div
      draggable
      onDragStart={(e) => {
        e.dataTransfer.setData('text/plain', player.player_id);
        e.dataTransfer.effectAllowed = 'move';
        onDragStart(player.player_id);
      }}
      onDragEnd={onDragEnd}
      className={`group relative rounded-lg border bg-white dark:bg-slate-800 px-2 py-1.5 cursor-grab active:cursor-grabbing transition-colors hover:shadow ${
        isComparing
          ? 'border-blue-500 ring-2 ring-blue-500/30'
          : tiered
          ? 'opacity-60 border-slate-200 dark:border-slate-700'
          : 'border-slate-200 dark:border-slate-700 hover:border-blue-300 dark:hover:border-blue-700'
      }`}
      title={
        tiered
          ? `${player.player_name} — already in tier ${tier}`
          : `${player.player_name} — drag to a tier`
      }
    >
      <div
        className="absolute left-0 top-0 bottom-0 w-1 rounded-l-lg"
        style={{ backgroundColor: teamColor }}
      />
      <div className="flex items-center gap-2 pl-1.5">
        <span className="text-[10px] font-mono font-black text-slate-400 dark:text-slate-500 w-5 text-right shrink-0">
          {rank}
        </span>
        <div className="w-7 h-7 rounded-full overflow-hidden bg-slate-100 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 shrink-0">
          {player.image ? (
            <img
              src={player.image}
              alt={player.player_name}
              className="w-full h-full object-cover"
              loading="lazy"
            />
          ) : null}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1">
            <button
              onClick={(e) => {
                e.stopPropagation();
                onClickName(player);
              }}
              className="text-xs font-bold text-slate-800 dark:text-slate-100 truncate hover:text-blue-600 dark:hover:text-blue-400 hover:underline text-left"
              title={`View ${player.team} offense`}
            >
              {player.player_name}
            </button>
            {player.is_rookie && (
              <span className="text-[8px] font-black px-1 rounded bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300">
                R
              </span>
            )}
            {tiered && (
              <span
                className={`text-[8px] font-black px-1 rounded ${TIER_STYLES[tier as Tier].chip}`}
              >
                {tier}
              </span>
            )}
          </div>
          <div className="flex items-center gap-1.5 mt-0.5 text-[9px]">
            <span className="font-bold uppercase text-slate-500 dark:text-slate-400">
              {player.team}
            </span>
            <span className="text-slate-300">·</span>
            {player.is_rookie && player.draft_number != null ? (
              <span className="font-mono text-purple-600 dark:text-purple-400">
                Pick #{player.draft_number}
              </span>
            ) : player.is_rookie ? (
              <span className="font-mono text-slate-400 italic">UDFA</span>
            ) : player.adp != null ? (
              <span className="font-mono text-slate-600 dark:text-slate-300">
                ADP {player.adp.toFixed(1)}
              </span>
            ) : (
              <span className="font-mono text-slate-600 dark:text-slate-300">
                {player.stats.season_avg_pts.toFixed(1)} proj
              </span>
            )}
            {!player.is_rookie && player.stats.snap_pct_avg > 0 && (
              <>
                <span className="text-slate-300">·</span>
                <span className="font-mono text-slate-500 dark:text-slate-400">
                  {Math.round(player.stats.snap_pct_avg)}% snap
                </span>
              </>
            )}
          </div>
        </div>
        <div className="flex flex-col gap-0.5">
          <button
            onClick={(e) => {
              e.stopPropagation();
              onToggleCompare(player.player_id);
            }}
            className={`w-5 h-5 rounded flex items-center justify-center text-[9px] transition ${
              isComparing
                ? 'bg-blue-600 text-white'
                : 'bg-slate-100 dark:bg-slate-700 text-slate-500 hover:text-blue-600 opacity-0 group-hover:opacity-100'
            }`}
            title="Compare"
          >
            {isComparing ? <Check size={9} strokeWidth={4} /> : <Plus size={9} strokeWidth={3} />}
          </button>
          <button
            onClick={(e) => {
              e.stopPropagation();
              onClickHistory(player.player_id);
            }}
            className="w-5 h-5 rounded flex items-center justify-center bg-slate-100 dark:bg-slate-700 text-slate-500 hover:text-purple-600 opacity-0 group-hover:opacity-100 transition"
            title="History"
          >
            <History size={9} />
          </button>
        </div>
      </div>
    </div>
  );
});
PoolCard.displayName = 'PoolCard';

// ───────────────────────────────────────── Tier row drop target

interface TierRowProps {
  tier: Tier;
  players: PoolPlayer[];
  onDrop: (tier: Tier) => void;
  onDropBefore: (tier: Tier, beforeId: string) => void;
  onDragOver: (tier: Tier | null) => void;
  onCardDragStart: (id: string) => void;
  onCardDragEnd: () => void;
  isHover: boolean;
  onRemove: (id: string) => void;
  onClickHistory: (id: string) => void;
  onToggleCompare: (id: string) => void;
  onClickName: (player: PoolPlayer) => void;
  compareSet: Set<string>;
}

// Order within a tier is a persisted, manually-set sequence (via onDropBefore) —
// NOT re-sorted by points on every render. The parent computes `players` already
// in the right order; this component just renders it and offers card-level drop
// zones so a card can be inserted before any existing one.
const TierRow: React.FC<TierRowProps> = memo(({
  tier,
  players,
  onDrop,
  onDropBefore,
  onDragOver,
  onCardDragStart,
  onCardDragEnd,
  isHover,
  onRemove,
  onClickHistory,
  onToggleCompare,
  onClickName,
  compareSet,
}) => {
  const style = TIER_STYLES[tier];
  return (
    <div
      onDragEnter={(e) => {
        e.preventDefault();
        onDragOver(tier);
      }}
      onDragOver={(e) => {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
      }}
      onDragLeave={() => onDragOver(null)}
      onDrop={(e) => {
        e.preventDefault();
        onDrop(tier);
      }}
      className={`flex gap-2 items-stretch border-2 rounded-xl transition-colors min-h-[5.5rem] ${
        isHover
          ? 'bg-blue-50 dark:bg-blue-900/20 border-blue-400 dark:border-blue-500 shadow-inner'
          : `bg-white dark:bg-slate-900 ${style.ring}`
      }`}
    >
      <div
        className={`w-16 sm:w-20 shrink-0 rounded-l-lg flex flex-col items-center justify-center font-black ${style.chip}`}
      >
        <span className="text-2xl sm:text-3xl leading-none">{tier === 'UNRANKED' ? '?' : tier}</span>
        <span className="text-[8px] mt-1 px-1 text-center uppercase tracking-wider opacity-90 hidden sm:block">
          {style.label}
        </span>
        <span className="text-[10px] mt-0.5 font-mono opacity-80">{players.length}</span>
      </div>
      <div className="flex-1 flex flex-wrap gap-2 p-2">
        {players.length === 0 ? (
          <div className="flex-1 flex items-center justify-center text-xs font-bold text-slate-300 dark:text-slate-600 italic">
            Drop players here
          </div>
        ) : (
          players.map((p) => {
            const tc = getTeamColor(p.team);
            const isComp = compareSet.has(p.player_id);
            return (
              <div
                key={p.player_id}
                draggable
                onDragStart={(e) => {
                  e.dataTransfer.setData('text/plain', p.player_id);
                  e.dataTransfer.effectAllowed = 'move';
                  onCardDragStart(p.player_id);
                }}
                onDragEnd={onCardDragEnd}
                onDragOver={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  e.dataTransfer.dropEffect = 'move';
                }}
                onDrop={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  onDropBefore(tier, p.player_id);
                }}
                className={`relative bg-white dark:bg-slate-800 rounded-lg border shadow-sm pl-2 pr-1.5 py-1.5 flex items-center gap-2 min-w-[148px] cursor-grab active:cursor-grabbing ${
                  isComp
                    ? 'border-blue-500 ring-2 ring-blue-500/30'
                    : 'border-slate-200 dark:border-slate-700'
                }`}
              >
                <span className="absolute left-0 top-0 bottom-0 w-1 rounded-l-lg" style={{ backgroundColor: tc }} />
                <div className="w-7 h-7 rounded-full overflow-hidden bg-slate-100 dark:bg-slate-700 shrink-0 border border-slate-200 dark:border-slate-600 ml-1">
                  {p.image && <img src={p.image} alt={p.player_name} className="w-full h-full object-cover" loading="lazy" />}
                </div>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => onClickName(p)}
                      className="text-xs font-bold text-slate-800 dark:text-slate-100 truncate hover:text-blue-600 dark:hover:text-blue-400 hover:underline text-left"
                      title={`View ${p.team} offense`}
                    >
                      {p.player_name}
                    </button>
                    {p.is_rookie && (
                      <span className="text-[8px] font-black px-1 rounded bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300">
                        R
                      </span>
                    )}
                  </div>
                  <div className="text-[9px] font-mono text-slate-500 dark:text-slate-400">
                    {p.team} · {p.stats.season_avg_pts.toFixed(1)}
                  </div>
                </div>
                <div className="flex flex-col gap-0.5">
                  <button
                    onClick={() => onToggleCompare(p.player_id)}
                    className={`w-5 h-5 rounded flex items-center justify-center text-[9px] ${
                      isComp
                        ? 'bg-blue-600 text-white'
                        : 'bg-slate-100 dark:bg-slate-700 text-slate-500 hover:text-blue-600'
                    }`}
                    title="Compare"
                  >
                    {isComp ? <Check size={9} strokeWidth={4} /> : <Plus size={9} strokeWidth={3} />}
                  </button>
                  <button
                    onClick={() => onClickHistory(p.player_id)}
                    className="w-5 h-5 rounded flex items-center justify-center bg-slate-100 dark:bg-slate-700 text-slate-500 hover:text-purple-600"
                    title="History"
                  >
                    <History size={9} />
                  </button>
                </div>
                <button
                  onClick={() => onRemove(p.player_id)}
                  className="absolute -top-1 -right-1 w-4 h-4 rounded-full bg-slate-300 dark:bg-slate-600 text-white text-[10px] flex items-center justify-center hover:bg-red-500 transition"
                  title="Remove from tier"
                >
                  <X size={9} strokeWidth={3} />
                </button>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
});
TierRow.displayName = 'TierRow';

interface PoolPanelProps {
  label: string;
  players: PoolPlayer[];
  startRank: number;
  loadingPool: boolean;
  poolError: string | null;
  position: Position;
  assignments: Record<string, Tier>;
  compareSet: Set<string>;
  onDragStart: (id: string) => void;
  onDragEnd: () => void;
  onClickHistory: (id: string) => void;
  onToggleCompare: (id: string) => void;
  onClickName: (player: PoolPlayer) => void;
}

// Each pool panel scrolls independently — they used to be scroll-synced (one
// panel's scroll drove the other via rAF), which doubled scroll-handler cost
// on every tick and made this page feel sluggish for no real benefit.
const PoolPanel: React.FC<PoolPanelProps> = memo(({
  label,
  players,
  startRank,
  loadingPool,
  poolError,
  position,
  assignments,
  compareSet,
  onDragStart,
  onDragEnd,
  onClickHistory,
  onToggleCompare,
  onClickName,
}) => (
  <aside className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl flex flex-col overflow-hidden h-[78vh]">
    <div className="px-3 py-2 border-b border-slate-100 dark:border-slate-800 bg-slate-50/60 dark:bg-slate-800/40 flex items-center justify-between">
      <h3 className="text-[10px] font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
        {label}
      </h3>
      <span className="text-[10px] font-mono text-slate-400 dark:text-slate-500">
        {players.length}
      </span>
    </div>
    <div className="flex-1 overflow-y-auto p-2 space-y-1.5 scrollbar-thin overscroll-contain">
      {loadingPool ? (
        <div className="text-xs text-slate-400 italic text-center mt-8">Loading {position}...</div>
      ) : poolError ? (
        <div className="px-3 py-8 text-center">
          <div className="text-xs font-black text-red-500">Player API unavailable</div>
          <div className="text-[10px] text-slate-400 mt-1">{poolError}</div>
        </div>
      ) : players.length === 0 ? (
        <div className="text-xs text-slate-400 italic text-center mt-8">No players</div>
      ) : (
        players.map((p, idx) => (
          // content-visibility skips layout/paint for rows scrolled out of view —
          // this list can run past 300 players, and that was the single biggest
          // cost behind the page feeling slow to scroll. `auto` in
          // contain-intrinsic-size caches each row's real height so a reveal
          // doesn't reflow the rows below it (see MatchupView for the details).
          <div key={p.player_id} style={{ contentVisibility: 'auto', containIntrinsicSize: 'auto 46px' }}>
            <PoolCard
              player={p}
              rank={startRank + idx}
              tier={assignments[p.player_id] || null}
              isComparing={compareSet.has(p.player_id)}
              onDragStart={onDragStart}
              onDragEnd={onDragEnd}
              onClickHistory={onClickHistory}
              onToggleCompare={onToggleCompare}
              onClickName={onClickName}
            />
          </div>
        ))
      )}
    </div>
  </aside>
));
PoolPanel.displayName = 'PoolPanel';

// ───────────────────────────────────────── Main view

const ASSIGNABLE_TIERS: Tier[] = ['S', 'A', 'B', 'C', 'D', 'F'];

export interface TierListState {
  position: Position;
  listName: string;
  assignments: Record<string, Tier>;
  // Manually-set display order within each tier (player_ids). Only touched by
  // drag actions — never auto-resorted by points — so a player dropped between
  // two others stays exactly where the user put it.
  tierOrder?: Partial<Record<Tier, string[]>>;
  search: string;
  showRookiesOnly: boolean;
}

interface TierListViewProps {
  state: TierListState;
  onStateChange: (next: TierListState) => void;
  onViewHistory: (id: string) => void;
  compareList: string[];
  onToggleCompare: (id: string) => void;
  onOpenCompare: () => void;
  onOpenTeam: (team: string, focusPlayerId?: string) => void;
}

const TierListView: React.FC<TierListViewProps> = ({
  state,
  onStateChange,
  onViewHistory,
  compareList,
  onToggleCompare,
  onOpenCompare,
  onOpenTeam,
}) => {
  const { position, listName, assignments, search, showRookiesOnly } = state;
  const tierOrder = state.tierOrder || {};

  // Helpers so the body reads the same as before with local setters.
  const setPosition = (p: Position) => onStateChange({ ...state, position: p });
  const setListName = (n: string) => onStateChange({ ...state, listName: n });
  const setAssignmentsState = (
    updater: Record<string, Tier> | ((prev: Record<string, Tier>) => Record<string, Tier>),
  ) => {
    const next = typeof updater === 'function' ? (updater as any)(assignments) : updater;
    onStateChange({ ...state, assignments: next });
  };
  const setAssignments = setAssignmentsState;
  const setSearch = (s: string) => onStateChange({ ...state, search: s });
  const setShowRookiesOnly = (v: boolean | ((prev: boolean) => boolean)) =>
    onStateChange({
      ...state,
      showRookiesOnly: typeof v === 'function' ? (v as any)(showRookiesOnly) : v,
    });
  const [draggingId, setDraggingId] = useState<string | null>(null);
  const [hoverTier, setHoverTier] = useState<Tier | null>(null);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const [refreshStatus, setRefreshStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle');

  const { pool, loadingPool, poolError } = usePositionPool(position);

  // Load saved list ONLY when position changes (not on every state mutation —
  // otherwise typing in the list-name input would wipe assignments on every keystroke).
  const lastLoadKey = useRef<string | null>(null);
  useEffect(() => {
    const key = `${position}::${listName}`;
    if (lastLoadKey.current === key) return;
    lastLoadKey.current = key;
    let active = true;
    fetchTierList(position, listName).then((saved) => {
      if (!active) return;
      const map: Record<string, Tier> = {};
      saved?.assignments.forEach((a) => {
        map[a.player_id] = a.tier;
      });
      setAssignments(map);
    });
    return () => {
      active = false;
    };
  }, [position, listName]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (saveStatus === 'saved') setSaveStatus('idle');
  }, [assignments]); // eslint-disable-line react-hooks/exhaustive-deps

  const filteredPool = useMemo(() => {
    let out = pool;
    if (showRookiesOnly) out = out.filter((p) => p.is_rookie);
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      out = out.filter(
        (p) =>
          p.player_name.toLowerCase().includes(q) || p.team.toLowerCase().includes(q),
      );
    }
    return out;
  }, [pool, showRookiesOnly, search]);

  // Ranked pool:
  //   - Rookies-only filter on → sort by draft pick ASC (undrafted last)
  //   - Otherwise              → sort by real ADP ASC (this is the order people
  //     actually draft in — matches the board they're tiering against), with
  //     projected PPG as the tiebreak for players with no ADP signal.
  const rankedPool = useMemo(() => {
    const arr = [...filteredPool];
    if (showRookiesOnly) {
      arr.sort((a, b) => {
        const ap = a.draft_number ?? 9999;
        const bp = b.draft_number ?? 9999;
        if (ap !== bp) return ap - bp;
        return a.player_name.localeCompare(b.player_name);
      });
    } else {
      arr.sort((a, b) => {
        const aAdp = a.adp ?? Infinity;
        const bAdp = b.adp ?? Infinity;
        if (aAdp !== bAdp) return aAdp - bAdp;
        return (b.stats.season_avg_pts || 0) - (a.stats.season_avg_pts || 0);
      });
    }
    return arr;
  }, [filteredPool, showRookiesOnly]);

  const tierBuckets = useMemo(() => {
    const buckets: Record<Tier, PoolPlayer[]> = {
      UNRANKED: [],
      S: [],
      A: [],
      B: [],
      C: [],
      D: [],
      F: [],
    };
    pool.forEach((p) => {
      const t: Tier = assignments[p.player_id] || 'UNRANKED';
      if (t !== 'UNRANKED') buckets[t].push(p);
    });
    return buckets;
  }, [pool, assignments]);

  // Tier rows render this — tierOrder's manual sequence first, then any tier
  // members missing from tierOrder (legacy saved lists, auto-tier races) appended
  // sorted by points so nothing silently disappears.
  const orderedTierPlayers = useMemo(() => {
    const poolById = new Map(pool.map((p) => [p.player_id, p]));
    const result: Record<Tier, PoolPlayer[]> = {
      UNRANKED: [],
      S: [],
      A: [],
      B: [],
      C: [],
      D: [],
      F: [],
    };
    ASSIGNABLE_TIERS.forEach((tier) => {
      const seen = new Set<string>();
      const ordered: PoolPlayer[] = [];
      (tierOrder[tier] || []).forEach((id) => {
        if (assignments[id] === tier && poolById.has(id) && !seen.has(id)) {
          seen.add(id);
          ordered.push(poolById.get(id)!);
        }
      });
      const leftover = tierBuckets[tier]
        .filter((p) => !seen.has(p.player_id))
        .sort((a, b) => b.stats.season_avg_pts - a.stats.season_avg_pts);
      result[tier] = [...ordered, ...leftover];
    });
    return result;
  }, [pool, assignments, tierOrder, tierBuckets]);

  // Single entry point for every tier mutation: assign/move/unassign a player,
  // and place them at a specific position (before `beforeId`, or appended to the
  // end when null). Assignments (membership) and tierOrder (display sequence)
  // are updated together in one onStateChange call so they never fall out of sync.
  const placePlayer = useCallback(
    (tier: Tier, playerId: string, beforeId: string | null) => {
      const nextAssignments = { ...assignments };
      if (tier === 'UNRANKED') delete nextAssignments[playerId];
      else nextAssignments[playerId] = tier;

      const nextOrder: Partial<Record<Tier, string[]>> = {};
      ASSIGNABLE_TIERS.forEach((t) => {
        nextOrder[t] = (tierOrder[t] || []).filter((id) => id !== playerId);
      });
      if (tier !== 'UNRANKED') {
        const arr = nextOrder[tier] || [];
        if (beforeId && beforeId !== playerId) {
          const idx = arr.indexOf(beforeId);
          if (idx === -1) arr.push(playerId);
          else arr.splice(idx, 0, playerId);
        } else {
          arr.push(playerId);
        }
        nextOrder[tier] = arr;
      }
      onStateChange({ ...state, assignments: nextAssignments, tierOrder: nextOrder });
    },
    [assignments, tierOrder, state, onStateChange],
  );

  const moveToTier = useCallback(
    (tier: Tier) => {
      if (!draggingId) return;
      placePlayer(tier, draggingId, null);
      setDraggingId(null);
      setHoverTier(null);
    },
    [draggingId, placePlayer],
  );

  const moveToTierBefore = useCallback(
    (tier: Tier, beforeId: string) => {
      if (!draggingId) return;
      placePlayer(tier, draggingId, beforeId);
      setDraggingId(null);
      setHoverTier(null);
    },
    [draggingId, placePlayer],
  );

  const handleDragEnd = useCallback(() => {
    setDraggingId(null);
    setHoverTier(null);
  }, []);

  const handleOpenTeam = useCallback(
    (player: PoolPlayer) => onOpenTeam(player.team, player.player_id),
    [onOpenTeam],
  );

  const removeFromTier = useCallback(
    (id: string) => placePlayer('UNRANKED', id, null),
    [placePlayer],
  );

  const handleSave = async () => {
    setSaveStatus('saving');
    const payload = {
      name: listName.trim() || 'My Tiers',
      position,
      assignments: Object.entries(assignments).map(([player_id, tier]) => ({
        player_id,
        tier,
      })) as TierAssignment[],
      season: pool[0]?.season || new Date().getFullYear(),
    };
    const res = await saveTierList(payload);
    setSaveStatus(res ? 'saved' : 'error');
    setTimeout(() => setSaveStatus('idle'), 2500);
  };

  const handleReset = () => {
    if (!confirm(`Clear all tier assignments for ${position}?`)) return;
    onStateChange({ ...state, assignments: {}, tierOrder: {} });
  };

  const handleAutoTier = () => {
    const ranked = [...pool].sort(
      (a, b) => b.stats.season_avg_pts - a.stats.season_avg_pts,
    );
    const next: Record<string, Tier> = {};
    const nextOrder: Partial<Record<Tier, string[]>> = { S: [], A: [], B: [], C: [], D: [], F: [] };
    const cuts: { tier: Tier; pct: number }[] = [
      { tier: 'S', pct: 0.05 },
      { tier: 'A', pct: 0.15 },
      { tier: 'B', pct: 0.3 },
      { tier: 'C', pct: 0.55 },
      { tier: 'D', pct: 0.8 },
      { tier: 'F', pct: 1.0 },
    ];
    const eligible = ranked.filter((p) => p.stats.games_played > 0);
    const assign = (p: PoolPlayer, pct: number) => {
      const tier = cuts.find((c) => pct <= c.pct)?.tier || 'F';
      next[p.player_id] = tier;
      nextOrder[tier]!.push(p.player_id);
    };
    if (eligible.length === 0) {
      // offseason: tier the top 60 by name order alphabetically into S/A/B
      const fallback = [...pool].slice(0, 60);
      fallback.forEach((p, i) => assign(p, (i + 1) / fallback.length));
    } else {
      eligible.forEach((p, i) => assign(p, (i + 1) / eligible.length));
    }
    onStateChange({ ...state, assignments: next, tierOrder: nextOrder });
  };

  const handleRefreshRookies = async () => {
    setRefreshStatus('running');
    const res = await refreshRookies();
    setRefreshStatus(res.status === 'success' ? 'done' : 'error');
    setTimeout(() => setRefreshStatus('idle'), 4000);
    setPosition(position); // trigger pool re-fetch by reasserting current position
  };

  const compareCount = compareList.length;
  const compareSet = useMemo(() => new Set(compareList), [compareList]);
  const rookieCount = pool.filter((p) => p.is_rookie).length;

  return (
    <div className="w-full">
      {/* HEADER */}
      <div className="flex flex-wrap items-end justify-between gap-3 mb-3">
        <div>
          <h2 className="text-xs font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest">
            Tier List
          </h2>
          <div className="flex items-center gap-2 mt-1">
            <input
              value={listName}
              onChange={(e) => setListName(e.target.value)}
              className="text-xl sm:text-2xl font-black bg-transparent border-b-2 border-transparent focus:border-blue-500 outline-none text-slate-800 dark:text-slate-100 px-1 w-44 sm:w-64"
              placeholder="My Tiers"
            />
            <span className="text-xs text-slate-400">·</span>
            <span className="text-xs font-bold text-slate-500 dark:text-slate-400">
              {pool.length} {position === 'ALL' ? 'players' : `${position}s`}
            </span>
            {rookieCount > 0 && (
              <span className="text-xs font-bold text-purple-600 dark:text-purple-400">
                ({rookieCount} rookies)
              </span>
            )}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <div className="relative">
            <Search size={12} className="absolute left-2 top-1/2 -translate-y-1/2 text-slate-400" />
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Filter..."
              className="pl-7 pr-2 py-2 rounded-lg text-xs font-medium bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 outline-none focus:border-blue-500 w-40"
            />
          </div>
          <button
            onClick={handleAutoTier}
            className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-bold bg-purple-100 text-purple-700 hover:bg-purple-200 dark:bg-purple-900/30 dark:text-purple-300"
          >
            <Sparkles size={14} />
            Auto-tier
          </button>
          <button
            onClick={handleReset}
            className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-bold bg-slate-100 text-slate-600 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-300"
          >
            <RotateCcw size={14} />
            Reset
          </button>
          <button
            onClick={handleSave}
            disabled={saveStatus === 'saving'}
            className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-bold bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-60 shadow-sm"
          >
            <Save size={14} />
            {saveStatus === 'saving'
              ? 'Saving...'
              : saveStatus === 'saved'
              ? 'Saved!'
              : saveStatus === 'error'
              ? 'Error'
              : 'Save'}
          </button>
          <button
            onClick={handleRefreshRookies}
            disabled={refreshStatus === 'running'}
            className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-bold bg-amber-100 text-amber-700 hover:bg-amber-200 dark:bg-amber-900/30 dark:text-amber-300 disabled:opacity-60"
            title="Refresh rosters from nflreadpy + ESPN (post-draft rookies)"
          >
            <RefreshCw size={14} className={refreshStatus === 'running' ? 'animate-spin' : ''} />
            {refreshStatus === 'running'
              ? 'Pulling...'
              : refreshStatus === 'done'
              ? 'Rookies pulled'
              : 'Pull rookies'}
          </button>
          {compareCount > 0 && (
            <button
              onClick={onOpenCompare}
              className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-bold bg-blue-600 text-white shadow-sm hover:bg-blue-700"
            >
              <TrendingUp size={14} />
              Compare ({compareCount})
            </button>
          )}
        </div>
      </div>

      {/* CONTROLS ABOVE TIER ROWS — only position + rookies-only */}
      <div className="flex items-center gap-3 mb-3 flex-wrap">
        <div className="flex items-center gap-1 bg-slate-100 dark:bg-slate-800/60 p-1 rounded-lg border border-slate-200/60 dark:border-slate-700/60">
          {POSITIONS.map((p) => {
            const active = p === position;
            return (
              <button
                key={p}
                onClick={() => setPosition(p)}
                className={`px-4 py-1.5 rounded-md text-xs font-black transition-all ${
                  active
                    ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm ring-1 ring-black/5 dark:ring-white/5'
                    : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'
                }`}
              >
                {p}
              </button>
            );
          })}
        </div>
        <button
          onClick={() => setShowRookiesOnly(!showRookiesOnly)}
          className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-bold transition border ${
            showRookiesOnly
              ? 'bg-purple-600 text-white border-purple-700'
              : 'bg-white dark:bg-slate-800 text-slate-600 dark:text-slate-300 border-slate-200 dark:border-slate-700'
          }`}
        >
          Rookies only
        </button>
      </div>

      {/* TWO-COLUMN LAYOUT: single pool panel | tier rows. A single panel (instead
          of splitting the pool top/bottom across two side columns) is simpler to
          work through and lets tier rows use the width the second panel used to
          take — they already grow vertically/wrap horizontally on their own as
          players are added, so this is the panel that needed the room. */}
      <div className="grid gap-4 grid-cols-1 lg:grid-cols-[420px_minmax(0,1fr)]">
        <PoolPanel
          label="Pool"
          players={rankedPool}
          startRank={1}
          loadingPool={loadingPool}
          poolError={poolError}
          position={position}
          assignments={assignments}
          compareSet={compareSet}
          onDragStart={setDraggingId}
          onDragEnd={handleDragEnd}
          onClickHistory={onViewHistory}
          onToggleCompare={onToggleCompare}
          onClickName={handleOpenTeam}
        />
        <div className="space-y-2">
          {loadingPool ? (
            <div className="flex flex-col items-center py-16 opacity-60">
              <div className="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin" />
              <p className="text-sm font-bold text-slate-400 mt-3">Loading {position} pool...</p>
            </div>
          ) : poolError ? (
            <div className="rounded-xl border border-red-200 bg-red-50 dark:bg-red-950/20 dark:border-red-900 px-4 py-10 text-center">
              <p className="text-sm font-black text-red-600 dark:text-red-400">Player API unavailable</p>
              <p className="text-xs text-red-500/80 dark:text-red-300/80 mt-1">{poolError}</p>
            </div>
          ) : (
            TIERS.filter((t) => t !== 'UNRANKED').map((tier) => (
              <TierRow
                key={tier}
                tier={tier}
                players={orderedTierPlayers[tier]}
                isHover={hoverTier === tier}
                onDragOver={setHoverTier}
                onDrop={moveToTier}
                onDropBefore={moveToTierBefore}
                onCardDragStart={setDraggingId}
                onCardDragEnd={handleDragEnd}
                onRemove={removeFromTier}
                onClickHistory={onViewHistory}
                onToggleCompare={onToggleCompare}
                onClickName={(p) => onOpenTeam(p.team, p.player_id)}
                compareSet={compareSet}
              />
            ))
          )}
          <p className="text-[10px] text-slate-400 dark:text-slate-500 text-center italic mt-2">
            Drag from the pool panel into a tier. Click <X size={10} className="inline" /> on a tier
            card to remove.
          </p>
        </div>
      </div>
    </div>
  );
};

// Mounted for the whole session and hidden with display:none so local state
// (scroll position, drag assignments, filters) survives navigation. Without
// memo, any App state change re-renders this entire tree while it is off-screen.
export default memo(TierListView);
