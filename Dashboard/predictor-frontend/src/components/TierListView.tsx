import React, { useEffect, useMemo, useRef, useState } from 'react';
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
import TierVisualizations from './TierVisualizations';

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

const PoolCard: React.FC<PoolCardProps> = ({
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
      className={`group relative rounded-lg border bg-white dark:bg-slate-800 px-2 py-1.5 cursor-grab active:cursor-grabbing transition-all hover:shadow ${
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
};

// ───────────────────────────────────────── Tier row drop target

interface TierRowProps {
  tier: Tier;
  players: PoolPlayer[];
  onDrop: (tier: Tier) => void;
  onDragOver: (tier: Tier | null) => void;
  isHover: boolean;
  onRemove: (id: string) => void;
  onClickHistory: (id: string) => void;
  onToggleCompare: (id: string) => void;
  onClickName: (player: PoolPlayer) => void;
  compareList: string[];
}

const TierRow: React.FC<TierRowProps> = ({
  tier,
  players,
  onDrop,
  onDragOver,
  isHover,
  onRemove,
  onClickHistory,
  onToggleCompare,
  onClickName,
  compareList,
}) => {
  const style = TIER_STYLES[tier];
  const sorted = useMemo(
    () => [...players].sort((a, b) => b.stats.season_avg_pts - a.stats.season_avg_pts),
    [players],
  );
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
      className={`flex gap-2 items-stretch border-2 rounded-xl transition-all min-h-[5.5rem] ${
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
        {sorted.length === 0 ? (
          <div className="flex-1 flex items-center justify-center text-xs font-bold text-slate-300 dark:text-slate-600 italic">
            Drop players here
          </div>
        ) : (
          sorted.map((p) => {
            const tc = getTeamColor(p.team);
            const isComp = compareList.includes(p.player_id);
            return (
              <div
                key={p.player_id}
                className={`relative bg-white dark:bg-slate-800 rounded-lg border shadow-sm pl-2 pr-1.5 py-1.5 flex items-center gap-2 min-w-[148px] ${
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
};

// ───────────────────────────────────────── Main view

export interface TierListState {
  position: Position;
  listName: string;
  assignments: Record<string, Tier>;
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

  const { pool, loadingPool } = usePositionPool(position);

  // Synced scroll between left + right pool panes.
  const leftRef = useRef<HTMLDivElement | null>(null);
  const rightRef = useRef<HTMLDivElement | null>(null);
  const syncing = useRef(false);
  const syncFrom = (src: HTMLDivElement | null, dst: HTMLDivElement | null) => {
    if (!src || !dst || syncing.current) return;
    syncing.current = true;
    dst.scrollTop = src.scrollTop;
    requestAnimationFrame(() => {
      syncing.current = false;
    });
  };

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
  //   - Rookies-only filter on  → sort by draft pick ASC (undrafted last)
  //   - Otherwise               → sort by projected PPG DESC
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
      arr.sort((a, b) => (b.stats.season_avg_pts || 0) - (a.stats.season_avg_pts || 0));
    }
    return arr;
  }, [filteredPool, showRookiesOnly]);

  // Split into two halves for left/right panels.
  const { leftPool, rightPool } = useMemo(() => {
    const mid = Math.ceil(rankedPool.length / 2);
    return {
      leftPool: rankedPool.slice(0, mid),
      rightPool: rankedPool.slice(mid),
    };
  }, [rankedPool]);

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

  const moveToTier = (tier: Tier) => {
    if (!draggingId) return;
    setAssignments((prev) => {
      const next = { ...prev };
      if (tier === 'UNRANKED') delete next[draggingId];
      else next[draggingId] = tier;
      return next;
    });
    setDraggingId(null);
    setHoverTier(null);
  };

  const removeFromTier = (id: string) => {
    setAssignments((prev) => {
      const next = { ...prev };
      delete next[id];
      return next;
    });
  };

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
    setAssignments({});
  };

  const handleAutoTier = () => {
    const ranked = [...pool].sort(
      (a, b) => b.stats.season_avg_pts - a.stats.season_avg_pts,
    );
    const next: Record<string, Tier> = {};
    const cuts: { tier: Tier; pct: number }[] = [
      { tier: 'S', pct: 0.05 },
      { tier: 'A', pct: 0.15 },
      { tier: 'B', pct: 0.3 },
      { tier: 'C', pct: 0.55 },
      { tier: 'D', pct: 0.8 },
      { tier: 'F', pct: 1.0 },
    ];
    const eligible = ranked.filter((p) => p.stats.games_played > 0);
    if (eligible.length === 0) {
      // offseason: tier the top 60 by name order alphabetically into S/A/B
      const fallback = [...pool].slice(0, 60);
      fallback.forEach((p, i) => {
        const pct = (i + 1) / fallback.length;
        const tier = cuts.find((c) => pct <= c.pct)?.tier || 'F';
        next[p.player_id] = tier;
      });
    } else {
      eligible.forEach((p, i) => {
        const pct = (i + 1) / eligible.length;
        const tier = cuts.find((c) => pct <= c.pct)?.tier || 'F';
        next[p.player_id] = tier;
      });
    }
    setAssignments(next);
  };

  const handleRefreshRookies = async () => {
    setRefreshStatus('running');
    const res = await refreshRookies();
    setRefreshStatus(res.status === 'success' ? 'done' : 'error');
    setTimeout(() => setRefreshStatus('idle'), 4000);
    setPosition(position); // trigger pool re-fetch by reasserting current position
  };

  const compareCount = compareList.length;
  const rookieCount = pool.filter((p) => p.is_rookie).length;

  const PoolPanel: React.FC<{
    label: string;
    players: PoolPlayer[];
    panelRef: React.RefObject<HTMLDivElement | null>;
    otherRef: React.RefObject<HTMLDivElement | null>;
    startRank: number;
  }> = ({ label, players, panelRef, otherRef, startRank }) => (
    <aside className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl flex flex-col overflow-hidden h-[78vh]">
      <div className="px-3 py-2 border-b border-slate-100 dark:border-slate-800 bg-slate-50/60 dark:bg-slate-800/40 flex items-center justify-between">
        <h3 className="text-[10px] font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
          {label}
        </h3>
        <span className="text-[10px] font-mono text-slate-400 dark:text-slate-500">
          {players.length}
        </span>
      </div>
      <div
        ref={panelRef}
        onScroll={() => syncFrom(panelRef.current, otherRef.current)}
        className="flex-1 overflow-y-auto p-2 space-y-1.5 scrollbar-thin"
      >
        {loadingPool ? (
          <div className="text-xs text-slate-400 italic text-center mt-8">Loading {position}...</div>
        ) : players.length === 0 ? (
          <div className="text-xs text-slate-400 italic text-center mt-8">No players</div>
        ) : (
          players.map((p, idx) => (
            <PoolCard
              key={p.player_id}
              player={p}
              rank={startRank + idx}
              tier={assignments[p.player_id] || null}
              isComparing={compareList.includes(p.player_id)}
              onDragStart={setDraggingId}
              onDragEnd={() => {
                setDraggingId(null);
                setHoverTier(null);
              }}
              onClickHistory={onViewHistory}
              onToggleCompare={onToggleCompare}
              onClickName={(pl) => onOpenTeam(pl.team, pl.player_id)}
            />
          ))
        )}
      </div>
    </aside>
  );

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

      {/* THREE-COLUMN LAYOUT: pool-left | tier rows | pool-right */}
      <div className="grid gap-3 grid-cols-1 lg:grid-cols-[260px_1fr_260px]">
        <PoolPanel
          label="Pool · Top half"
          players={leftPool}
          panelRef={leftRef}
          otherRef={rightRef}
          startRank={1}
        />
        <div className="space-y-2">
          {loadingPool ? (
            <div className="flex flex-col items-center py-16 opacity-60">
              <div className="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin" />
              <p className="text-sm font-bold text-slate-400 mt-3">Loading {position} pool...</p>
            </div>
          ) : (
            TIERS.filter((t) => t !== 'UNRANKED').map((tier) => (
              <TierRow
                key={tier}
                tier={tier}
                players={tierBuckets[tier]}
                isHover={hoverTier === tier}
                onDragOver={setHoverTier}
                onDrop={moveToTier}
                onRemove={removeFromTier}
                onClickHistory={onViewHistory}
                onToggleCompare={onToggleCompare}
                onClickName={(p) => onOpenTeam(p.team, p.player_id)}
                compareList={compareList}
              />
            ))
          )}
          <p className="text-[10px] text-slate-400 dark:text-slate-500 text-center italic mt-2">
            Drag from a side panel into a tier. Click <X size={10} className="inline" /> on a tier
            card to remove.
          </p>
        </div>
        <PoolPanel
          label="Pool · Bottom half"
          players={rightPool}
          panelRef={rightRef}
          otherRef={leftRef}
          startRank={leftPool.length + 1}
        />
      </div>

      {/* INSIGHTS — moved BELOW tier rows */}
      <div className="mt-6">
        <TierVisualizations pool={filteredPool} assignments={assignments} position={position} />
      </div>
    </div>
  );
};

export default TierListView;
