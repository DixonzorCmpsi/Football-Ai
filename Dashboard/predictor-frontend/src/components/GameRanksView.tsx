import React, { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  StickyNote,
  Plus,
  Search,
  Activity,
} from 'lucide-react';
import { RankBoardSkeleton } from './Skeleton';
import MatchupBanner from './MatchupBanner';
import type { ScheduleGame } from '../hooks/useNflData';
import type { PlayerData } from '../types';
import { sizedPlayerImage } from '../utils/playerImage';

// ───────────────────────────────────────── Config: five start/sit tiers

const RANK_TIERS = ['MUST_START', 'FEELS_GOOD', 'W_FLEX', 'SHAKY_FLEX', 'RATHER_NOT'] as const;
type RankTier = (typeof RANK_TIERS)[number];

interface TierMeta {
  label: string;
  chip: string;
  ring: string;
}

const TIER_META: Record<RankTier, TierMeta> = {
  MUST_START: {
    label: 'Must Start',
    chip: 'bg-gradient-to-br from-emerald-400 to-green-600 text-white',
    ring: 'border-emerald-400 dark:border-emerald-500',
  },
  FEELS_GOOD: {
    label: 'Feels Good',
    chip: 'bg-gradient-to-br from-sky-400 to-blue-600 text-white',
    ring: 'border-sky-400 dark:border-sky-500',
  },
  W_FLEX: {
    label: 'W Flex',
    chip: 'bg-gradient-to-br from-amber-400 to-yellow-500 text-white',
    ring: 'border-amber-400 dark:border-amber-500',
  },
  SHAKY_FLEX: {
    label: 'Shaky Flex',
    chip: 'bg-gradient-to-br from-stone-400 to-stone-600 text-white',
    ring: 'border-stone-400 dark:border-stone-500',
  },
  RATHER_NOT: {
    label: 'Rather Not',
    chip: 'bg-gradient-to-br from-rose-400 to-red-600 text-white',
    ring: 'border-rose-400 dark:border-rose-500',
  },
};

type Position = 'ALL' | 'QB' | 'RB' | 'WR' | 'TE';
const POSITIONS: Position[] = ['ALL', 'QB', 'RB', 'WR', 'TE'];
const SKILL_POSITIONS: PlayerData['position'][] = ['QB', 'RB', 'WR', 'TE'];

// ───────────────────────────────────────── Persistence helpers

const STORAGE_KEY = 'gameRanks.state.v1';

// The fields RankedPlayerCard actually reads. We snapshot these when a player
// is placed into a tier so the week-wide board can still render a player whose
// game is no longer the one loaded, without refetching every matchup.
export type RankedPlayerMeta = Pick<
  PlayerData,
  | 'player_id' | 'player_name' | 'team' | 'position' | 'image' | 'opponent'
  | 'prediction' | 'average_points' | 'floor_prediction' | 'snap_percentage'
  | 'anytime_td_prob' | 'prop_line' | 'rec_line' | 'overunder' | 'spread'
  | 'injury_status'
>;

const pickMeta = (p: PlayerData): RankedPlayerMeta => ({
  player_id: p.player_id, player_name: p.player_name, team: p.team,
  position: p.position, image: p.image, opponent: p.opponent,
  prediction: p.prediction, average_points: p.average_points,
  floor_prediction: p.floor_prediction, snap_percentage: p.snap_percentage,
  anytime_td_prob: p.anytime_td_prob, prop_line: p.prop_line,
  rec_line: p.rec_line, overunder: p.overunder, spread: p.spread,
  injury_status: p.injury_status,
});

interface GameRankState {
  notes: string;
  assignments: Record<string, RankTier>;
  // Snapshot of each ranked player's display fields, keyed by player_id. Lets
  // the tier board show players carried over from other games in the week.
  playerMeta?: Record<string, RankedPlayerMeta>;
  // Manually-set display order within each tier (player_ids). Only touched by
  // drag actions — never auto-resorted — so a card dropped between two others
  // stays exactly where the user put it.
  tierOrder?: Partial<Record<RankTier, string[]>>;
}

export const gameKey = (week: number, away: string, home: string) => `${week}__${away}_@_${home}`;

function loadAll(): Record<string, GameRankState> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) return JSON.parse(raw);
  } catch {
    /* ignore */
  }
  return {};
}

// ───────────────────────────────────────── Player Card Visuals

const StatBubble = ({ label, value, color }: { label: string; value: string | number; color: string }) => (
  <div className="flex flex-col items-center justify-center w-8 h-8 rounded-full bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 shadow-sm">
    <span className="text-[7px] uppercase font-black text-slate-400 leading-none mb-0.5">{label}</span>
    <span className={`text-[10px] font-black leading-none ${color}`}>{value}</span>
  </div>
);

const RankedPlayerCard = memo<{
  player: PlayerData;
  tier: RankTier | null;
  isComparing: boolean;
  onDragStart: (id: string) => void;
  onDragEnd: () => void;
  onAssign: (id: string, tier: RankTier | null) => void;
  onToggleCompare: (id: string) => void;
  onOpenHistory?: (id: string) => void;
  onDropBefore?: (id: string) => void;
}>(({ player, tier, isComparing, onDragStart, onDragEnd, onAssign, onToggleCompare, onOpenHistory, onDropBefore }) => {
  return (
    <div
      draggable
      data-testid="ranked-card"
      data-player-team={player.team}
      data-player-name={player.player_name}
      onDragStart={(e) => {
        e.dataTransfer.setData('text/plain', player.player_id);
        e.dataTransfer.effectAllowed = 'move';
        onDragStart(player.player_id);
      }}
      onDragEnd={onDragEnd}
      onDragOver={
        onDropBefore
          ? (e) => {
              e.preventDefault();
              e.stopPropagation();
              e.dataTransfer.dropEffect = 'move';
            }
          : undefined
      }
      onDrop={
        onDropBefore
          ? (e) => {
              e.preventDefault();
              e.stopPropagation();
              onDropBefore(player.player_id);
            }
          : undefined
      }
      className="relative w-full max-w-[200px] cursor-grab active:cursor-grabbing bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 shadow-md overflow-hidden transition-all hover:ring-2 hover:ring-blue-500/50"
    >
      <div className="relative h-32 bg-slate-100 dark:bg-slate-800 flex items-end justify-center overflow-hidden">
        <div className="absolute top-2 left-2 px-1.5 py-0.5 rounded bg-black/60 backdrop-blur-md text-white text-[9px] font-bold">
          {player.team}
        </div>
        <img src={sizedPlayerImage(player.image, 64)} alt={player.player_name} loading="lazy" className="h-full object-cover object-top" decoding="async" />
      </div>

      <div className="p-3 space-y-3">
        <div className="text-center">
          {onOpenHistory ? (
            <button
              onClick={(e) => {
                e.stopPropagation();
                onOpenHistory(player.player_id);
              }}
              className="text-sm font-black text-slate-800 dark:text-slate-100 truncate hover:text-blue-600 dark:hover:text-blue-400 hover:underline"
              title={`View ${player.player_name} history`}
            >
              {player.player_name}
            </button>
          ) : (
            <h4 className="text-sm font-black text-slate-800 dark:text-slate-100 truncate">{player.player_name}</h4>
          )}
          <div className="flex items-center justify-center gap-2 mt-1">
            <span className="text-[10px] font-bold text-slate-500 dark:text-slate-400 uppercase">
              SPREAD {player.spread !== null ? (player.spread > 0 ? `+${player.spread}` : player.spread) : 'N/A'}
            </span>
            <span className="w-1 h-1 rounded-full bg-slate-300 dark:bg-slate-600" />
            <span className="text-[10px] font-bold text-slate-500 dark:text-slate-400 uppercase">
              OU {player.overunder || 'N/A'}
            </span>
          </div>
        </div>

        <div className="grid grid-cols-4 gap-1 justify-center">
          {/* Exactly 0.0 on prediction/floor/avg means the model has no features
              for this player/week yet (common pre-kickoff), not a real projected
              zero — show "no data" honestly instead of a misleading "0.0". */}
          <StatBubble label="FPS" value={player.prediction ? player.prediction.toFixed(1) : '—'} color="text-emerald-500" />
          <StatBubble label="FLOOR" value={player.floor_prediction ? player.floor_prediction.toFixed(1) : '—'} color="text-blue-500" />
          <StatBubble label="AVG" value={player.average_points ? player.average_points.toFixed(1) : '—'} color="text-amber-500" />
          <StatBubble
            label="SNAP%"
            value={player.snap_percentage != null ? `${Math.round(player.snap_percentage)}` : '—'}
            color="text-rose-500"
          />
        </div>

        <div className="space-y-1 border-t border-slate-100 dark:border-slate-800 pt-2">
          <div className="text-[9px] font-black text-center text-slate-400 uppercase tracking-widest mb-1">Player Props</div>
          <div className="flex justify-between items-center text-[10px]">
            <span className="text-slate-500 dark:text-slate-400">Main line:</span>
            <span className="font-bold text-slate-800 dark:text-slate-200">{player.prop_line ?? '—'}</span>
          </div>
          <div className="flex justify-between items-center text-[10px]">
            <span className="text-slate-500 dark:text-slate-400">Receptions:</span>
            <span className="font-bold text-slate-800 dark:text-slate-200">{player.rec_line ?? '—'}</span>
          </div>
          <div className="flex justify-between items-center text-[10px]">
            <span className="text-slate-500 dark:text-slate-400">Any TD:</span>
            <span className="font-bold text-emerald-500">{player.anytime_td_prob ? `${Math.round(player.anytime_td_prob)}%` : '—'}</span>
          </div>
        </div>

        <div className="flex gap-2">
          <button
            onClick={() => onToggleCompare(player.player_id)}
            className={`flex-1 py-1 rounded-md text-[10px] font-bold transition ${
              isComparing ? 'bg-blue-600 text-white' : 'bg-slate-100 dark:bg-slate-800 text-slate-500 hover:bg-slate-200'
            }`}
          >
            {isComparing ? 'Added' : 'Compare'}
          </button>
          {tier && (
            <button
              onClick={() => onAssign(player.player_id, null)}
              className="px-2 py-1 rounded-md bg-slate-100 dark:bg-slate-800 text-slate-500 hover:text-red-500"
              title="Remove from tier"
            >
              <Plus size={12} className="rotate-45" />
            </button>
          )}
        </div>
      </div>
    </div>
  );
});
RankedPlayerCard.displayName = 'RankedPlayerCard';

// ───────────────────────────────────────── Practice Report Component

const PracticeReport: React.FC<{
  matchup: any;
  selectedTeam: string;
}> = ({ matchup, selectedTeam }) => {
  const injuries = selectedTeam === 'home' ? matchup.home_injuries : matchup.away_injuries;

  const getStatusColor = (status: string) => {
    const s = status.toLowerCase();
    if (s.includes('out') || s.includes('ir')) return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400';
    if (s.includes('doubtful')) return 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400';
    if (s.includes('questionable')) return 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400';
    return 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400';
  };

  return (
    <div className="mt-6 border-t border-slate-200 dark:border-slate-700 pt-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-black text-slate-800 dark:text-slate-100">Practice Report — {selectedTeam === 'home' ? 'Home' : 'Away'}</h3>
        <span className="text-[10px] text-slate-400">Feed checked 47m ago · no change since last data load</span>
      </div>
      <div className="overflow-x-auto rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900">
        <table className="w-full text-left text-xs">
          <thead className="bg-slate-50 dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700">
            <tr>
              <th className="px-3 py-2 font-black text-slate-500 dark:text-slate-400">Player Name</th>
              <th className="px-3 py-2 font-black text-slate-500 dark:text-slate-400">Pos</th>
              <th className="px-3 py-2 font-black text-slate-500 dark:text-slate-400">Status</th>
              <th className="px-3 py-2 font-black text-slate-500 dark:text-slate-400">Injury</th>
              {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map(day => (
                <th key={day} className="px-3 py-2 font-black text-slate-500 dark:text-slate-400">{day}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
            {injuries && injuries.length > 0 ? injuries.map((p: any, i: number) => (
              <tr key={i} className="hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                <td className="px-3 py-2 font-bold text-slate-800 dark:text-slate-200">{p.name}</td>
                <td className="px-3 py-2 text-slate-500 dark:text-slate-400">{p.position}</td>
                <td className="px-3 py-2">
                  <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${getStatusColor(p.status)}`}>
                    {p.status}
                  </span>
                </td>
                <td className="px-3 py-2 text-slate-500 dark:text-slate-400">{p.avg_snaps ? `${p.avg_snaps} snaps` : '-'}</td>
                {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map(day => (
                  <td key={day} className="px-3 py-2 text-center text-slate-400">-</td>
                ))}
              </tr>
            )) : (
              <tr>
                <td colSpan={11} className="px-3 py-8 text-center text-slate-400 italic">No injuries reported for this team</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};


interface TierDropProps {
  tier: RankTier;
  players: PlayerData[];
  isHover: boolean;
  onDragOver: (tier: RankTier | null) => void;
  onDrop: (tier: RankTier) => void;
  onDropBefore: (tier: RankTier, beforeId: string) => void;
  onDragStart: (id: string) => void;
  onDragEnd: () => void;
  onAssign: (id: string, tier: RankTier | null) => void;
  onToggleCompare: (id: string) => void;
  onOpenHistory: (id: string) => void;
  compareSet: Set<string>;
}

const TierDrop: React.FC<TierDropProps> = memo(
  ({ tier, players, isHover, onDragOver, onDrop, onDropBefore, onDragStart, onDragEnd, onAssign, onToggleCompare, onOpenHistory, compareSet }) => {
    const meta = TIER_META[tier];
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
        data-testid="tier-row"
        data-tier={tier}
        data-count={players.length}
        className={`flex items-stretch gap-2 rounded-xl border-2 p-2 min-h-[9rem] shrink-0 transition-colors ${
          isHover ? 'bg-blue-50 dark:bg-blue-900/20 border-blue-400 dark:border-blue-500 shadow-inner' : `bg-white dark:bg-slate-900 ${meta.ring}`
        }`}
      >
        <div className={`w-16 sm:w-20 shrink-0 rounded-lg flex flex-col items-center justify-center font-black ${meta.chip}`}>
          <span className="text-2xl sm:text-3xl leading-none">{RANK_TIERS.indexOf(tier) + 1}</span>
          <span className="text-[8px] mt-1 px-1 text-center uppercase tracking-wider opacity-90 hidden sm:block">
            {meta.label}
          </span>
          <span className="text-[10px] mt-0.5 font-mono opacity-80">{players.length}</span>
        </div>
        <div className="flex-1 flex flex-wrap items-start gap-2">
          {players.length === 0 ? (
            <div className="flex-1 flex items-center justify-center text-xs font-bold text-slate-300 dark:text-slate-600 italic">
              Drop players here
            </div>
          ) : (
            players.map((p) => (
              <RankedPlayerCard
                key={p.player_id}
                player={p}
                tier={tier}
                isComparing={compareSet.has(p.player_id)}
                onDragStart={onDragStart}
                onDragEnd={onDragEnd}
                onAssign={onAssign}
                onToggleCompare={onToggleCompare}
                onOpenHistory={onOpenHistory}
                onDropBefore={(beforeId) => onDropBefore(tier, beforeId)}
              />
            ))
          )}
        </div>
      </div>
    );
  },
);
TierDrop.displayName = 'TierDrop';

// ───────────────────────────────────────── Single game card

interface GameCardProps {
  game: ScheduleGame;
  week: number;
  expanded: boolean;
  onToggleExpand: () => void;
  onToggleCompare: (id: string) => void;
  onOpenHistory: (id: string) => void;
  compareList: string[];
  // When the caller (e.g. the matchup drill-through's Rank tab) already fetched
  // this exact matchup's rosters/weather, pass them here to skip the redundant
  // re-fetch — this is what made the Rank tab feel slow to open.
  preloadedMatchup?: FetchedMatchup | null;
  // When the caller already renders a MatchupBanner with team logos, weather,
  // and game-script above this card (the drill-through's Rank tab), skip this
  // card's own banner entirely instead of showing a duplicate.
  bannerless?: boolean;
}

export interface GameWeather {
  is_dome: boolean;
  roof: string;
  temp_f: number | null;
  wind_mph: number | null;
  condition: string;
  precip_chance: number | null;
}

export interface TeamRankMetric {
  label: string;
  value: number;
  rank: number;
  rank_out_of: number;
  lower_is_better: boolean;
}

export interface TeamSideRankings {
  metrics: Record<string, TeamRankMetric>;
  average_rank: number;
  overall?: TeamRankMetric;
}

export interface TeamRankings {
  season: number;
  source: string;
  offense: TeamSideRankings | null;
  defense: TeamSideRankings | null;
}

export interface GameScript {
  tag: 'SHOOTOUT' | 'GRIND_IT_OUT' | 'BLOWOUT_RISK' | 'BALANCED';
  label: string;
  summary: string;
  home_implied_total: number | null;
  away_implied_total: number | null;
  home_strength_note: string | null;
  away_strength_note: string | null;
}

export interface FetchedMatchup {
  home_roster: PlayerData[];
  away_roster: PlayerData[];
  weather: GameWeather | null;
  home_rankings?: TeamRankings | null;
  away_rankings?: TeamRankings | null;
  game_script?: GameScript | null;
  over_under?: number | null;
  spread?: number | null;
  home_win_prob?: number | null;
  away_win_prob?: number | null;
}

// Highest-projection-first, with recent snap share as a tiebreak so likely
// starters (who may have a thinner prediction sample) still sort ahead of
// clearly-buried bench players with the same rounded projection.
const byPriority = (a: PlayerData, b: PlayerData) =>
  (b.prediction || 0) - (a.prediction || 0) || (b.snap_percentage || 0) - (a.snap_percentage || 0);

const rankPlayersByPts = (roster: PlayerData[], allowed: Set<PlayerData['position']>) =>
  roster.filter((p) => allowed.has(p.position)).sort(byPriority);

export const GameCard: React.FC<GameCardProps> = ({ game, week, expanded, onToggleCompare, onOpenHistory, compareList, preloadedMatchup, bannerless }) => {
  const away = game.away_team;
  const home = game.home_team;
  const stateKey = gameKey(week, away, home);

  const [localAll, setLocalAll] = useState<Record<string, GameRankState>>(() => loadAll());
  const [matchup, setMatchup] = useState<FetchedMatchup | null>(preloadedMatchup ?? null);
  const [loadingMatchup, setLoadingMatchup] = useState(false);
  const [dragId, setDragId] = useState<string | null>(null);
  const [hoverTier, setHoverTier] = useState<RankTier | null>(null);
  const [position, setPosition] = useState<Position>('WR');
  const [search, setSearch] = useState('');
  const [reportTeam, setReportTeam] = useState<'away' | 'home'>('away');
  const [reloadToken, setReloadToken] = useState(0);

  const gState = localAll[stateKey] || { notes: '', assignments: {} };
  const notes = gState.notes;
  const assignments = gState.assignments;
  const tierOrder = gState.tierOrder || {};

  // Writes into a specific game's slice. Needed because the tier board now
  // shows players carried over from other matchups in the week — unassigning
  // one of those has to update the game that actually owns it, not the game
  // currently on screen.
  const commitTo = useCallback(
    (key: string, updater: (prev: GameRankState) => GameRankState) => {
      setLocalAll((prevAll) => {
        const nextAll = { ...prevAll };
        const prev = nextAll[key] || { notes: '', assignments: {} };
        nextAll[key] = updater(prev);
        return nextAll;
      });
    },
    [],
  );

  const commit = useCallback(
    (updater: (prev: GameRankState) => GameRankState) => commitTo(stateKey, updater),
    [commitTo, stateKey],
  );

  const setNotes = (text: string) => commit((prev) => ({ ...prev, notes: text }));

  // Single entry point for every tier mutation — keeps membership (assignments)
  // and manual display order (tierOrder) in sync in one commit. `tier: null`
  // unassigns. `beforeId: null` appends to the end of the tier.
  const placePlayer = useCallback(
    (tier: RankTier | null, playerId: string, beforeId: string | null) => {
      commit((prev) => {
        const nextAssignments = { ...prev.assignments };
        const nextMeta = { ...(prev.playerMeta || {}) };
        if (tier === null) {
          delete nextAssignments[playerId];
          delete nextMeta[playerId];
        } else {
          nextAssignments[playerId] = tier;
          // Snapshot on assign so this player stays renderable once the user
          // moves on to a different matchup.
          const src = playersByIdRef.current.get(playerId);
          if (src) nextMeta[playerId] = pickMeta(src);
        }

        const prevOrder = prev.tierOrder || {};
        const nextOrder: Partial<Record<RankTier, string[]>> = {};
        RANK_TIERS.forEach((t) => {
          nextOrder[t] = (prevOrder[t] || []).filter((id) => id !== playerId);
        });
        if (tier !== null) {
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
        return { ...prev, assignments: nextAssignments, tierOrder: nextOrder, playerMeta: nextMeta };
      });
    },
    [commit],
  );

  const assignTo = useCallback(
    (tier: RankTier) => {
      if (!dragId) return;
      placePlayer(tier, dragId, null);
      setDragId(null);
      setHoverTier(null);
    },
    [dragId, placePlayer],
  );

  const assignToBefore = useCallback(
    (tier: RankTier, beforeId: string) => {
      if (!dragId) return;
      placePlayer(tier, dragId, beforeId);
      setDragId(null);
      setHoverTier(null);
    },
    [dragId, placePlayer],
  );

  const unassign = useCallback(
    (id: string) => {
      const owner = weekRankedRef.current[id]?.fromKey;
      if (owner && owner !== stateKey) {
        // Player belongs to a different matchup in this week — clear them there.
        commitTo(owner, (prev) => {
          const nextAssignments = { ...prev.assignments };
          delete nextAssignments[id];
          const nextMeta = { ...(prev.playerMeta || {}) };
          delete nextMeta[id];
          const prevOrder = prev.tierOrder || {};
          const nextOrder: Partial<Record<RankTier, string[]>> = {};
          RANK_TIERS.forEach((t) => { nextOrder[t] = (prevOrder[t] || []).filter((x) => x !== id); });
          return { ...prev, assignments: nextAssignments, tierOrder: nextOrder, playerMeta: nextMeta };
        });
        return;
      }
      placePlayer(null, id, null);
    },
    [placePlayer, commitTo, stateKey],
  );

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(localAll));
    } catch {
      /* ignore */
    }
  }, [localAll]);

  // Auto-fill tiers when the card first expands and has no assignments yet.
  useEffect(() => {
    if (!expanded || assignments && Object.keys(assignments).length > 0) return;
    if (matchup) {
      // skip auto-draft; user does it explicitly
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expanded, matchup]);

  useEffect(() => {
    if (!expanded) return;
    if (matchup) return;
    // Skip the initial fetch when preloaded data was handed to us — but once the
    // user explicitly hits "Refresh players & props" (which bumps reloadToken and
    // clears matchup), always fetch fresh regardless of what was preloaded.
    if (preloadedMatchup && reloadToken === 0) return;
    setLoadingMatchup(true);
    let active = true;
    import('../lib/api').then(({ fetchMatchup }) =>
      fetchMatchup(week, home, away)
        .then((d) => {
          if (!active) return;
          setMatchup(
            d
              ? {
                  home_roster: Array.isArray(d.home_roster) ? d.home_roster : [],
                  away_roster: Array.isArray(d.away_roster) ? d.away_roster : [],
                  weather: d.weather ?? null,
                  home_rankings: d.home_rankings ?? null,
                  away_rankings: d.away_rankings ?? null,
                  game_script: d.game_script ?? null,
                  over_under: d.over_under ?? null,
                  spread: d.spread ?? null,
                  home_win_prob: d.home_win_prob ?? null,
                  away_win_prob: d.away_win_prob ?? null,
                }
              : { home_roster: [], away_roster: [], weather: null },
          );
        })
        .catch(() => {
          if (active) setMatchup({ home_roster: [], away_roster: [], weather: null });
        })
        .finally(() => {
          if (active) setLoadingMatchup(false);
        }),
    ).catch(() => {
      if (active) setLoadingMatchup(false);
    });
    return () => {
      active = false;
    };
  }, [expanded, week, home, away, matchup, reloadToken, preloadedMatchup]);

  const filteredMatchup = useMemo(() => {
    if (!matchup) return matchup;
    const allowed = new Set<PlayerData['position']>(position === 'ALL' ? SKILL_POSITIONS : [position]);
    // Merge both rosters and re-sort by priority — a combine-then-sort keeps the
    // higher-projected/likelier starter on top regardless of which team they're on.
    let players = [...matchup.away_roster, ...matchup.home_roster]
      .filter((p) => allowed.has(p.position))
      .sort(byPriority);
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      players = players.filter((p) => p.player_name.toLowerCase().includes(q) || p.team.toLowerCase().includes(q));
    }
    return { away_roster: rankPlayersByPts(matchup.away_roster, allowed), home_roster: rankPlayersByPts(matchup.home_roster, allowed), players };
  }, [matchup, position, search]);

  // playersById spans the FULL roster (not the position/search-filtered list) so
  // a player stays visible in their tier even while the pool is filtered down.
  const playersById = useMemo(() => {
    const map = new Map<string, PlayerData>();
    if (matchup) {
      [...matchup.away_roster, ...matchup.home_roster].forEach((p) => map.set(p.player_id, p));
    }
    return map;
  }, [matchup]);

  // placePlayer needs the live roster to snapshot a player on assign, but must
  // not be re-created every time the roster changes (it is a drag handler on
  // every card), so read it through a ref.
  const playersByIdRef = useRef(playersById);
  useEffect(() => { playersByIdRef.current = playersById; }, [playersById]);

  // ── Week-wide view ──────────────────────────────────────────────────────
  // Tier rows show every player ranked anywhere in this week, not just the
  // matchup currently loaded, so rankings accumulate as the user works through
  // the slate. The Unranked pool below stays scoped to the selected game.
  const weekPrefix = `${week}__`;
  const weekRanked = useMemo(() => {
    const out: Record<string, { tier: RankTier; player: PlayerData; fromKey: string }> = {};
    Object.entries(localAll).forEach(([key, st]) => {
      if (!key.startsWith(weekPrefix) || !st) return;
      const meta = st.playerMeta || {};
      Object.entries(st.assignments || {}).forEach(([pid, tier]) => {
        // Prefer the live roster object for the game on screen (fresher
        // projections/injury status); fall back to the stored snapshot.
        const live = key === stateKey ? playersById.get(pid) : undefined;
        const snap = meta[pid];
        const player = (live || snap) as PlayerData | undefined;
        if (player) out[pid] = { tier, player, fromKey: key };
      });
    });
    return out;
  }, [localAll, weekPrefix, stateKey, playersById]);

  const weekRankedRef = useRef(weekRanked);
  useEffect(() => { weekRankedRef.current = weekRanked; }, [weekRanked]);

  const weekAssignments = useMemo(() => {
    const a: Record<string, RankTier> = {};
    Object.entries(weekRanked).forEach(([pid, v]) => { a[pid] = v.tier; });
    return a;
  }, [weekRanked]);

  // Pool stays scoped to the selected matchup, but a player ranked in an
  // earlier game must not reappear here as unranked.
  const unranked = filteredMatchup ? filteredMatchup.players.filter((p) => !weekAssignments[p.player_id]) : [];

  const buckets = useMemo(() => {
    const b: Record<RankTier, PlayerData[]> = { MUST_START: [], FEELS_GOOD: [], W_FLEX: [], SHAKY_FLEX: [], RATHER_NOT: [] };
    // Manual drag order is stored per game, so walk this game's order first to
    // keep the user's exact placement, then append everyone else ranked this
    // week (other games) after it.
    RANK_TIERS.forEach((tier) => {
      const seen = new Set<string>();
      const ordered: PlayerData[] = [];
      (tierOrder[tier] || []).forEach((id) => {
        const entry = weekRanked[id];
        if (entry && entry.tier === tier && !seen.has(id)) {
          seen.add(id);
          ordered.push(entry.player);
        }
      });
      const rest = Object.values(weekRanked)
        .filter((e) => e.tier === tier && !seen.has(e.player.player_id))
        // Current game first so the matchup you are working on stays on top.
        .sort((x, y) => {
          const cx = x.fromKey === stateKey ? 0 : 1;
          const cy = y.fromKey === stateKey ? 0 : 1;
          if (cx !== cy) return cx - cy;
          return (y.player.prediction || 0) - (x.player.prediction || 0);
        })
        .map((e) => e.player);
      b[tier] = [...ordered, ...rest];
    });
    return b;
  }, [tierOrder, weekRanked, stateKey]);

  const handleAutoTier = () => {
    if (!filteredMatchup) return;
    const ranked = filteredMatchup.players;
    const next: Record<string, RankTier> = {};
    const nextOrder: Partial<Record<RankTier, string[]>> = {
      MUST_START: [],
      FEELS_GOOD: [],
      W_FLEX: [],
      SHAKY_FLEX: [],
      RATHER_NOT: [],
    };
    ranked.forEach((p, i) => {
      const pct = (i + 1) / ranked.length;
      let tier: RankTier;
      if (pct <= 0.2) tier = 'MUST_START';
      else if (pct <= 0.45) tier = 'FEELS_GOOD';
      else if (pct <= 0.7) tier = 'W_FLEX';
      else if (pct <= 0.88) tier = 'SHAKY_FLEX';
      else tier = 'RATHER_NOT';
      next[p.player_id] = tier;
      nextOrder[tier]!.push(p.player_id);
    });
    // Snapshot display data for every auto-tiered player too, not just ones
    // placed by drag — otherwise they vanish from the week board the moment the
    // user switches to another matchup.
    const nextMeta: Record<string, RankedPlayerMeta> = {};
    ranked.forEach((p) => { nextMeta[p.player_id] = pickMeta(p); });
    commit((prev) => ({
      ...prev,
      assignments: { ...prev.assignments, ...next },
      tierOrder: nextOrder,
      playerMeta: { ...(prev.playerMeta || {}), ...nextMeta },
    }));
  };

  const compareSet = useMemo(() => new Set(compareList), [compareList]);
  const totalPlayers = filteredMatchup ? filteredMatchup.players.length : 0;
  const rankedCount = Object.keys(assignments).length;
  const weekRankedCount = Object.keys(weekRanked).length;

  // Full-week export: every player ranked across every matchup, in tier order.
  const exportWeekCsv = useCallback(() => {
    const esc = (v: unknown) => {
      const str = v == null ? '' : String(v);
      return /[",\n]/.test(str) ? '"' + str.replace(/"/g, '""') + '"' : str;
    };
    const rows: string[][] = [[
      'week', 'tier', 'tier_rank', 'player', 'position', 'team', 'opponent',
      'matchup', 'projection', 'avg_points', 'injury_status',
    ]];
    RANK_TIERS.forEach((tier) => {
      buckets[tier].forEach((pl, i) => {
        const entry = weekRanked[pl.player_id];
        const g = (entry ? entry.fromKey : '').slice(String(week).length + 2).replace('_@_', ' @ ');
        rows.push([
          String(week), TIER_META[tier].label, String(i + 1), pl.player_name,
          pl.position || '', pl.team || '', pl.opponent || '', g,
          String(pl.prediction != null ? pl.prediction : ''),
          String(pl.average_points != null ? pl.average_points : ''),
          pl.injury_status || '',
        ]);
      });
    });
    const csv = rows.map((r) => r.map(esc).join(',')).join('\n');
    const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8;' }));
    const a = document.createElement('a');
    a.href = url;
    a.download = 'week-' + week + '-ranks.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, [buckets, weekRanked, week]);

  return (
    <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm overflow-hidden">
      {/* The caller (matchup drill-through) already renders a full banner with
          logos, weather, and game script above this card when bannerless — no
          need to repeat it here. */}
      {!bannerless && (
        <MatchupBanner
          matchup={`${away} @ ${home}`}
          gameTime={game?.gametime}
          gameDay={game?.gameday}
          overUnder={matchup?.over_under ?? game?.game_total ?? null}
          spread={matchup?.spread ?? null}
          homeWinProb={matchup?.home_win_prob ?? null}
          awayWinProb={matchup?.away_win_prob ?? null}
          weather={matchup?.weather ?? null}
          gameScript={matchup?.game_script ?? null}
        />
      )}

      {expanded && (
        <div className="border-t border-slate-200 dark:border-slate-700 p-4 space-y-4">
          {/* Controls */}
          <div className="flex flex-wrap items-center gap-2">
            <div className="flex items-center gap-1 bg-slate-100 dark:bg-slate-800/60 p-1 rounded-lg border border-slate-200/60 dark:border-slate-700/60">
              {POSITIONS.map((p) => (
                <button
                  key={p}
                  onClick={() => setPosition(p)}
                  className={`px-3 py-1 rounded-md text-[11px] font-black transition-all ${
                    position === p
                      ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm'
                      : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'
                  }`}
                >
                  {p}
                </button>
              ))}
            </div>
            <div className="relative">
              <Search size={11} className="absolute left-2 top-1/2 -translate-y-1/2 text-slate-400" />
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Filter players…"
                className="pl-7 pr-2 py-1.5 rounded-lg text-xs font-medium bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 outline-none focus:border-blue-500 w-40"
              />
            </div>
            <button
              onClick={() => {
                handleAutoTier();
              }}
              className="flex items-center gap-1 px-3 py-1.5 rounded-lg text-[11px] font-bold bg-purple-100 text-purple-700 hover:bg-purple-200 dark:bg-purple-900/30 dark:text-purple-300"
              title="Place every shown player into the five start/sit tiers by projected points"
            >
              Auto-tier
            </button>
            <button
              onClick={() => commit((prev) => ({ ...prev, assignments: {}, tierOrder: {}, playerMeta: {} }))}
              className="px-3 py-1.5 rounded-lg text-[11px] font-bold bg-slate-100 text-slate-600 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-300"
              title="Clears only this matchup's rankings - other games in the week are untouched"
            >
              Clear this game
            </button>
            <button
              onClick={exportWeekCsv}
              disabled={weekRankedCount === 0}
              className="px-3 py-1.5 rounded-lg text-[11px] font-bold bg-emerald-100 text-emerald-700 hover:bg-emerald-200 disabled:opacity-40 disabled:cursor-not-allowed dark:bg-emerald-900/30 dark:text-emerald-300"
              title="Download every player you have ranked this week as a CSV"
            >
              Export week CSV
            </button>
            <button
              onClick={() => {
                setMatchup(null);
                setReloadToken((value) => value + 1);
              }}
              className="px-3 py-1.5 rounded-lg text-[11px] font-bold bg-blue-600 text-white hover:bg-blue-700"
            >
              Refresh players & props
            </button>
            <div className="ml-auto flex items-center gap-2 text-[10px] text-slate-400">
              <Activity size={11} />
              <span>
                {loadingMatchup
                  ? 'Loading rosters…'
                  : `${totalPlayers} in game · ${rankedCount} here · ${weekRankedCount} ranked in week ${week}`}
              </span>
            </div>
          </div>

          {loadingMatchup ? (
            <RankBoardSkeleton />
          ) : !filteredMatchup || filteredMatchup.players.length === 0 ? (
            <div className="py-10 text-center">
              <p className="text-sm font-black text-slate-300 dark:text-slate-600">NO PLAYERS</p>
              <p className="text-xs text-slate-400 mt-1">
                Roster data unavailable for {position === 'ALL' ? 'skill positions' : position}.
              </p>
            </div>
          ) : (
            <div className="grid gap-3 lg:grid-cols-[260px_1fr]">
              {/* Unranked pool — side panel */}
              <aside className="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 flex flex-col overflow-hidden lg:h-[70vh]">
                <div className="flex items-center justify-between px-3 py-2 border-b border-slate-100 dark:border-slate-800 bg-slate-50/60 dark:bg-slate-800/40">
                  <h4 className="text-[10px] font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
                    Unranked Players
                  </h4>
                  <span className="text-[10px] font-mono text-slate-400">{unranked.length}</span>
                </div>
                <div className="flex-1 overflow-y-auto overscroll-contain p-2 space-y-2 scrollbar-thin">
                  {unranked.length === 0 ? (
                    <div className="text-[10px] text-slate-400 italic text-center mt-6">All ranked — clear a tier to add more</div>
                  ) : (
                    unranked.map((p) => (
                      <RankedPlayerCard
                        key={p.player_id}
                        player={p}
                        tier={null}
                        isComparing={compareSet.has(p.player_id)}
                        onDragStart={setDragId}
                        onDragEnd={() => {
                          setDragId(null);
                          setHoverTier(null);
                        }}
                        onAssign={unassign}
                        onToggleCompare={onToggleCompare}
                        onOpenHistory={onOpenHistory}
                      />
                    ))
                  )}
                </div>
              </aside>

              {/* Tier rows — horizontal, stacked. Same fixed height + independent
                  scroll as the pool panel beside it, so the two columns always
                  read as one matched-height board instead of the tier column
                  trailing off shorter (or taller) than the pool. */}
              <div className="lg:h-[70vh] overflow-y-auto overscroll-contain flex flex-col gap-2 pr-1 scrollbar-thin">
                {RANK_TIERS.map((tier) => (
                  <TierDrop
                    key={tier}
                    tier={tier}
                    players={buckets[tier]}
                    isHover={hoverTier === tier}
                    onDragOver={setHoverTier}
                    onDrop={assignTo}
                    onDropBefore={assignToBefore}
                    onDragStart={setDragId}
                    onDragEnd={() => {
                      setDragId(null);
                      setHoverTier(null);
                    }}
                    onAssign={unassign}
                    onToggleCompare={onToggleCompare}
                    onOpenHistory={onOpenHistory}
                    compareSet={compareSet}
                  />
                ))}
              </div>
            </div>
          )}

          {/* Notes — de-emphasized, below the board */}
          <div className="rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50/60 dark:bg-slate-800/40 p-3">
            <div className="flex items-center gap-1.5 mb-1.5">
              <StickyNote size={12} className="text-slate-400" />
              <span className="text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-500">
                Notes
              </span>
            </div>
            <textarea
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              placeholder="Optional: Vegas lines, injuries, weather, game script, etc…"
              rows={2}
              className="w-full resize-y rounded-md border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 px-3 py-2 text-xs text-slate-700 dark:text-slate-200 outline-none focus:border-blue-500 placeholder:text-slate-400"
            />
            <span className="text-[10px] text-slate-400 italic">Auto-saved to this browser</span>
          </div>

          {/* Practice Report — below the board */}
          {matchup && (
            <div className="rounded-lg border border-slate-200 dark:border-slate-700 p-3">
              <div className="mb-2 flex items-center justify-between gap-2">
                <span className="text-[10px] font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">Practice report</span>
                <div className="flex rounded-md bg-slate-100 p-0.5 dark:bg-slate-800">
                  {(['away', 'home'] as const).map((side) => (
                    <button
                      key={side}
                      onClick={() => setReportTeam(side)}
                      className={`px-2 py-1 text-[10px] font-bold rounded transition ${reportTeam === side ? 'bg-white text-blue-600 shadow-sm dark:bg-slate-700 dark:text-blue-300' : 'text-slate-500'}`}
                    >
                      {side === 'away' ? away : home}
                    </button>
                  ))}
                </div>
              </div>
              <PracticeReport matchup={matchup} selectedTeam={reportTeam} />
            </div>
          )}
        </div>
      )}
    </div>
  );
};

// ───────────────────────────────────────── Main view

interface GameRanksViewProps {
  games: ScheduleGame[];
  loadingSchedule: boolean;
  week: number | null;
  compareList: string[];
  onToggleCompare: (id: string) => void;
  onOpenHistory: (id: string) => void;
}

const GameRanksView: React.FC<GameRanksViewProps> = ({ games, loadingSchedule, week, compareList, onToggleCompare, onOpenHistory }) => {
  const [selectedGameKey, setSelectedGameKey] = useState<string>('');

  const firstKey = games.length > 0 ? gameKey(week || 0, games[0].away_team, games[0].home_team) : '';

  useEffect(() => {
    setSelectedGameKey(firstKey);
  }, [firstKey]);

  const selectedGame = games.find((game) => gameKey(week || 0, game.away_team, game.home_team) === selectedGameKey) || games[0];

  return (
    <div className="w-full space-y-3">
      <div className="flex flex-wrap items-end justify-between gap-3 mb-3">
        <div>
          <h2 className="text-xs font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest">
            Ranks · Start or Sit
          </h2>
          <h1 className="text-xl sm:text-2xl font-black text-slate-800 dark:text-slate-100 mt-1">
            Week {week} Game Situations &amp; Tiers
          </h1>
          <p className="text-xs text-slate-400 dark:text-slate-500 mt-1">
            Select a matchup, then drag each skill player into the start/sit tier that fits the game situation.
          </p>
        </div>
        {games.length > 0 && (
          <label className="text-xs font-bold text-slate-500 dark:text-slate-400">
            Game
            <select
              value={selectedGameKey}
              onChange={(event) => setSelectedGameKey(event.target.value)}
              className="ml-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-black text-slate-700 outline-none focus:border-blue-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100"
            >
              {games.map((game) => {
                const key = gameKey(week || 0, game.away_team, game.home_team);
                return <option key={key} value={key}>{game.away_team} @ {game.home_team}</option>;
              })}
            </select>
          </label>
        )}
      </div>

      {/* Tier legend */}
      <div className="flex flex-wrap items-center gap-2">
        {RANK_TIERS.map((t) => (
          <span key={t} className={`flex items-center gap-1.5 px-2 py-1 rounded-md ${TIER_META[t].chip}`}>
            <span className="text-xs font-black">{t}</span>
            <span className="text-[10px] font-semibold">{TIER_META[t].label}</span>
          </span>
        ))}
        <span className="text-[10px] text-slate-400 italic ml-auto flex items-center gap-1">
          Drag from Unranked into a tier · auto-saves locally
        </span>
      </div>

      {loadingSchedule ? (
        <div className="flex flex-col items-center justify-center py-16 opacity-60">
          <div className="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin" />
          <p className="text-sm font-bold text-slate-400 mt-3">Loading Week {week} schedule…</p>
        </div>
      ) : games.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-16 opacity-70">
          <p className="text-xl font-black text-slate-300 dark:text-slate-600 mb-2">NO GAMES</p>
          <p className="text-sm text-slate-400">No schedule data for Week {week}.</p>
        </div>
      ) : selectedGame ? (
        <GameCard
          // Keyed off the game actually being shown (already resolved via the
          // games[0] fallback above) rather than the `selectedGameKey` state,
          // which starts at '' and only catches up a render later — keying on
          // that caused an extra, avoidable remount (and its extra fetch) on
          // every load.
          key={gameKey(week || 0, selectedGame.away_team, selectedGame.home_team)}
          game={selectedGame}
          week={week || 0}
          expanded
          onToggleExpand={() => undefined}
          onToggleCompare={onToggleCompare}
          onOpenHistory={onOpenHistory}
          compareList={compareList}
        />
      ) : null}
    </div>
  );
};

// Mounted for the whole session and hidden with display:none so local state
// (scroll position, drag assignments, filters) survives navigation. Without
// memo, any App state change re-renders this entire tree while it is off-screen.
export default memo(GameRanksView);
