import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { CSSProperties, DragEvent } from 'react';
import axios from 'axios';
import { ChevronDown, ChevronRight, GripVertical, PanelLeftClose, PanelLeftOpen, Plus, Save, Search, Trash2, X, RotateCcw, UserPlus } from 'lucide-react';
import type { OffensePlayer } from './TeamOffenseModal';
import { sizedPlayerImage } from '../utils/playerImage';

const API_BASE_URL =
  typeof window !== 'undefined' && window.__env && window.__env.API_BASE_URL
    ? window.__env.API_BASE_URL
    : '/api';

type PositionFilter = 'ALL' | 'QB' | 'RB' | 'WR' | 'TE' | 'OL';
type SortMode = 'rank' | 'name' | 'team' | 'position';
type BuilderSlotId = 'qb' | 'rb' | 'wr_left' | 'wr_slot' | 'wr_right' | 'te' | 'ol_1' | 'ol_2' | 'ol_3' | 'ol_4' | 'ol_5';
type SaveStatus = 'idle' | 'saved' | 'deleted' | 'error';

interface TeamBuilderData {
  qb: OffensePlayer[];
  rb: OffensePlayer[];
  wr: OffensePlayer[];
  te: OffensePlayer[];
  ol: OffensePlayer[];
}

interface SelectedSlot {
  id: BuilderSlotId;
  option: number;
}

type BuilderAssignments = Record<BuilderSlotId, Array<OffensePlayer | null>>;
type ExpandedSlots = Partial<Record<BuilderSlotId, boolean>>;

interface DragPayload {
  type: 'pool' | 'assignment';
  playerId: string;
  from?: SelectedSlot;
}

interface SavedAssignment {
  playerId: string;
  snapshot: OffensePlayer;
}

type SavedAssignments = Record<BuilderSlotId, Array<SavedAssignment | null>>;

interface SavedRoster {
  id: string;
  name: string;
  team: string;
  createdAt: string;
  updatedAt: string;
  assignments: SavedAssignments;
  expandedSlots?: ExpandedSlots;
}

interface Props {
  team: string;
  teamColor: string;
  teamData: TeamBuilderData;
  onOpenDetail: (player: OffensePlayer) => void;
}

const POSITION_FILTERS: PositionFilter[] = ['ALL', 'QB', 'RB', 'WR', 'TE', 'OL'];
const TEAM_BUILDER_STORAGE_KEY = 'teamBuilder.savedRosters.v1';
const SLOT_IDS: BuilderSlotId[] = ['qb', 'rb', 'wr_left', 'wr_slot', 'wr_right', 'te', 'ol_1', 'ol_2', 'ol_3', 'ol_4', 'ol_5'];

const SLOT_META: Record<BuilderSlotId, { label: string; accept: PositionFilter; sub?: string }> = {
  qb: { label: 'QB', accept: 'QB', sub: 'Quarterback' },
  rb: { label: 'RB', accept: 'RB', sub: 'Next to QB' },
  wr_left: { label: 'WR X', accept: 'WR', sub: 'Far left' },
  wr_slot: { label: 'Slot WR', accept: 'WR', sub: 'Inside right' },
  wr_right: { label: 'WR Z', accept: 'WR', sub: 'Far right' },
  te: { label: 'TE', accept: 'TE', sub: 'Tight end side' },
  ol_1: { label: 'LT', accept: 'OL', sub: 'Left tackle' },
  ol_2: { label: 'LG', accept: 'OL', sub: 'Left guard' },
  ol_3: { label: 'C', accept: 'OL', sub: 'Center' },
  ol_4: { label: 'RG', accept: 'OL', sub: 'Right guard' },
  ol_5: { label: 'RT', accept: 'OL', sub: 'Right tackle' },
};

const emptyAssignments = (): BuilderAssignments => ({
  qb: [null, null, null],
  rb: [null, null, null],
  wr_left: [null, null, null],
  wr_slot: [null, null, null],
  wr_right: [null, null, null],
  te: [null, null, null],
  ol_1: [null, null, null],
  ol_2: [null, null, null],
  ol_3: [null, null, null],
  ol_4: [null, null, null],
  ol_5: [null, null, null],
});

const emptySavedAssignments = (): SavedAssignments => ({
  qb: [null, null, null],
  rb: [null, null, null],
  wr_left: [null, null, null],
  wr_slot: [null, null, null],
  wr_right: [null, null, null],
  te: [null, null, null],
  ol_1: [null, null, null],
  ol_2: [null, null, null],
  ol_3: [null, null, null],
  ol_4: [null, null, null],
  ol_5: [null, null, null],
});

const readSavedRosters = (): SavedRoster[] => {
  if (typeof window === 'undefined') return [];
  try {
    const raw = window.localStorage.getItem(TEAM_BUILDER_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter((item) => item?.id && item?.assignments) : [];
  } catch {
    return [];
  }
};

const makeRosterId = () => {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) return crypto.randomUUID();
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
};

const serializeAssignments = (assignments: BuilderAssignments): SavedAssignments => {
  const saved = emptySavedAssignments();
  SLOT_IDS.forEach((slotId) => {
    saved[slotId] = assignments[slotId].map((player) =>
      player ? { playerId: player.player_id, snapshot: player } : null,
    );
  });
  return saved;
};

const hydrateAssignments = (
  saved: SavedAssignments,
  playersById: Map<string, OffensePlayer>,
): BuilderAssignments => {
  const next = emptyAssignments();
  SLOT_IDS.forEach((slotId) => {
    const savedOptions = saved?.[slotId] || [];
    next[slotId] = next[slotId].map((_, option) => {
      const cell = savedOptions[option];
      if (!cell) return null;
      const player = playersById.get(cell.playerId) || cell.snapshot || null;
      return player && isCompatible({ id: slotId, option }, player) ? player : null;
    });
  });
  return next;
};

const defaultRosterName = (team: string) => `${team} build`;

const rankLabel = (p: OffensePlayer) => {
  if (!p.last_season_rank_position || !p.last_season_position_rank) return null;
  return `${p.last_season_rank_position}${p.last_season_position_rank}`;
};

const playerPositionFilter = (p: OffensePlayer): PositionFilter => {
  const group = p.position_group;
  if (group === 'qb') return 'QB';
  if (group === 'rb') return 'RB';
  if (group === 'wr') return 'WR';
  if (group === 'te') return 'TE';
  if (group === 'ol') return 'OL';
  return 'ALL';
};

const isCompatible = (slot: SelectedSlot | null, player: OffensePlayer) => {
  if (!slot) return false;
  return SLOT_META[slot.id].accept === playerPositionFilter(player);
};

const sameSlot = (a: SelectedSlot | null | undefined, b: SelectedSlot | null | undefined) => {
  return Boolean(a && b && a.id === b.id && a.option === b.option);
};

const optionLabel = (option: number) => String.fromCharCode(65 + option);

const pickStarter = (players: OffensePlayer[]) => players.find((p) => p.is_starter) || players[0] || null;
const sortDepth = (players: OffensePlayer[]) =>
  [...players].sort((a, b) => (a.is_starter === b.is_starter ? 0 : a.is_starter ? -1 : 1));

const buildInitialAssignments = (teamData: TeamBuilderData): BuilderAssignments => {
  const next = emptyAssignments();
  const qbs = sortDepth(teamData.qb);
  const ol = sortDepth(teamData.ol);
  const usedOl = new Set<string>();
  const pickOl = (preferred: string[]) => {
    const normalized = preferred.map((pos) => pos.toUpperCase());
    const loosePreferred = normalized.filter((pos) => pos.length > 1);
    const exact = ol.find((p) => !usedOl.has(p.player_id) && normalized.includes(String(p.position || '').toUpperCase()));
    const loose = ol.find((p) => !usedOl.has(p.player_id) && loosePreferred.some((pos) => String(p.position || '').toUpperCase().includes(pos)));
    const player = exact || loose || ol.find((p) => !usedOl.has(p.player_id)) || null;
    if (player) usedOl.add(player.player_id);
    return player;
  };

  next.qb[0] = qbs[0] || pickStarter(teamData.qb);
  next.qb[1] = qbs[1] || null;
  next.qb[2] = qbs[2] || null;
  next.ol_1[0] = pickOl(['LT', 'T', 'OT', 'TACKLE']);
  next.ol_2[0] = pickOl(['LG', 'G', 'OG', 'GUARD']);
  next.ol_3[0] = pickOl(['C', 'CENTER']);
  next.ol_4[0] = pickOl(['RG', 'G', 'OG', 'GUARD']);
  next.ol_5[0] = pickOl(['RT', 'T', 'OT', 'TACKLE']);
  return next;
};

const MiniPlayer = ({
  player,
  onOpenDetail,
  onClear,
  onDragStart,
  onDragEnd,
}: {
  player: OffensePlayer | null;
  onOpenDetail: (player: OffensePlayer) => void;
  onClear: () => void;
  onDragStart?: (player: OffensePlayer, event: DragEvent<HTMLDivElement>) => void;
  onDragEnd?: () => void;
}) => {
  if (!player) return null;
  const finish = rankLabel(player);
  return (
    <div
      draggable={Boolean(onDragStart)}
      onDragStart={(event) => onDragStart?.(player, event)}
      onDragEnd={onDragEnd}
      className="flex items-center gap-2 min-w-0 rounded-lg bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 px-2 py-1.5 cursor-grab active:cursor-grabbing"
    >
      <button type="button" onClick={() => onOpenDetail(player)} className="flex items-center gap-2 min-w-0 flex-1 text-left">
        <div className="w-7 h-7 rounded-full bg-slate-100 dark:bg-slate-700 overflow-hidden shrink-0">
          {player.image && <img src={sizedPlayerImage(player.image, 32)} alt={player.player_name} className="w-full h-full object-cover" decoding="async" />}
        </div>
        <div className="min-w-0">
          <div className="text-xs font-black text-slate-800 dark:text-slate-100 truncate">{player.player_name}</div>
          <div className="text-[9px] font-mono text-slate-400 dark:text-slate-500 truncate">
            {player.team} · {player.position}{finish ? ` · ${finish}` : ''}
          </div>
        </div>
      </button>
      <button
        type="button"
        onClick={onClear}
        className="w-6 h-6 rounded-md flex items-center justify-center text-slate-400 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20"
        aria-label="Remove player"
      >
        <X size={12} />
      </button>
    </div>
  );
};

const BuilderSlot = ({
  id,
  assignments,
  selectedSlot,
  setSelectedSlot,
  assignPlayer,
  onOpenDetail,
  draggingPlayer,
  dragOverSlot,
  setDragOverSlot,
  onDropPlayer,
  onDragStartAssigned,
  onDragEnd,
  expandedSlots,
  toggleSlotExpanded,
  className = '',
}: {
  id: BuilderSlotId;
  assignments: BuilderAssignments;
  selectedSlot: SelectedSlot | null;
  setSelectedSlot: (slot: SelectedSlot) => void;
  assignPlayer: (slot: SelectedSlot, player: OffensePlayer | null) => void;
  onOpenDetail: (player: OffensePlayer) => void;
  draggingPlayer: OffensePlayer | null;
  dragOverSlot: SelectedSlot | null;
  setDragOverSlot: (slot: SelectedSlot | null) => void;
  onDropPlayer: (slot: SelectedSlot, event: DragEvent<HTMLDivElement>) => void;
  onDragStartAssigned: (player: OffensePlayer, slot: SelectedSlot, event: DragEvent<HTMLDivElement>) => void;
  onDragEnd: () => void;
  expandedSlots: ExpandedSlots;
  toggleSlotExpanded: (id: BuilderSlotId) => void;
  className?: string;
}) => {
  const meta = SLOT_META[id];
  const options = assignments[id];
  const expanded = Boolean(expandedSlots[id]);
  const optionIndexes = expanded ? options.map((_, index) => index) : [0];
  const extraCount = options.slice(1).filter(Boolean).length;

  return (
    <div className={`rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50/95 dark:bg-slate-900/80 p-2 shadow-sm ${className}`}>
      <div className="flex items-center justify-between gap-2 mb-2">
        <div>
          <div className="text-[10px] font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">{meta.label}</div>
          {meta.sub && <div className="text-[9px] font-bold uppercase tracking-widest text-slate-400 dark:text-slate-500">{meta.sub}</div>}
        </div>
        <button
          type="button"
          onClick={() => toggleSlotExpanded(id)}
          className="h-7 px-2 rounded-md bg-white dark:bg-slate-950/40 border border-slate-200 dark:border-slate-700 text-[9px] font-black uppercase tracking-widest text-slate-500 dark:text-slate-400 flex items-center gap-1 hover:text-blue-600 dark:hover:text-blue-400"
        >
          {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          {extraCount ? `+${extraCount}` : 'A/B/C'}
        </button>
      </div>
      <div className="space-y-1.5">
        {optionIndexes.map((option) => {
          const player = options[option];
          const slot = { id, option };
          const active = sameSlot(selectedSlot, slot);
          const isDragOver = sameSlot(dragOverSlot, slot);
          const acceptsDrag = draggingPlayer ? isCompatible(slot, draggingPlayer) : false;
          const stateClass = isDragOver && draggingPlayer
            ? acceptsDrag
              ? 'ring-2 ring-emerald-400/70 bg-emerald-500/10'
              : 'ring-2 ring-red-400/70 bg-red-500/10'
            : active
              ? 'ring-2 ring-blue-500/40'
              : '';

          return (
            <div
              key={`${id}-${option}`}
              className={`rounded-lg transition ${stateClass}`}
              onDragEnter={() => draggingPlayer && setDragOverSlot(slot)}
              onDragOver={(event) => {
                if (!draggingPlayer) return;
                event.preventDefault();
                event.dataTransfer.dropEffect = acceptsDrag ? 'move' : 'none';
              }}
              onDragLeave={(event) => {
                const related = event.relatedTarget;
                if (related instanceof Node && event.currentTarget.contains(related)) return;
                if (sameSlot(dragOverSlot, slot)) setDragOverSlot(null);
              }}
              onDrop={(event) => {
                event.preventDefault();
                setDragOverSlot(null);
                onDropPlayer(slot, event);
              }}
            >
              {expanded && (
                <div className="mb-1 text-[9px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-500">
                  Option {optionLabel(option)}
                </div>
              )}
              {player ? (
                <MiniPlayer
                  player={player}
                  onOpenDetail={onOpenDetail}
                  onClear={() => assignPlayer({ id, option }, null)}
                  onDragStart={(draggedPlayer, event) => onDragStartAssigned(draggedPlayer, slot, event)}
                  onDragEnd={onDragEnd}
                />
              ) : (
                <button
                  type="button"
                  onClick={() => setSelectedSlot({ id, option })}
                  className="w-full min-h-[3.1rem] rounded-lg border border-dashed border-slate-300 dark:border-slate-700 bg-white/70 dark:bg-slate-950/30 text-[10px] font-black uppercase tracking-widest text-slate-400 hover:text-blue-600 hover:border-blue-400 dark:hover:text-blue-400 transition"
                >
                  Add {meta.accept}
                </button>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
};

const PlayerPoolCard = memo(({
  player,
  selectedSlot,
  onAssign,
  onOpenDetail,
  onDragStart,
  onDragEnd,
}: {
  player: OffensePlayer;
  selectedSlot: SelectedSlot | null;
  onAssign: (player: OffensePlayer) => void;
  onOpenDetail: (player: OffensePlayer) => void;
  onDragStart: (player: OffensePlayer, event: DragEvent<HTMLDivElement>) => void;
  onDragEnd: () => void;
}) => {
  const compatible = isCompatible(selectedSlot, player);
  const finish = rankLabel(player);
  return (
    <div
      draggable
      onDragStart={(event) => onDragStart(player, event)}
      onDragEnd={onDragEnd}
      className="rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 p-2 flex items-center gap-2 cursor-grab active:cursor-grabbing"
    >
      <button type="button" onClick={() => onOpenDetail(player)} className="flex items-center gap-2 min-w-0 flex-1 text-left">
        <div className="w-8 h-8 rounded-full overflow-hidden bg-slate-100 dark:bg-slate-700 shrink-0">
          {player.image && <img src={sizedPlayerImage(player.image, 32)} alt={player.player_name} className="w-full h-full object-cover" loading="lazy" decoding="async" />}
        </div>
        <div className="min-w-0">
          <div className="flex items-center gap-1 min-w-0">
            <span className="text-xs font-black text-slate-800 dark:text-slate-100 truncate">{player.player_name}</span>
            {player.is_starter && <span className="text-[8px] font-black px-1 rounded bg-green-500 text-white">ST</span>}
          </div>
          <div className="text-[9px] font-mono text-slate-400 dark:text-slate-500 truncate">
            {player.position} · {player.team}{finish ? ` · ${finish}` : ''}
          </div>
        </div>
      </button>
      <button
        type="button"
        disabled={!compatible}
        onClick={() => onAssign(player)}
        className={`w-7 h-7 rounded-md flex items-center justify-center transition ${
          compatible
            ? 'bg-blue-600 text-white hover:bg-blue-700'
            : 'bg-slate-100 dark:bg-slate-800 text-slate-300 dark:text-slate-600 cursor-not-allowed'
        }`}
        title={compatible ? 'Add to selected slot' : 'Select a matching slot first'}
      >
        <Plus size={14} />
      </button>
    </div>
  );
});
PlayerPoolCard.displayName = 'PlayerPoolCard';

const TeamBuilderView = ({ team, teamColor, teamData, onOpenDetail }: Props) => {
  const [pool, setPool] = useState<OffensePlayer[]>([]);
  const [loadingPool, setLoadingPool] = useState(false);
  const [poolError, setPoolError] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [positionFilter, setPositionFilter] = useState<PositionFilter>('ALL');
  const [teamFilter, setTeamFilter] = useState(team);
  const [sortMode, setSortMode] = useState<SortMode>('rank');
  const [selectedSlot, setSelectedSlotRaw] = useState<SelectedSlot | null>({ id: 'qb', option: 0 });
  const [assignments, setAssignments] = useState<BuilderAssignments>(() => buildInitialAssignments(teamData));
  const [expandedSlots, setExpandedSlots] = useState<ExpandedSlots>({});
  const [savedRosters, setSavedRosters] = useState<SavedRoster[]>(readSavedRosters);
  const [activeSavedRosterId, setActiveSavedRosterId] = useState<string>('');
  const [rosterName, setRosterName] = useState(defaultRosterName(team));
  const [saveStatus, setSaveStatus] = useState<SaveStatus>('idle');
  const [dragPayload, setDragPayload] = useState<DragPayload | null>(null);
  const [draggingPlayer, setDraggingPlayer] = useState<OffensePlayer | null>(null);
  const [dragOverSlot, setDragOverSlot] = useState<SelectedSlot | null>(null);
  const [sidePanelCollapsed, setSidePanelCollapsed] = useState(false);
  const [sidePanelWidth, setSidePanelWidth] = useState(320);
  const [resizingPanel, setResizingPanel] = useState(false);
  const panelRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    setAssignments(buildInitialAssignments(teamData));
    setExpandedSlots({});
    setActiveSavedRosterId('');
    setRosterName(defaultRosterName(team));
    setSaveStatus('idle');
    setTeamFilter(team);
    setSelectedSlotRaw({ id: 'qb', option: 0 });
  }, [team, teamData]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      window.localStorage.setItem(TEAM_BUILDER_STORAGE_KEY, JSON.stringify(savedRosters));
    } catch {
      setSaveStatus('error');
    }
  }, [savedRosters]);

  useEffect(() => {
    if (saveStatus === 'idle') return;
    const timeout = window.setTimeout(() => setSaveStatus('idle'), 2200);
    return () => window.clearTimeout(timeout);
  }, [saveStatus]);

  useEffect(() => {
    if (!resizingPanel) return;
    const previousCursor = document.body.style.cursor;
    const previousUserSelect = document.body.style.userSelect;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    const onMove = (event: MouseEvent) => {
      const left = panelRef.current?.getBoundingClientRect().left ?? 0;
      const nextWidth = Math.round(event.clientX - left);
      setSidePanelWidth(Math.min(520, Math.max(260, nextWidth)));
    };
    const onUp = () => setResizingPanel(false);

    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
    return () => {
      document.body.style.cursor = previousCursor;
      document.body.style.userSelect = previousUserSelect;
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    };
  }, [resizingPanel]);

  useEffect(() => {
    let active = true;
    setLoadingPool(true);
    setPoolError(null);
    axios
      .get(`${API_BASE_URL}/team-builder/players`)
      .then((res) => {
        if (active) setPool(res.data || []);
      })
      .catch((err) => {
        if (active) setPoolError(err.message || 'Failed to load players');
      })
      .finally(() => {
        if (active) setLoadingPool(false);
      });
    return () => {
      active = false;
    };
  }, []);

  const teams = useMemo(() => {
    const unique = Array.from(new Set(pool.map((p) => p.team).filter(Boolean))).sort();
    return unique.includes(team) ? unique : [team, ...unique].sort();
  }, [pool, team]);

  // Read-latest ref so the pool's "assign" button gets a stable callback
  // and memoized PlayerPoolCards don't re-render when `selectedSlot` changes.
  const selectedSlotRef = useRef(selectedSlot);
  useEffect(() => { selectedSlotRef.current = selectedSlot; }, [selectedSlot]);
  const handlePoolAssign = useCallback((player: OffensePlayer) => {
    const slot = selectedSlotRef.current;
    if (slot) assignPlayer(slot, player);
  }, []);

  const playersById = useMemo(() => {
    const map = new Map<string, OffensePlayer>();
    const add = (player: OffensePlayer | null | undefined) => {
      if (player?.player_id) map.set(player.player_id, player);
    };
    pool.forEach(add);
    [...teamData.qb, ...teamData.rb, ...teamData.wr, ...teamData.te, ...teamData.ol].forEach(add);
    Object.values(assignments).flat().forEach(add);
    savedRosters.forEach((roster) => {
      SLOT_IDS.forEach((slotId) => roster.assignments?.[slotId]?.forEach((cell) => add(cell?.snapshot)));
    });
    return map;
  }, [pool, teamData, assignments, savedRosters]);

  const setSelectedSlot = (slot: SelectedSlot) => {
    setSelectedSlotRaw(slot);
    setPositionFilter(SLOT_META[slot.id].accept);
  };

  const assignPlayer = useCallback((slot: SelectedSlot, player: OffensePlayer | null) => {
    setAssignments((prev) => {
      const next = { ...prev, [slot.id]: [...prev[slot.id]] };
      next[slot.id][slot.option] = player;
      return next;
    });
  }, []);

  const toggleSlotExpanded = (id: BuilderSlotId) => {
    setExpandedSlots((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  const saveRoster = (saveAsNew = false) => {
    const now = new Date().toISOString();
    const name = rosterName.trim() || defaultRosterName(team);
    const id = !saveAsNew && activeSavedRosterId ? activeSavedRosterId : makeRosterId();
    const existing = savedRosters.find((roster) => roster.id === id);
    const nextRoster: SavedRoster = {
      id,
      name,
      team,
      createdAt: existing?.createdAt || now,
      updatedAt: now,
      assignments: serializeAssignments(assignments),
      expandedSlots,
    };

    setSavedRosters((prev) => {
      const others = prev.filter((roster) => roster.id !== id);
      return [nextRoster, ...others].sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
    });
    setActiveSavedRosterId(id);
    setRosterName(name);
    setSaveStatus('saved');
  };

  const loadSavedRoster = (id: string) => {
    setActiveSavedRosterId(id);
    const roster = savedRosters.find((item) => item.id === id);
    if (!roster) return;
    setAssignments(hydrateAssignments(roster.assignments, playersById));
    setExpandedSlots(roster.expandedSlots || {});
    setRosterName(roster.name);
    setTeamFilter(roster.team || team);
    setSelectedSlot({ id: 'qb', option: 0 });
  };

  const deleteSavedRoster = () => {
    if (!activeSavedRosterId) return;
    setSavedRosters((prev) => prev.filter((roster) => roster.id !== activeSavedRosterId));
    setActiveSavedRosterId('');
    setRosterName(defaultRosterName(team));
    setSaveStatus('deleted');
  };

  const clearDragState = useCallback(() => {
    setDragPayload(null);
    setDraggingPlayer(null);
    setDragOverSlot(null);
  }, []);

  const setDragData = (event: DragEvent, payload: DragPayload) => {
    const serialized = JSON.stringify(payload);
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('application/json', serialized);
    event.dataTransfer.setData('text/plain', serialized);
  };

  const startPoolDrag = useCallback((player: OffensePlayer, event: DragEvent<HTMLDivElement>) => {
    const payload: DragPayload = { type: 'pool', playerId: player.player_id };
    setDragPayload(payload);
    setDraggingPlayer(player);
    setDragData(event, payload);
  }, []);

  const startAssignmentDrag = (player: OffensePlayer, slot: SelectedSlot, event: DragEvent<HTMLDivElement>) => {
    const payload: DragPayload = { type: 'assignment', playerId: player.player_id, from: slot };
    setDragPayload(payload);
    setDraggingPlayer(player);
    setDragData(event, payload);
  };

  const findPlayerById = (playerId: string) => {
    return (
      pool.find((p) => p.player_id === playerId) ||
      Object.values(assignments).flat().find((p) => p?.player_id === playerId) ||
      null
    );
  };

  const getDropPayload = (event: DragEvent<HTMLDivElement>) => {
    if (dragPayload) return dragPayload;
    const raw = event.dataTransfer.getData('application/json') || event.dataTransfer.getData('text/plain');
    if (!raw) return null;
    try {
      return JSON.parse(raw) as DragPayload;
    } catch {
      return null;
    }
  };

  const handleSlotDrop = (slot: SelectedSlot, event: DragEvent<HTMLDivElement>) => {
    const payload = getDropPayload(event);
    const player = payload ? findPlayerById(payload.playerId) : null;
    if (!payload || !player || !isCompatible(slot, player)) {
      clearDragState();
      return;
    }

    setSelectedSlot(slot);
    setAssignments((prev) => {
      const source = payload.from;
      if (source && sameSlot(source, slot)) return prev;

      const next: BuilderAssignments = { ...prev };
      const ensureList = (id: BuilderSlotId) => {
        if (next[id] === prev[id]) next[id] = [...prev[id]];
        return next[id];
      };

      const targetList = ensureList(slot.id);
      const targetPlayer = targetList[slot.option];

      if (source) {
        const sourceList = ensureList(source.id);
        sourceList[source.option] = targetPlayer && isCompatible(source, targetPlayer) ? targetPlayer : null;
      }

      if (!source) {
        (Object.keys(next) as BuilderSlotId[]).forEach((slotId) => {
          const list = ensureList(slotId);
          list.forEach((existing, index) => {
            if (existing?.player_id === player.player_id && !sameSlot({ id: slotId, option: index }, slot)) {
              list[index] = null;
            }
          });
        });
      }

      targetList[slot.option] = player;
      return next;
    });
    clearDragState();
  };

  const resetToTeam = () => {
    setAssignments(buildInitialAssignments(teamData));
    setExpandedSlots({});
    setActiveSavedRosterId('');
    setRosterName(defaultRosterName(team));
    setTeamFilter(team);
    setSelectedSlot({ id: 'qb', option: 0 });
  };

  const filteredPool = useMemo(() => {
    const q = search.trim().toLowerCase();
    const selectedPlayerIds = new Set(Object.values(assignments).flat().filter(Boolean).map((p) => p!.player_id));
    const filtered = pool.filter((p) => {
      if (selectedPlayerIds.has(p.player_id)) return false;
      if (positionFilter !== 'ALL' && playerPositionFilter(p) !== positionFilter) return false;
      if (teamFilter !== 'ALL' && p.team !== teamFilter) return false;
      if (q && !`${p.player_name} ${p.team} ${p.position}`.toLowerCase().includes(q)) return false;
      return true;
    });

    filtered.sort((a, b) => {
      if (sortMode === 'team') return `${a.team}${a.player_name}`.localeCompare(`${b.team}${b.player_name}`);
      if (sortMode === 'position') return `${playerPositionFilter(a)}${a.player_name}`.localeCompare(`${playerPositionFilter(b)}${b.player_name}`);
      if (sortMode === 'name') return String(a.player_name || '').localeCompare(String(b.player_name || ''));
      return (
        (a.last_season_position_rank || 9999) - (b.last_season_position_rank || 9999) ||
        (a.is_starter === b.is_starter ? 0 : a.is_starter ? -1 : 1) ||
        String(a.player_name || '').localeCompare(String(b.player_name || ''))
      );
    });
    return filtered.slice(0, 140);
  }, [pool, assignments, positionFilter, teamFilter, search, sortMode]);

  const selectedLabel = selectedSlot ? `${SLOT_META[selectedSlot.id].label} ${optionLabel(selectedSlot.option)}` : 'None';

  const builderSlotProps = {
    assignments,
    selectedSlot,
    setSelectedSlot,
    assignPlayer,
    onOpenDetail,
    draggingPlayer,
    dragOverSlot,
    setDragOverSlot,
    onDropPlayer: handleSlotDrop,
    onDragStartAssigned: startAssignmentDrag,
    onDragEnd: clearDragState,
    expandedSlots,
    toggleSlotExpanded,
  };
  const panelWidthStyle = {
    '--builder-panel-width': sidePanelCollapsed ? '3.5rem' : `${sidePanelWidth}px`,
  } as CSSProperties & Record<'--builder-panel-width', string>;

  return (
    <div className="flex flex-col xl:flex-row gap-4" style={panelWidthStyle}>
      <aside
        ref={panelRef}
        className={`relative w-full xl:w-[var(--builder-panel-width)] shrink-0 rounded-2xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 overflow-hidden ${sidePanelCollapsed ? 'min-h-[4rem]' : 'min-h-[34rem]'}`}
      >
        {sidePanelCollapsed ? (
          <div className="h-full min-h-[4rem] p-2 flex xl:flex-col items-center justify-between gap-2">
            <button
              type="button"
              onClick={() => setSidePanelCollapsed(false)}
              className="w-9 h-9 rounded-lg bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400 flex items-center justify-center hover:text-blue-600"
              title="Open players"
            >
              <PanelLeftOpen size={15} />
            </button>
            <div className="xl:[writing-mode:vertical-rl] text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-500">
              Players
            </div>
          </div>
        ) : (
          <>
            <div className="p-3 border-b border-slate-200 dark:border-slate-700">
              <div className="flex items-center justify-between gap-2 mb-3">
                <div>
                  <h3 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">Players</h3>
                  <div className="text-[10px] text-slate-400 dark:text-slate-500">Selected slot: {selectedLabel}</div>
                </div>
                <div className="flex items-center gap-1">
                  <button
                    type="button"
                    onClick={resetToTeam}
                    className="w-8 h-8 rounded-lg bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400 flex items-center justify-center hover:text-blue-600"
                    title="Reset to current team"
                  >
                    <RotateCcw size={14} />
                  </button>
                  <button
                    type="button"
                    onClick={() => setSidePanelCollapsed(true)}
                    className="w-8 h-8 rounded-lg bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400 flex items-center justify-center hover:text-blue-600"
                    title="Collapse players"
                  >
                    <PanelLeftClose size={14} />
                  </button>
                </div>
              </div>

              <div className="relative mb-2">
                <Search size={13} className="absolute left-2 top-1/2 -translate-y-1/2 text-slate-400" />
                <input
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search players"
                  className="w-full pl-7 pr-2 py-2 rounded-lg bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-xs font-bold text-slate-700 dark:text-slate-200 outline-none focus:border-blue-400"
                />
              </div>

              <div className="grid grid-cols-2 gap-2">
                <select value={positionFilter} onChange={(e) => setPositionFilter(e.target.value as PositionFilter)} className="rounded-lg bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 px-2 py-2 text-xs font-bold">
                  {POSITION_FILTERS.map((pos) => <option key={pos} value={pos}>{pos}</option>)}
                </select>
                <select value={teamFilter} onChange={(e) => setTeamFilter(e.target.value)} className="rounded-lg bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 px-2 py-2 text-xs font-bold">
                  <option value="ALL">ALL</option>
                  {teams.map((abbr) => <option key={abbr} value={abbr}>{abbr}</option>)}
                </select>
                <select value={sortMode} onChange={(e) => setSortMode(e.target.value as SortMode)} className="col-span-2 rounded-lg bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 px-2 py-2 text-xs font-bold">
                  <option value="rank">Sort: 2025 rank</option>
                  <option value="name">Sort: name</option>
                  <option value="team">Sort: team</option>
                  <option value="position">Sort: position</option>
                </select>
              </div>
            </div>

            <div className="p-3 space-y-2 max-h-[62vh] overflow-y-auto scrollbar-thin">
              {loadingPool ? (
                <div className="text-xs font-bold text-slate-400 text-center py-8">Loading players...</div>
              ) : poolError ? (
                <div className="text-xs font-bold text-red-500 text-center py-8">{poolError}</div>
              ) : filteredPool.length === 0 ? (
                <div className="text-xs font-bold text-slate-400 text-center py-8">No players match</div>
              ) : (
                filteredPool.map((player) => (
                  <PlayerPoolCard
                    key={player.player_id}
                    player={player}
                    selectedSlot={selectedSlot}
                    onAssign={handlePoolAssign}
                    onOpenDetail={onOpenDetail}
                    onDragStart={startPoolDrag}
                    onDragEnd={clearDragState}
                  />
                ))
              )}
            </div>

            <button
              type="button"
              onMouseDown={(event) => {
                event.preventDefault();
                setResizingPanel(true);
              }}
              className="hidden xl:flex absolute inset-y-0 -right-1 w-3 items-center justify-center text-slate-300 hover:text-blue-500 cursor-col-resize"
              title="Resize players"
            >
              <GripVertical size={14} />
            </button>
          </>
        )}
      </aside>

      <section
        className="flex-1 min-w-0 rounded-2xl border border-slate-200 dark:border-slate-700 bg-slate-50/80 dark:bg-slate-950/30 p-3 md:p-5 min-h-[34rem]"
        style={{
          borderTopColor: teamColor,
          borderTopWidth: 3,
          backgroundImage:
            'linear-gradient(90deg, rgba(148, 163, 184, 0.12) 1px, transparent 1px)',
          backgroundSize: '12.5% 100%',
        }}
      >
        <div className="flex items-center justify-between gap-3 mb-4">
          <div>
            <h3 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">Team Builder</h3>
            <div className="text-[10px] text-slate-400 dark:text-slate-500">Drag players into slots, or pick a slot and add from the panel.</div>
          </div>
          <div className="hidden sm:flex items-center gap-2 text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-500">
            <UserPlus size={13} />
            {team}
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-[minmax(12rem,1fr)_minmax(12rem,1fr)_auto] gap-2 mb-4">
          <input
            value={rosterName}
            onChange={(event) => setRosterName(event.target.value)}
            placeholder="Roster name"
            className="min-w-0 rounded-lg bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 px-3 py-2 text-xs font-bold text-slate-700 dark:text-slate-200 outline-none focus:border-blue-400"
          />
          <select
            value={activeSavedRosterId}
            onChange={(event) => loadSavedRoster(event.target.value)}
            className="min-w-0 rounded-lg bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 px-3 py-2 text-xs font-bold text-slate-700 dark:text-slate-200 outline-none focus:border-blue-400"
          >
            <option value="">Saved rosters</option>
            {savedRosters.map((roster) => (
              <option key={roster.id} value={roster.id}>
                {roster.name} ({roster.team})
              </option>
            ))}
          </select>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => saveRoster(false)}
              className="h-9 px-3 rounded-lg bg-blue-600 text-white text-[10px] font-black uppercase tracking-widest flex items-center gap-1.5 hover:bg-blue-700 transition"
              title="Save roster"
            >
              <Save size={13} />
              {saveStatus === 'saved' ? 'Saved' : 'Save'}
            </button>
            <button
              type="button"
              onClick={() => saveRoster(true)}
              className="h-9 w-9 rounded-lg bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400 flex items-center justify-center hover:text-blue-600"
              title="Save as new roster"
            >
              <Plus size={14} />
            </button>
            <button
              type="button"
              onClick={deleteSavedRoster}
              disabled={!activeSavedRosterId}
              className="h-9 w-9 rounded-lg bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400 flex items-center justify-center hover:text-red-600 disabled:opacity-40 disabled:cursor-not-allowed"
              title="Delete saved roster"
            >
              <Trash2 size={14} />
            </button>
            {saveStatus === 'deleted' && (
              <span className="text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-500">
                Deleted
              </span>
            )}
            {saveStatus === 'error' && (
              <span className="text-[10px] font-black uppercase tracking-widest text-red-500">
                Save error
              </span>
            )}
          </div>
        </div>

        <div className="grid grid-cols-1 gap-4">
          <div className="grid grid-cols-1 lg:grid-cols-9 gap-3 items-start">
            <BuilderSlot id="wr_left" {...builderSlotProps} className="lg:col-span-1" />
            <BuilderSlot id="te" {...builderSlotProps} className="lg:col-span-1" />
            <BuilderSlot id="ol_1" {...builderSlotProps} className="lg:col-span-1" />
            <BuilderSlot id="ol_2" {...builderSlotProps} className="lg:col-span-1" />
            <BuilderSlot id="ol_3" {...builderSlotProps} className="lg:col-span-1" />
            <BuilderSlot id="ol_4" {...builderSlotProps} className="lg:col-span-1" />
            <BuilderSlot id="ol_5" {...builderSlotProps} className="lg:col-span-1" />
            <BuilderSlot id="wr_slot" {...builderSlotProps} className="lg:col-span-1" />
            <BuilderSlot id="wr_right" {...builderSlotProps} className="lg:col-span-1" />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 max-w-2xl mx-auto w-full">
            <BuilderSlot id="qb" {...builderSlotProps} />
            <BuilderSlot id="rb" {...builderSlotProps} />
          </div>
        </div>
      </section>
    </div>
  );
};

export default TeamBuilderView;
