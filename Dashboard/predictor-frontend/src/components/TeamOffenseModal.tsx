import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { X, Plus, Check, History } from 'lucide-react';
import { getTeamColor } from '../utils/nflColors';
import PlayerDetailModal from './PlayerDetailModal';

const API_BASE_URL =
  typeof window !== 'undefined' && window.__env && window.__env.API_BASE_URL
    ? window.__env.API_BASE_URL
    : '/api';

export interface OffensePlayer {
  player_id: string;
  player_name: string;
  position: string;
  position_group: 'qb' | 'rb' | 'wr' | 'te' | 'ol';
  team: string;
  image?: string;
  injury_status?: string;
  is_rookie: boolean;
  draft_year?: number | null;
  draft_number?: number | null;
  age?: number | null;
  height?: number | null;
  weight?: number | null;
  college?: string | null;
  // latest-season aggregates (zero for rookies who haven't played yet)
  season_avg_pts: number;
  season_total_pts: number;
  recent_avg_pts: number;
  games_played: number;
  boom_games: number;
  bust_games: number;
  total_yds: number;
  total_tds: number;
  total_receptions: number;
  total_targets: number;
  total_carries: number;
  snaps_total: number;
  snap_pct_avg: number;
}

interface TeamOffenseResponse {
  team: string;
  qb: OffensePlayer[];
  rb: OffensePlayer[];
  wr: OffensePlayer[];
  te: OffensePlayer[];
  ol: OffensePlayer[];
}

interface Props {
  team: string;
  focusPlayerId?: string | null;
  onClose: () => void;
  compareList: string[];
  onToggleCompare: (id: string) => void;
  onViewHistory: (id: string) => void;
}

const GROUP_ORDER: Array<{ key: keyof Omit<TeamOffenseResponse, 'team'>; label: string }> = [
  { key: 'qb', label: 'QBs' },
  { key: 'rb', label: 'RBs' },
  { key: 'wr', label: 'WRs' },
  { key: 'te', label: 'TEs' },
  { key: 'ol', label: 'OLine' },
];

const PlayerRow: React.FC<{
  p: OffensePlayer;
  rank: number;
  groupTopSnap: number;
  groupTopPpg: number;
  isFocus: boolean;
  isComparing: boolean;
  onToggleCompare: (id: string) => void;
  onViewHistory: (id: string) => void;
  onOpenDetail: (p: OffensePlayer) => void;
}> = ({ p, rank, groupTopSnap, groupTopPpg, isFocus, isComparing, onToggleCompare, onViewHistory, onOpenDetail }) => {
  const isStarter = rank === 0 && (p.snap_pct_avg > 40 || p.games_played === 0);
  const snapShare = groupTopSnap > 0 ? Math.min(100, (p.snap_pct_avg / groupTopSnap) * 100) : 0;
  const ppgShare = groupTopPpg > 0 ? Math.min(100, (p.season_avg_pts / groupTopPpg) * 100) : 0;
  const status = (p.injury_status || '').toLowerCase();
  const statusBadge =
    status.includes('out') || status.includes('ir')
      ? { label: 'OUT', cls: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' }
      : status.includes('doubtful')
      ? { label: 'D', cls: 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400' }
      : status.includes('questionable')
      ? { label: 'Q', cls: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400' }
      : null;

  return (
    <div
      onClick={() => onOpenDetail(p)}
      className={`flex items-center gap-2 px-2 py-1.5 rounded-md border transition cursor-pointer ${
        isStarter ? 'border-l-4 border-l-green-500' : ''
      } ${
        isFocus
          ? 'bg-blue-50 dark:bg-blue-900/30 border-blue-400 dark:border-blue-500 ring-2 ring-blue-400/20'
          : 'bg-white dark:bg-slate-800 border-slate-200 dark:border-slate-700 hover:border-blue-300 dark:hover:border-blue-700 hover:bg-slate-50 dark:hover:bg-slate-800/80'
      }`}
    >
      <div className="w-7 h-7 rounded-full overflow-hidden bg-slate-100 dark:bg-slate-700 shrink-0 border border-slate-200 dark:border-slate-600">
        {p.image && (
          <img src={p.image} alt={p.player_name} className="w-full h-full object-cover" loading="lazy" />
        )}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-1 flex-wrap">
          <span className="text-xs font-bold text-slate-800 dark:text-slate-100 truncate hover:text-blue-600 dark:hover:text-blue-400">
            {p.player_name}
          </span>
          {isStarter && (
            <span className="text-[8px] font-black px-1 rounded bg-green-500 text-white uppercase tracking-wider">
              Starter
            </span>
          )}
          {p.is_rookie && (
            <span className="text-[8px] font-black px-1 rounded bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300">
              R
            </span>
          )}
          {statusBadge && (
            <span className={`text-[8px] font-black px-1 rounded ${statusBadge.cls}`}>
              {statusBadge.label}
            </span>
          )}
        </div>
        <div className="flex items-center gap-1.5 text-[9px] text-slate-500 dark:text-slate-400 font-mono">
          <span className="font-bold uppercase">{p.position}</span>
          {p.age != null && (
            <>
              <span>·</span>
              <span>{p.age}y</span>
            </>
          )}
          {p.draft_number != null ? (
            <>
              <span>·</span>
              <span className="text-purple-600 dark:text-purple-400">#{p.draft_number}</span>
            </>
          ) : p.is_rookie ? (
            <>
              <span>·</span>
              <span className="italic text-slate-400 dark:text-slate-500" title="Undrafted Free Agent">UDFA</span>
            </>
          ) : null}
          {p.snap_pct_avg > 0 && (
            <>
              <span>·</span>
              <span>{Math.round(p.snap_pct_avg)}% snap</span>
            </>
          )}
          {p.position_group !== 'ol' && p.season_avg_pts > 0 && (
            <>
              <span>·</span>
              <span>{p.season_avg_pts.toFixed(1)} ppg</span>
            </>
          )}
        </div>
        {(snapShare > 0 || ppgShare > 0) && (
          <div className="flex gap-1.5 mt-1">
            {snapShare > 0 && (
              <div className="flex-1 h-1 rounded-full bg-slate-100 dark:bg-slate-700 overflow-hidden" title={`${Math.round(p.snap_pct_avg)}% snap`}>
                <div className="h-full bg-blue-500 dark:bg-blue-400" style={{ width: `${snapShare}%` }} />
              </div>
            )}
            {ppgShare > 0 && p.position_group !== 'ol' && (
              <div className="flex-1 h-1 rounded-full bg-slate-100 dark:bg-slate-700 overflow-hidden" title={`${p.season_avg_pts.toFixed(1)} ppg`}>
                <div className="h-full bg-amber-500 dark:bg-amber-400" style={{ width: `${ppgShare}%` }} />
              </div>
            )}
          </div>
        )}
      </div>
      <div className="flex flex-col gap-0.5" onClick={(e) => e.stopPropagation()}>
        <button
          onClick={() => onToggleCompare(p.player_id)}
          className={`w-5 h-5 rounded flex items-center justify-center ${
            isComparing
              ? 'bg-blue-600 text-white'
              : 'bg-slate-100 dark:bg-slate-700 text-slate-500 hover:text-blue-600'
          }`}
          title="Compare"
        >
          {isComparing ? <Check size={9} strokeWidth={4} /> : <Plus size={9} strokeWidth={3} />}
        </button>
        <button
          onClick={() => onViewHistory(p.player_id)}
          className="w-5 h-5 rounded flex items-center justify-center bg-slate-100 dark:bg-slate-700 text-slate-500 hover:text-purple-600"
          title="History"
        >
          <History size={9} />
        </button>
      </div>
    </div>
  );
};

const TeamOffenseModal: React.FC<Props> = ({
  team,
  focusPlayerId,
  onClose,
  compareList,
  onToggleCompare,
  onViewHistory,
}) => {
  const [data, setData] = useState<TeamOffenseResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [detailPlayer, setDetailPlayer] = useState<OffensePlayer | null>(null);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
    axios
      .get(`${API_BASE_URL}/team/${team}/offense`)
      .then((res) => {
        if (active) setData(res.data);
      })
      .catch((err) => {
        if (active) setError(err.message || 'Failed to load roster');
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [team]);

  // Close on ESC
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const teamColor = getTeamColor(team);

  // Quick fan-facing stats: total skill players, injury count, average age of starters.
  const summary = React.useMemo(() => {
    if (!data) return null;
    const all = [...data.qb, ...data.rb, ...data.wr, ...data.te, ...data.ol];
    const injured = all.filter((p) => {
      const s = (p.injury_status || '').toLowerCase();
      return s.includes('out') || s.includes('ir') || s.includes('doubtful') || s.includes('questionable');
    });
    const skill = [...data.qb, ...data.rb, ...data.wr, ...data.te];
    const rookies = all.filter((p) => p.is_rookie);
    const starters = skill.filter((p) => p.snap_pct_avg >= 50);
    const avgAge =
      starters.length > 0
        ? Math.round(
            (starters.reduce((acc, p) => acc + (p.age || 0), 0) / starters.length) * 10,
          ) / 10
        : null;
    return { total: all.length, injured: injured.length, rookies: rookies.length, avgAge };
  }, [data]);

  return (
    <div
      className="fixed inset-0 z-[60] flex items-center justify-center bg-black/50 backdrop-blur-sm p-4 animate-in fade-in duration-200"
      onClick={onClose}
    >
      <div
        className="bg-white dark:bg-slate-900 rounded-2xl shadow-2xl border border-slate-200 dark:border-slate-700 w-full max-w-5xl max-h-[88vh] flex flex-col overflow-hidden animate-in zoom-in-95 duration-200"
        onClick={(e) => e.stopPropagation()}
      >
        {/* HEADER */}
        <div
          className="px-5 py-4 flex items-center justify-between border-b border-slate-200 dark:border-slate-700"
          style={{ background: `linear-gradient(135deg, ${teamColor}22 0%, transparent 100%)` }}
        >
          <div className="flex items-center gap-3">
            <div
              className="w-10 h-10 rounded-lg flex items-center justify-center text-white font-black"
              style={{ backgroundColor: teamColor }}
            >
              {team}
            </div>
            <div>
              <h3 className="text-xs font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest">
                Team Offense
              </h3>
              <h2 className="text-xl font-black text-slate-800 dark:text-slate-100">{team} setup</h2>
            </div>
            {summary && (
              <div className="hidden sm:flex items-center gap-3 ml-4 pl-4 border-l border-slate-200 dark:border-slate-700">
                <div className="text-center">
                  <div className="text-[10px] font-bold uppercase text-slate-400 tracking-wider">Players</div>
                  <div className="text-sm font-black text-slate-800 dark:text-slate-100">{summary.total}</div>
                </div>
                {summary.avgAge != null && (
                  <div className="text-center">
                    <div className="text-[10px] font-bold uppercase text-slate-400 tracking-wider">Avg age</div>
                    <div className="text-sm font-black text-slate-800 dark:text-slate-100">{summary.avgAge}y</div>
                  </div>
                )}
                <div className="text-center">
                  <div className="text-[10px] font-bold uppercase text-slate-400 tracking-wider">Rookies</div>
                  <div className="text-sm font-black text-purple-600 dark:text-purple-400">{summary.rookies}</div>
                </div>
                <div className="text-center">
                  <div className="text-[10px] font-bold uppercase text-slate-400 tracking-wider">Injuries</div>
                  <div
                    className={`text-sm font-black ${
                      summary.injured > 0 ? 'text-red-600 dark:text-red-400' : 'text-slate-800 dark:text-slate-100'
                    }`}
                  >
                    {summary.injured}
                  </div>
                </div>
              </div>
            )}
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-lg text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800 transition"
            aria-label="Close"
          >
            <X size={18} />
          </button>
        </div>

        {/* BODY */}
        <div className="flex-1 overflow-y-auto p-5 scrollbar-thin">
          {loading ? (
            <div className="flex flex-col items-center py-12 opacity-60">
              <div className="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin" />
              <p className="text-sm font-bold text-slate-400 mt-3">Loading {team} roster...</p>
            </div>
          ) : error ? (
            <div className="text-center py-12 text-red-500 font-bold text-sm">{error}</div>
          ) : data ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {GROUP_ORDER.map(({ key, label }) => {
                const players = data[key];
                const topSnap = Math.max(0, ...players.map((p) => p.snap_pct_avg || 0));
                const topPpg = Math.max(0, ...players.map((p) => p.season_avg_pts || 0));
                return (
                  <div
                    key={key}
                    className={`bg-slate-50 dark:bg-slate-800/40 rounded-xl border border-slate-200 dark:border-slate-700 p-3 ${
                      key === 'wr' || key === 'ol' ? 'lg:col-span-1' : ''
                    }`}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
                        {label}
                      </h4>
                      <span className="text-[10px] font-mono text-slate-400 dark:text-slate-500">
                        {players.length}
                      </span>
                    </div>
                    <div className="space-y-1.5 max-h-[52vh] overflow-y-auto pr-1 scrollbar-thin">
                      {players.length === 0 ? (
                        <div className="text-[10px] italic text-slate-400 dark:text-slate-500 text-center py-2">
                          No players
                        </div>
                      ) : (
                        players.map((p, idx) => (
                          <PlayerRow
                            key={p.player_id}
                            p={p}
                            rank={idx}
                            groupTopSnap={topSnap}
                            groupTopPpg={topPpg}
                            isFocus={p.player_id === focusPlayerId}
                            isComparing={compareList.includes(p.player_id)}
                            onToggleCompare={onToggleCompare}
                            onViewHistory={(id) => {
                              onViewHistory(id);
                              onClose();
                            }}
                            onOpenDetail={setDetailPlayer}
                          />
                        ))
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          ) : null}
        </div>
      </div>

      {/* Stacked player detail modal */}
      {detailPlayer && (
        <PlayerDetailModal
          player={detailPlayer}
          isComparing={compareList.includes(detailPlayer.player_id)}
          onToggleCompare={onToggleCompare}
          onViewHistory={(id) => {
            onViewHistory(id);
            setDetailPlayer(null);
            onClose();
          }}
          onClose={() => setDetailPlayer(null)}
        />
      )}
    </div>
  );
};

export default TeamOffenseModal;
