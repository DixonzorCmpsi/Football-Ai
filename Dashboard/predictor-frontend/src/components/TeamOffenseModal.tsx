import React, { memo, useEffect, useRef, useState } from 'react';
import axios from 'axios';
import { X, Plus, Check, History, Shield, TrendingUp, Maximize2 } from 'lucide-react';
import { getTeamColor } from '../utils/nflColors';
import PlayerDetailModal from './PlayerDetailModal';
import TeamBuilderView from './TeamBuilderView';

const API_BASE_URL =
  typeof window !== 'undefined' && window.__env && window.__env.API_BASE_URL
    ? window.__env.API_BASE_URL
    : '/api';

export interface OffensePlayer {
  player_id: string;
  player_name: string;
  position: string;
  position_group: 'qb' | 'rb' | 'wr' | 'te' | 'ol' | 'dl' | 'lb' | 'db';
  team: string;
  image?: string;
  injury_status?: string;
  is_rookie: boolean;
  // Sourced from nflreadpy depth chart (pos_rank == 1); not inferred from snaps.
  is_starter?: boolean;
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
  def_tackles_total?: number;
  def_tackles_solo?: number;
  def_tackle_assists?: number;
  def_tackles_for_loss?: number;
  def_sacks?: number;
  def_qb_hits?: number;
  def_interceptions?: number;
  def_pass_defended?: number;
  def_fumbles_forced?: number;
  def_fumble_recoveries?: number;
  snaps_total: number;
  snap_pct_avg: number;
  last_season?: number | null;
  last_season_rank_position?: string | null;
  last_season_position_rank?: number | null;
  last_season_position_ppg_rank?: number | null;
  last_season_position_rank_out_of?: number | null;
  last_season_games_played?: number;
  last_season_total_pts?: number;
  last_season_avg_pts?: number;
  last_season_team?: string | null;
  last_season_team_sacks_taken?: number | null;
  last_season_team_sacks_per_game?: number | null;
  last_season_team_sacks_rank?: number | null;
  last_season_team_sacks_rank_out_of?: number | null;
  last_season_team_rush_tds?: number | null;
  last_season_team_rush_tds_per_game?: number | null;
  last_season_team_rush_tds_rank?: number | null;
  last_season_team_rush_tds_rank_out_of?: number | null;
}

interface TeamMetric {
  label: string;
  value: number;
  rank: number;
  rank_out_of: number;
  lower_is_better: boolean;
}

interface TeamSideRanking {
  overall?: TeamMetric;
  metrics: Record<string, TeamMetric>;
}

interface TeamRankings {
  season: number;
  source: string;
  offense?: TeamSideRanking | null;
  defense?: TeamSideRanking | null;
}

interface TeamFreshness {
  roster_loaded_at?: string | null;
  depth_charts_loaded_at?: string | null;
  generated_at?: string | null;
}

type DefenseGroupKey = 'dl' | 'lb' | 'db';

interface TeamOffenseResponse {
  team: string;
  last_season?: number;
  rankings?: TeamRankings | null;
  freshness?: TeamFreshness;
  qb: OffensePlayer[];
  rb: OffensePlayer[];
  wr: OffensePlayer[];
  te: OffensePlayer[];
  ol: OffensePlayer[];
  defense?: Record<DefenseGroupKey, OffensePlayer[]>;
}

interface Props {
  team: string;
  focusPlayerId?: string | null;
  initialDetailPlayerId?: string | null;
  initialTab?: 'overview' | 'builder';
  onClose: () => void;
  compareList: string[];
  onToggleCompare: (id: string) => void;
  // `ctx` lets App stash which player-detail modal was open so Back from History
  // can restore it. Optional so existing callers (Schedule/Lookup) keep working.
  onViewHistory: (id: string, ctx?: { detailPlayerId?: string | null; activeTab?: 'overview' | 'builder' }) => void;
}

interface PositionView {
  label: string;
  slot?: string;
  players: OffensePlayer[];
}

const DEFENSE_ORDER: Array<{ key: DefenseGroupKey; label: string }> = [
  { key: 'dl', label: 'DL' },
  { key: 'lb', label: 'LB' },
  { key: 'db', label: 'DB' },
];

const formatSackValue = (value?: number | null) => {
  if (value == null) return '0';
  return Number.isInteger(value) ? String(value) : value.toFixed(1);
};

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
}> = memo(({ p, rank: _rank, groupTopSnap, groupTopPpg, isFocus, isComparing, onToggleCompare, onViewHistory, onOpenDetail }) => {
  // Authoritative source: nflreadpy depth chart on the server. No client-side guessing.
  const isStarter = !!p.is_starter;
  const snapShare = groupTopSnap > 0 ? Math.min(100, (p.snap_pct_avg / groupTopSnap) * 100) : 0;
  const ppgShare = groupTopPpg > 0 ? Math.min(100, (p.season_avg_pts / groupTopPpg) * 100) : 0;
  const status = (p.injury_status || '').toLowerCase();
  const lastSeasonRank =
    p.last_season_rank_position && p.last_season_position_rank
      ? `${p.last_season_rank_position}${p.last_season_position_rank}`
      : null;
  const lastSeason = p.last_season ?? null;
  const lastSeasonShort = lastSeason ? `'${String(lastSeason).slice(-2)}` : 'last season';
  const isOlFinish = p.last_season_rank_position === 'OL';
  const isIdpFinish = ['DL', 'LB', 'DB'].includes(p.last_season_rank_position || '');
  const isDefenseGroup = ['dl', 'lb', 'db'].includes(p.position_group);
  const lastSeasonScoring = isOlFinish ? 'offensive snaps' : isIdpFinish ? 'defensive points' : 'PPR';
  const lastSeasonTitle = lastSeasonRank && lastSeason
    ? isOlFinish
      ? `${lastSeason} ${lastSeasonRank}: ${Math.round(p.last_season_total_pts || 0).toLocaleString()} offensive snaps, ${Math.round(p.last_season_avg_pts || 0)}% snap rate over ${p.last_season_games_played || 0} games${
          p.last_season_team_sacks_taken != null
            ? `; ${p.last_season_team || 'team'} QBs took ${Math.round(p.last_season_team_sacks_taken)} sacks (${(p.last_season_team_sacks_per_game || 0).toFixed(2)}/game, rank #${p.last_season_team_sacks_rank || '-'})`
            : ''
        }${
          p.last_season_team_rush_tds != null
            ? `; ${p.last_season_team || 'team'} scored ${Math.round(p.last_season_team_rush_tds)} rushing TDs (${(p.last_season_team_rush_tds_per_game || 0).toFixed(2)}/game, rank #${p.last_season_team_rush_tds_rank || '-'})`
            : ''
        }`
      : `${lastSeason} ${lastSeasonRank}: ${(p.last_season_total_pts || 0).toFixed(1)} ${lastSeasonScoring}, ${(p.last_season_avg_pts ?? 0).toFixed(1)} PPG over ${p.last_season_games_played || 0} games`
    : undefined;
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
          {lastSeasonRank && (
            <span
              className="text-[8px] font-black px-1 rounded bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300"
              title={lastSeasonTitle}
            >
              {lastSeason ? `${lastSeason} ` : ''}{lastSeasonRank}
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
          {isDefenseGroup && (p.def_sacks || 0) > 0 && (
            <>
              <span>·</span>
              <span>{formatSackValue(p.def_sacks)} sacks</span>
            </>
          )}
          {isDefenseGroup && (p.def_pass_defended || 0) > 0 && (
            <>
              <span>·</span>
              <span>{p.def_pass_defended} PBU</span>
            </>
          )}
          {isDefenseGroup && (p.def_tackles_total || 0) > 0 && (
            <>
              <span>·</span>
              <span>{p.def_tackles_total} tackles</span>
            </>
          )}
          {!isDefenseGroup && p.position_group !== 'ol' && p.season_avg_pts > 0 && (
            <>
              <span>·</span>
              <span>{p.season_avg_pts.toFixed(1)} ppg</span>
            </>
          )}
          {lastSeasonRank && (p.last_season_avg_pts || 0) > 0 && (
            <>
              <span>·</span>
              <span>
                {isOlFinish
                  ? `${Math.round(p.last_season_avg_pts || 0)}% ${lastSeasonShort} snap`
                  : `${(p.last_season_avg_pts || 0).toFixed(1)} ${lastSeasonShort} ppg`}
              </span>
            </>
          )}
          {isOlFinish && p.last_season_team_sacks_taken != null && (
            <>
              <span>·</span>
              <span>{p.last_season_team || 'TM'} QB sacks: {Math.round(p.last_season_team_sacks_taken)}</span>
            </>
          )}
          {isOlFinish && p.last_season_team_rush_tds != null && (
            <>
              <span>·</span>
              <span>{p.last_season_team || 'TM'} rush TD: {Math.round(p.last_season_team_rush_tds)}</span>
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
});
PlayerRow.displayName = 'PlayerRow';

const rankTone = (metric: TeamMetric) => {
  const pct = metric.rank_out_of > 0 ? metric.rank / metric.rank_out_of : 1;
  if (pct <= 0.25) return 'text-emerald-600 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-900/20';
  if (pct <= 0.62) return 'text-amber-600 dark:text-amber-400 bg-amber-50 dark:bg-amber-900/20';
  return 'text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20';
};

const formatMetricValue = (metric: TeamMetric) => {
  if (Number.isInteger(metric.value)) return metric.value.toFixed(0);
  return metric.value.toFixed(metric.value < 10 ? 2 : 1);
};

const RankingSideCard: React.FC<{
  title: string;
  icon: React.ReactNode;
  side?: TeamSideRanking | null;
  metricOrder: string[];
}> = ({ title, icon, side, metricOrder }) => {
  const metrics = metricOrder.map((key) => side?.metrics?.[key]).filter(Boolean) as TeamMetric[];
  return (
    <div className="bg-slate-50 dark:bg-slate-800/40 rounded-xl border border-slate-200 dark:border-slate-700 p-3">
      <div className="flex items-center justify-between gap-2 mb-2">
        <div className="flex items-center gap-2 min-w-0">
          <div className="w-7 h-7 rounded-lg bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 flex items-center justify-center text-slate-500 dark:text-slate-300 shrink-0">
            {icon}
          </div>
          <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400 truncate">
            {title}
          </h4>
        </div>
        {side?.overall && (
          <div className={`px-2 py-1 rounded-md text-[10px] font-black ${rankTone(side.overall)}`}>
            #{side.overall.rank}
          </div>
        )}
      </div>

      {metrics.length === 0 ? (
        <div className="text-[10px] italic text-slate-400 dark:text-slate-500 py-3 text-center">
          No ranking data
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-1.5">
          {metrics.map((metric) => (
            <div key={metric.label} className="rounded-lg bg-white dark:bg-slate-900/60 border border-slate-200 dark:border-slate-700 px-2 py-1.5">
              <div className="flex items-center justify-between gap-1">
                <span className="text-[9px] font-bold uppercase text-slate-400 dark:text-slate-500 truncate">
                  {metric.label}
                </span>
                <span className={`text-[9px] font-black px-1.5 py-0.5 rounded ${rankTone(metric)}`}>
                  #{metric.rank}
                </span>
              </div>
              <div className="text-sm font-black text-slate-800 dark:text-slate-100 mt-0.5">
                {formatMetricValue(metric)}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

const TeamRankingsPanel: React.FC<{ rankings?: TeamRankings | null; lastSeason?: number }> = ({ rankings, lastSeason }) => {
  if (!rankings) return null;
  const season = rankings.season || lastSeason;
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
      <RankingSideCard
        title={`${season} Offense`}
        icon={<TrendingUp size={15} />}
        side={rankings.offense}
        metricOrder={['points_per_game', 'yards_per_game', 'pass_yards_per_game', 'rush_yards_per_game', 'sacks_taken_per_game', 'turnovers_per_game']}
      />
      <RankingSideCard
        title={`${season} Defense`}
        icon={<Shield size={15} />}
        side={rankings.defense}
        metricOrder={[
          'points_allowed_per_game',
          'yards_allowed_per_game',
          'pass_yards_allowed_per_game',
          'rush_yards_allowed_per_game',
          'sacks_per_game',
          'takeaways_per_game',
        ]}
      />
    </div>
  );
};

const LineupGroup: React.FC<{
  label: string;
  slot?: string;
  players: OffensePlayer[];
  focusPlayerId?: string | null;
  compareList: string[];
  onToggleCompare: (id: string) => void;
  onViewHistory: (id: string) => void;
  onOpenDetail: (p: OffensePlayer) => void;
  onOpenGroup: () => void;
  className?: string;
}> = ({
  label,
  slot,
  players,
  focusPlayerId,
  compareList,
  onToggleCompare,
  onViewHistory,
  onOpenDetail,
  onOpenGroup,
  className = '',
}) => {
  const topSnap = Math.max(0, ...players.map((p) => p.snap_pct_avg || 0));
  const topPpg = Math.max(0, ...players.map((p) => p.season_avg_pts || 0));
  const starterCount = players.filter((p) => p.is_starter).length;
  const visibleLimit = label === 'WRs' || label === 'Offensive Line' ? 10 : 7;
  const visiblePlayers = players.slice(0, visibleLimit);
  const hiddenCount = Math.max(0, players.length - visiblePlayers.length);

  return (
    <section className={`min-h-0 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/90 dark:bg-slate-900/70 p-3 shadow-sm ${className}`}>
      <button
        type="button"
        onClick={onOpenGroup}
        className="w-full flex items-center justify-between gap-2 mb-2 text-left rounded-lg -mx-1 px-1 py-0.5 transition hover:bg-slate-100 dark:hover:bg-slate-800/70 focus:outline-none focus:ring-2 focus:ring-blue-500/30"
      >
        <div className="min-w-0">
          <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400 truncate">
            {label}
          </h4>
          {slot && (
            <div className="mt-0.5 text-[9px] font-bold uppercase tracking-widest text-slate-400 dark:text-slate-500 truncate">
              {slot}
            </div>
          )}
        </div>
        <div className="flex items-center gap-1.5">
          {starterCount > 0 && (
            <span className="text-[9px] font-black px-1.5 py-0.5 rounded bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300">
              {starterCount} ST
            </span>
          )}
          <span className="text-[10px] font-mono text-slate-400 dark:text-slate-500">
            {players.length}
          </span>
          <span className="w-6 h-6 rounded-md bg-slate-100 dark:bg-slate-800 text-slate-400 dark:text-slate-500 flex items-center justify-center">
            <Maximize2 size={11} />
          </span>
        </div>
      </button>

      <div className="space-y-1.5">
        {players.length === 0 ? (
          <div className="text-[10px] italic text-slate-400 dark:text-slate-500 text-center py-2">
            No players
          </div>
        ) : (
          visiblePlayers.map((p, idx) => (
            <PlayerRow
              key={p.player_id}
              p={p}
              rank={idx}
              groupTopSnap={topSnap}
              groupTopPpg={topPpg}
              isFocus={p.player_id === focusPlayerId}
              isComparing={compareList.includes(p.player_id)}
              onToggleCompare={onToggleCompare}
              onViewHistory={onViewHistory}
              onOpenDetail={onOpenDetail}
            />
          ))
        )}
        {hiddenCount > 0 && (
          <button
            type="button"
            onClick={onOpenGroup}
            className="w-full rounded-md border border-dashed border-slate-200 dark:border-slate-700 py-1.5 text-[10px] font-black uppercase tracking-widest text-slate-400 hover:text-blue-600 dark:hover:text-blue-400 hover:border-blue-300 dark:hover:border-blue-700 transition"
          >
            {hiddenCount} more
          </button>
        )}
      </div>
    </section>
  );
};

const DefenseColumn: React.FC<{
  defense?: Record<DefenseGroupKey, OffensePlayer[]>;
  focusPlayerId?: string | null;
  compareList: string[];
  onToggleCompare: (id: string) => void;
  onViewHistory: (id: string) => void;
  onOpenDetail: (p: OffensePlayer) => void;
  onOpenGroup: (group: PositionView) => void;
}> = ({ defense, focusPlayerId, compareList, onToggleCompare, onViewHistory, onOpenDetail, onOpenGroup }) => {
  const groups = defense || { dl: [], lb: [], db: [] };
  const all = [...groups.dl, ...groups.lb, ...groups.db];
  const starterCount = all.filter((p) => p.is_starter).length;

  return (
    <section className="rounded-xl border border-slate-200 dark:border-slate-700 bg-white/90 dark:bg-slate-900/70 p-3 shadow-sm">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <Shield size={14} className="text-slate-500 dark:text-slate-400" />
          <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
            Defense
          </h4>
        </div>
        <span className="text-[10px] font-mono text-slate-400 dark:text-slate-500">
          {starterCount || all.length}
        </span>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
        {DEFENSE_ORDER.map(({ key, label }) => {
          const players = groups[key] || [];
          const starters = players.filter((p) => p.is_starter);
          const reserves = players.filter((p) => !p.is_starter);
          const visible = starters.length > 0 ? [...starters, ...reserves.slice(0, 3)] : players.slice(0, 7);
          return (
            <div key={key} className="min-h-0">
              <button
                type="button"
                onClick={() => onOpenGroup({ label, slot: 'Defense', players })}
                className="w-full flex items-center justify-between mb-1 rounded-md px-1 py-0.5 -mx-1 text-left transition hover:bg-slate-100 dark:hover:bg-slate-800/70 focus:outline-none focus:ring-2 focus:ring-blue-500/30"
              >
                <span className="text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-500">
                  {label}
                </span>
                <span className="flex items-center gap-1.5">
                  <span className="text-[9px] font-mono text-slate-400 dark:text-slate-500">
                    {starters.length || players.length}
                  </span>
                  <span className="w-5 h-5 rounded bg-slate-100 dark:bg-slate-800 text-slate-400 dark:text-slate-500 flex items-center justify-center">
                    <Maximize2 size={10} />
                  </span>
                </span>
              </button>
              <div className="space-y-1.5">
                {visible.length === 0 ? (
                  <div className="text-[10px] italic text-slate-400 dark:text-slate-500 text-center py-2">
                    No players
                  </div>
                ) : (
                  visible.map((p, idx) => (
                    <PlayerRow
                      key={p.player_id}
                      p={p}
                      rank={idx}
                      groupTopSnap={0}
                      groupTopPpg={0}
                      isFocus={p.player_id === focusPlayerId}
                      isComparing={compareList.includes(p.player_id)}
                      onToggleCompare={onToggleCompare}
                      onViewHistory={onViewHistory}
                      onOpenDetail={onOpenDetail}
                    />
                  ))
                )}
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
};

const PositionStatTile: React.FC<{ label: string; value: string | number; sub?: string; tone?: 'default' | 'pos' | 'neg' | 'muted' }> = ({
  label,
  value,
  sub,
  tone = 'default',
}) => {
  const valueColor =
    tone === 'pos'
      ? 'text-emerald-600 dark:text-emerald-400'
      : tone === 'neg'
      ? 'text-red-600 dark:text-red-400'
      : tone === 'muted'
      ? 'text-slate-400 dark:text-slate-500'
      : 'text-slate-800 dark:text-slate-100';

  return (
    <div className="rounded-lg bg-slate-50 dark:bg-slate-800/70 border border-slate-200 dark:border-slate-700 p-3">
      <div className="text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-500">
        {label}
      </div>
      <div className={`mt-1 text-lg font-black ${valueColor}`}>{value}</div>
      {sub && <div className="mt-0.5 text-[10px] text-slate-400 dark:text-slate-500">{sub}</div>}
    </div>
  );
};

const PositionFocusModal: React.FC<{
  group: PositionView;
  team: string;
  teamColor: string;
  focusPlayerId?: string | null;
  compareList: string[];
  onToggleCompare: (id: string) => void;
  onViewHistory: (id: string) => void;
  onOpenDetail: (p: OffensePlayer) => void;
  onClose: () => void;
}> = ({ group, team, teamColor, focusPlayerId, compareList, onToggleCompare, onViewHistory, onOpenDetail, onClose }) => {
  const players = group.players;
  const topSnap = Math.max(0, ...players.map((p) => p.snap_pct_avg || 0));
  const topPpg = Math.max(0, ...players.map((p) => p.season_avg_pts || 0));
  const starters = players.filter((p) => p.is_starter);
  const rookies = players.filter((p) => p.is_rookie);
  const injured = players.filter((p) => {
    const status = (p.injury_status || '').toLowerCase();
    return status.includes('out') || status.includes('ir') || status.includes('doubtful') || status.includes('questionable');
  });
  const ages = players.map((p) => p.age).filter((age): age is number => age != null && age > 0);
  const avgAge = ages.length ? Math.round((ages.reduce((sum, age) => sum + age, 0) / ages.length) * 10) / 10 : null;
  const bestFinish = players
    .filter((p) => p.last_season_rank_position && p.last_season_position_rank)
    .sort((a, b) => (a.last_season_position_rank || 9999) - (b.last_season_position_rank || 9999))[0];
  const productionLeader = players
    .filter((p) => p.position_group !== 'ol' && p.season_avg_pts > 0)
    .sort((a, b) => b.season_avg_pts - a.season_avg_pts)[0];
  const snapLeader = players
    .filter((p) => (p.last_season_total_pts || p.snap_pct_avg || 0) > 0)
    .sort((a, b) => (b.last_season_total_pts || b.snap_pct_avg || 0) - (a.last_season_total_pts || a.snap_pct_avg || 0))[0];
  const isOlGroup = players.some((p) => p.position_group === 'ol' || p.last_season_rank_position === 'OL');
  const isDefenseGroupView = players.some((p) => ['dl', 'lb', 'db'].includes(p.position_group));
  const sackLeader = [...players].sort((a, b) => (b.def_sacks || 0) - (a.def_sacks || 0))[0];
  const bestFinishLabel =
    bestFinish?.last_season_rank_position && bestFinish?.last_season_position_rank
      ? `${bestFinish.last_season_rank_position}${bestFinish.last_season_position_rank}`
      : '—';

  return (
    <div
      className="fixed inset-0 z-[65] flex items-center justify-center bg-black/45 p-3 md:p-5 animate-in fade-in duration-150"
      onClick={onClose}
    >
      <div
        className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-700 shadow-2xl w-full max-w-6xl max-h-[88vh] flex flex-col overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        <div
          className="px-5 py-4 flex items-center justify-between gap-4 border-b border-slate-200 dark:border-slate-700"
          style={{ background: `linear-gradient(135deg, ${teamColor}24 0%, transparent 100%)` }}
        >
          <div className="flex items-center gap-3 min-w-0">
            <div
              className="w-10 h-10 rounded-lg flex items-center justify-center text-white text-xs font-black shrink-0"
              style={{ backgroundColor: teamColor }}
            >
              {team}
            </div>
            <div className="min-w-0">
              <div className="text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-500 truncate">
                {group.slot || 'Position'}
              </div>
              <h3 className="text-xl font-black text-slate-800 dark:text-slate-100 truncate">
                {group.label}
              </h3>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-lg text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800 transition"
            aria-label="Close position view"
          >
            <X size={18} />
          </button>
        </div>

        <div className="flex-1 min-h-0 overflow-y-auto p-4 md:p-5 scrollbar-thin">
          <div className="grid grid-cols-1 xl:grid-cols-[18rem_minmax(0,1fr)] gap-4">
            <aside className="space-y-3">
              <div className="grid grid-cols-2 xl:grid-cols-1 gap-3">
                <PositionStatTile label="Players" value={players.length} />
                <PositionStatTile label="Starters" value={starters.length} tone={starters.length ? 'pos' : 'muted'} />
                <PositionStatTile label="Avg age" value={avgAge != null ? `${avgAge}y` : '—'} />
                <PositionStatTile label="Rookies" value={rookies.length} tone={rookies.length ? 'pos' : 'default'} />
                <PositionStatTile label="Injuries" value={injured.length} tone={injured.length ? 'neg' : 'default'} />
                <PositionStatTile
                  label="Best 2025"
                  value={bestFinishLabel}
                  sub={bestFinish?.player_name}
                  tone={bestFinish ? 'pos' : 'muted'}
                />
              </div>
              {isOlGroup && snapLeader ? (
                <PositionStatTile
                  label="Snap leader"
                  value={snapLeader.last_season_total_pts ? Math.round(snapLeader.last_season_total_pts).toLocaleString() : `${Math.round(snapLeader.snap_pct_avg || 0)}%`}
                  sub={snapLeader.player_name}
                />
              ) : isDefenseGroupView && sackLeader && (sackLeader.def_sacks || 0) > 0 ? (
                <PositionStatTile
                  label="Sack leader"
                  value={formatSackValue(sackLeader.def_sacks)}
                  sub={`${sackLeader.player_name}${sackLeader.def_pass_defended ? ` · ${sackLeader.def_pass_defended} PBU` : ''}`}
                />
              ) : productionLeader ? (
                <PositionStatTile
                  label={isDefenseGroupView ? 'Def pts/g' : 'Current PPG'}
                  value={productionLeader.season_avg_pts.toFixed(1)}
                  sub={productionLeader.player_name}
                />
              ) : null}
            </aside>

            <div className="min-w-0">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-2">
                {players.length === 0 ? (
                  <div className="lg:col-span-2 rounded-xl border border-dashed border-slate-200 dark:border-slate-700 p-8 text-center text-sm font-bold text-slate-400 dark:text-slate-500">
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
                      onViewHistory={onViewHistory}
                      onOpenDetail={onOpenDetail}
                    />
                  ))
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

const TeamOffenseModal: React.FC<Props> = ({
  team,
  focusPlayerId,
  initialDetailPlayerId,
  initialTab = 'overview',
  onClose,
  compareList,
  onToggleCompare,
  onViewHistory,
}) => {
  const [data, setData] = useState<TeamOffenseResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [detailPlayer, setDetailPlayer] = useState<OffensePlayer | null>(null);
  const [expandedGroup, setExpandedGroup] = useState<PositionView | null>(null);
  const [activeTab, setActiveTab] = useState<'overview' | 'builder'>(initialTab);

  // When App restores us with an initial detail player (e.g. after Back from
  // History), open that player's detail modal once the roster has loaded. Guarded
  // with a ref so closing the detail modal doesn't immediately reopen it.
  const restoredInitialRef = useRef(false);
  useEffect(() => {
    if (restoredInitialRef.current) return;
    if (!initialDetailPlayerId || !data) return;
    const defense = data.defense || { dl: [], lb: [], db: [] };
    const all = [...data.qb, ...data.rb, ...data.wr, ...data.te, ...data.ol, ...defense.dl, ...defense.lb, ...defense.db];
    const match = all.find((p) => p.player_id === initialDetailPlayerId);
    if (match) {
      setDetailPlayer(match);
      restoredInitialRef.current = true;
    }
  }, [initialDetailPlayerId, data]);

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

  useEffect(() => {
    setActiveTab(initialTab);
  }, [team, initialTab]);

  // Close on ESC
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      if (detailPlayer) return;
      if (expandedGroup) {
        setExpandedGroup(null);
        return;
      }
      onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [detailPlayer, expandedGroup, onClose]);

  const teamColor = getTeamColor(team);

  // Quick fan-facing stats: total skill players, injury count, average age of starters.
  const summary = React.useMemo(() => {
    if (!data) return null;
    const defense = data.defense || { dl: [], lb: [], db: [] };
    const all = [...data.qb, ...data.rb, ...data.wr, ...data.te, ...data.ol, ...defense.dl, ...defense.lb, ...defense.db];
    const injured = all.filter((p) => {
      const s = (p.injury_status || '').toLowerCase();
      return s.includes('out') || s.includes('ir') || s.includes('doubtful') || s.includes('questionable');
    });
    const skill = [...data.qb, ...data.rb, ...data.wr, ...data.te];
    const rookies = all.filter((p) => p.is_rookie);
    const starters = skill.filter((p) => p.is_starter);
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
      className="fixed inset-0 z-[60] flex items-stretch justify-center bg-black/55 p-2 md:p-4 animate-in fade-in duration-200"
      onClick={onClose}
    >
      <div
        className="bg-white dark:bg-slate-900 rounded-2xl shadow-2xl border border-slate-200 dark:border-slate-700 w-full max-w-[96rem] h-[94vh] flex flex-col overflow-hidden animate-in zoom-in-95 duration-200"
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
                Team Overview
              </h3>
              <h2 className="text-xl font-black text-slate-800 dark:text-slate-100">{team} lineup</h2>
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
          <div className="flex items-center gap-3">
            <div className="hidden md:flex rounded-lg bg-slate-100 dark:bg-slate-800/70 border border-slate-200 dark:border-slate-700 p-1">
              <button
                onClick={() => setActiveTab('overview')}
                className={`px-3 py-1.5 rounded-md text-xs font-black uppercase tracking-widest transition ${
                  activeTab === 'overview'
                    ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm'
                    : 'text-slate-500 dark:text-slate-400 hover:text-slate-800 dark:hover:text-slate-200'
                }`}
              >
                Overview
              </button>
              <button
                onClick={() => setActiveTab('builder')}
                className={`px-3 py-1.5 rounded-md text-xs font-black uppercase tracking-widest transition ${
                  activeTab === 'builder'
                    ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm'
                    : 'text-slate-500 dark:text-slate-400 hover:text-slate-800 dark:hover:text-slate-200'
                }`}
              >
                Team Builder
              </button>
            </div>
            <button
              onClick={onClose}
              className="p-2 rounded-lg text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800 transition"
              aria-label="Close"
            >
              <X size={18} />
            </button>
          </div>
        </div>

        {/* BODY */}
        <div className="flex-1 overflow-y-auto p-3 md:p-5 scrollbar-thin">
          <div className="md:hidden flex rounded-lg bg-slate-100 dark:bg-slate-800/70 border border-slate-200 dark:border-slate-700 p-1 mb-3">
            <button
              onClick={() => setActiveTab('overview')}
              className={`flex-1 px-2 py-1.5 rounded-md text-[10px] font-black uppercase tracking-widest transition ${
                activeTab === 'overview'
                  ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm'
                  : 'text-slate-500 dark:text-slate-400'
              }`}
            >
              Overview
            </button>
            <button
              onClick={() => setActiveTab('builder')}
              className={`flex-1 px-2 py-1.5 rounded-md text-[10px] font-black uppercase tracking-widest transition ${
                activeTab === 'builder'
                  ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm'
                  : 'text-slate-500 dark:text-slate-400'
              }`}
            >
              Team Builder
            </button>
          </div>
          {loading ? (
            <div className="flex flex-col items-center py-12 opacity-60">
              <div className="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin" />
              <p className="text-sm font-bold text-slate-400 mt-3">Loading {team} roster...</p>
            </div>
          ) : error ? (
            <div className="text-center py-12 text-red-500 font-bold text-sm">{error}</div>
          ) : data && activeTab === 'overview' ? (
            <div className="space-y-4">
              <TeamRankingsPanel rankings={data.rankings} lastSeason={data.last_season} />

              <div
                className="rounded-2xl border border-slate-200 dark:border-slate-700 bg-slate-50/80 dark:bg-slate-950/30 p-3 md:p-4"
                style={{
                  backgroundImage:
                    'linear-gradient(90deg, rgba(148, 163, 184, 0.10) 1px, transparent 1px)',
                  backgroundSize: '12.5% 100%',
                }}
              >
                <div className="grid grid-cols-1 xl:grid-cols-[minmax(16rem,0.9fr)_minmax(24rem,1.3fr)_minmax(16rem,0.9fr)] gap-4">
                  <div className="space-y-4 xl:col-start-2 xl:row-start-1">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <LineupGroup
                        label="QBs"
                        slot="Backfield"
                        players={data.qb}
                        focusPlayerId={focusPlayerId}
                        compareList={compareList}
                        onToggleCompare={onToggleCompare}
                        onViewHistory={(id) => {
                          onViewHistory(id, { detailPlayerId: null });
                          onClose();
                        }}
                        onOpenDetail={setDetailPlayer}
                        onOpenGroup={() => setExpandedGroup({ label: 'QBs', slot: 'Backfield', players: data.qb })}
                      />
                      <LineupGroup
                        label="RBs"
                        slot="Offset back"
                        players={data.rb}
                        focusPlayerId={focusPlayerId}
                        compareList={compareList}
                        onToggleCompare={onToggleCompare}
                        onViewHistory={(id) => {
                          onViewHistory(id, { detailPlayerId: null });
                          onClose();
                        }}
                        onOpenDetail={setDetailPlayer}
                        onOpenGroup={() => setExpandedGroup({ label: 'RBs', slot: 'Offset back', players: data.rb })}
                      />
                    </div>

                    <LineupGroup
                      label="Offensive Line"
                      slot="Line / pass pro"
                      players={data.ol}
                      focusPlayerId={focusPlayerId}
                      compareList={compareList}
                      onToggleCompare={onToggleCompare}
                      onViewHistory={(id) => {
                        onViewHistory(id, { detailPlayerId: null });
                        onClose();
                      }}
                      onOpenDetail={setDetailPlayer}
                      onOpenGroup={() => setExpandedGroup({ label: 'Offensive Line', slot: 'Line / pass pro', players: data.ol })}
                    />
                  </div>

                  <LineupGroup
                    label="WRs"
                    slot="Perimeter"
                    players={data.wr}
                    focusPlayerId={focusPlayerId}
                    compareList={compareList}
                    onToggleCompare={onToggleCompare}
                    onViewHistory={(id) => {
                      onViewHistory(id, { detailPlayerId: null });
                      onClose();
                    }}
                    onOpenDetail={setDetailPlayer}
                    onOpenGroup={() => setExpandedGroup({ label: 'WRs', slot: 'Perimeter', players: data.wr })}
                    className="xl:col-start-1 xl:row-start-1 xl:row-span-2"
                  />

                  <LineupGroup
                    label="TEs"
                    slot="Tight end side"
                    players={data.te}
                    focusPlayerId={focusPlayerId}
                    compareList={compareList}
                    onToggleCompare={onToggleCompare}
                    onViewHistory={(id) => {
                      onViewHistory(id, { detailPlayerId: null });
                      onClose();
                    }}
                    onOpenDetail={setDetailPlayer}
                    onOpenGroup={() => setExpandedGroup({ label: 'TEs', slot: 'Tight end side', players: data.te })}
                    className="xl:col-start-3 xl:row-start-1"
                  />

                  <div className="xl:col-span-3">
                    <DefenseColumn
                      defense={data.defense}
                      focusPlayerId={focusPlayerId}
                      compareList={compareList}
                      onToggleCompare={onToggleCompare}
                      onViewHistory={(id) => {
                        onViewHistory(id, { detailPlayerId: null });
                        onClose();
                      }}
                      onOpenDetail={setDetailPlayer}
                      onOpenGroup={setExpandedGroup}
                    />
                  </div>
                </div>
              </div>
            </div>
          ) : data && activeTab === 'builder' ? (
            <TeamBuilderView
              team={team}
              teamColor={teamColor}
              teamData={data}
              onOpenDetail={setDetailPlayer}
            />
          ) : null}
        </div>
      </div>

      {expandedGroup && (
        <PositionFocusModal
          group={expandedGroup}
          team={team}
          teamColor={teamColor}
          focusPlayerId={focusPlayerId}
          compareList={compareList}
          onToggleCompare={onToggleCompare}
          onViewHistory={(id) => {
            setExpandedGroup(null);
            onViewHistory(id, { detailPlayerId: null });
            onClose();
          }}
          onOpenDetail={setDetailPlayer}
          onClose={() => setExpandedGroup(null)}
        />
      )}

      {/* Stacked player detail modal */}
      {detailPlayer && (
        <PlayerDetailModal
          player={detailPlayer}
          isComparing={compareList.includes(detailPlayer.player_id)}
          onToggleCompare={onToggleCompare}
          onViewHistory={(id) => {
            // Capture which detail modal was open so App can restore it after
            // the user pops out of History.
            onViewHistory(id, { detailPlayerId: detailPlayer.player_id, activeTab });
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
