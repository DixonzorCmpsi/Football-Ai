import React, { useMemo } from 'react';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ScatterChart,
  Scatter,
  ZAxis,
  Cell,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
  Legend,
  ReferenceLine,
} from 'recharts';
import type { PoolPlayer, Tier } from '../hooks/useNflData';
import { TIERS } from '../hooks/useNflData';

interface Props {
  pool: PoolPlayer[];
  assignments: Record<string, Tier>;
  position: 'ALL' | 'QB' | 'RB' | 'WR' | 'TE';
}

const TIER_COLORS: Record<Tier, string> = {
  S: '#f59e0b',
  A: '#ef4444',
  B: '#fb923c',
  C: '#10b981',
  D: '#3b82f6',
  F: '#64748b',
  UNRANKED: '#cbd5e1',
};

// Compute percentile rank of value in sorted (ascending) array.
function percentile(sortedAsc: number[], value: number): number {
  if (sortedAsc.length === 0) return 0;
  let lo = 0;
  let hi = sortedAsc.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (sortedAsc[mid] < value) lo = mid + 1;
    else hi = mid;
  }
  return Math.round((lo / sortedAsc.length) * 100);
}

const StatCard: React.FC<{ label: string; value: string; sub?: string; color?: string }> = ({
  label,
  value,
  sub,
  color,
}) => (
  <div className="bg-white dark:bg-slate-800 rounded-lg p-3 border border-slate-200 dark:border-slate-700 shadow-sm">
    <p className="text-[10px] font-bold uppercase tracking-wider text-slate-400 dark:text-slate-500">
      {label}
    </p>
    <p className={`text-lg font-black mt-0.5 ${color || 'text-slate-800 dark:text-slate-100'}`}>
      {value}
    </p>
    {sub && <p className="text-[10px] text-slate-400 dark:text-slate-500 mt-0.5">{sub}</p>}
  </div>
);

const TierVisualizations: React.FC<Props> = ({ pool, assignments, position }) => {
  const tierCounts = useMemo(() => {
    const counts: Record<Tier, number> = {
      S: 0,
      A: 0,
      B: 0,
      C: 0,
      D: 0,
      F: 0,
      UNRANKED: 0,
    };
    pool.forEach((p) => {
      const t = assignments[p.player_id] || 'UNRANKED';
      counts[t] += 1;
    });
    return TIERS.filter((t) => t !== 'UNRANKED').map((t) => ({
      tier: t,
      count: counts[t],
      avgPpg:
        pool
          .filter((p) => (assignments[p.player_id] || 'UNRANKED') === t)
          .reduce((acc, p) => acc + (p.stats.season_avg_pts || 0), 0) /
        Math.max(counts[t], 1),
    }));
  }, [pool, assignments]);

  const ranked = useMemo(() => {
    const tiered = pool.filter((p) => assignments[p.player_id]);
    return tiered.sort((a, b) => {
      const tierIdx = (t: Tier) => TIERS.indexOf(t);
      const aT = assignments[a.player_id];
      const bT = assignments[b.player_id];
      const diff = tierIdx(aT) - tierIdx(bT);
      if (diff !== 0) return diff;
      return b.stats.season_avg_pts - a.stats.season_avg_pts;
    });
  }, [pool, assignments]);

  const scatter = useMemo(() => {
    return pool
      .filter((p) => p.stats.games_played > 0)
      .map((p) => ({
        x: p.stats.snap_pct_avg || 0,
        y: p.stats.season_avg_pts || 0,
        z: p.stats.games_played,
        name: p.player_name,
        team: p.team,
        rookie: p.is_rookie,
        tier: (assignments[p.player_id] || 'UNRANKED') as Tier,
      }));
  }, [pool, assignments]);

  // Top 3 from S/A combined for radar.
  const radarTop = useMemo(() => {
    const elite = ranked
      .filter(
        (p) => assignments[p.player_id] === 'S' || assignments[p.player_id] === 'A',
      )
      .slice(0, 3);

    if (elite.length === 0) return null;

    const totalsAsc = (key: keyof PoolPlayer['stats']) =>
      [...pool.map((p) => Number(p.stats[key]) || 0)].sort((a, b) => a - b);
    const ppgArr = totalsAsc('season_avg_pts');
    const recentArr = totalsAsc('recent_avg_pts');
    const tdArr = totalsAsc('total_tds');
    const ydsArr = totalsAsc('total_yds');
    const snapArr = totalsAsc('snap_pct_avg');
    const boomArr = totalsAsc('boom_games');

    return elite.map((p, idx) => ({
      name: p.player_name,
      color: ['#3b82f6', '#ef4444', '#10b981'][idx],
      data: [
        { axis: 'PPG', value: percentile(ppgArr, p.stats.season_avg_pts) },
        { axis: 'Recent', value: percentile(recentArr, p.stats.recent_avg_pts) },
        { axis: 'TDs', value: percentile(tdArr, p.stats.total_tds) },
        { axis: 'Yards', value: percentile(ydsArr, p.stats.total_yds) },
        { axis: 'Snap %', value: percentile(snapArr, p.stats.snap_pct_avg) },
        { axis: 'Boom', value: percentile(boomArr, p.stats.boom_games) },
      ],
    }));
  }, [ranked, pool, assignments]);

  const radarMerged = useMemo(() => {
    if (!radarTop || radarTop.length === 0) return [];
    const axes = radarTop[0].data.map((d) => d.axis);
    return axes.map((axis) => {
      const obj: Record<string, number | string> = { axis };
      radarTop.forEach((p) => {
        const v = p.data.find((d) => d.axis === axis)?.value || 0;
        obj[p.name] = v;
      });
      return obj;
    });
  }, [radarTop]);

  const summary = useMemo(() => {
    const total = pool.length;
    const tieredCount = pool.filter((p) => assignments[p.player_id]).length;
    const eliteCount = pool.filter(
      (p) => assignments[p.player_id] === 'S' || assignments[p.player_id] === 'A',
    ).length;
    const avgPpg =
      pool.reduce((acc, p) => acc + (p.stats.season_avg_pts || 0), 0) / Math.max(total, 1);
    const rookies = pool.filter((p) => p.is_rookie).length;
    return { total, tieredCount, eliteCount, avgPpg, rookies };
  }, [pool, assignments]);

  if (pool.length === 0) return null;

  return (
    <div className="bg-slate-50 dark:bg-slate-900/40 border border-slate-200 dark:border-slate-700/60 rounded-xl p-4 mb-4">
      <div className="grid grid-cols-2 sm:grid-cols-5 gap-2 mb-4">
        <StatCard label="Pool" value={String(summary.total)} sub={`${position} active`} />
        <StatCard
          label="Tiered"
          value={String(summary.tieredCount)}
          sub={`${Math.round((summary.tieredCount / Math.max(summary.total, 1)) * 100)}%`}
          color="text-blue-600 dark:text-blue-400"
        />
        <StatCard
          label="Elite (S/A)"
          value={String(summary.eliteCount)}
          color="text-amber-600 dark:text-amber-400"
        />
        <StatCard
          label="Rookies"
          value={String(summary.rookies)}
          color="text-purple-600 dark:text-purple-400"
        />
        <StatCard label="Pool avg PPG" value={summary.avgPpg.toFixed(2)} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Tier distribution */}
        <div className="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
          <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400 mb-2">
            Tier distribution
          </h4>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={tierCounts}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.2)" />
              <XAxis dataKey="tier" stroke="#94a3b8" fontSize={11} />
              <YAxis stroke="#94a3b8" fontSize={11} allowDecimals={false} />
              <Tooltip
                contentStyle={{ background: '#0f172a', border: '1px solid #1e293b', borderRadius: 6 }}
                labelStyle={{ color: '#f1f5f9' }}
                formatter={(_value, _name, item) => {
                  const datum = item?.payload as { count?: number; avgPpg?: number } | undefined;
                  const count = datum?.count ?? 0;
                  const avg = datum?.avgPpg ?? 0;
                  return [`${count} (${avg.toFixed(1)} PPG avg)`, 'Players'];
                }}
              />
              <Bar dataKey="count">
                {tierCounts.map((entry) => (
                  <Cell key={entry.tier} fill={TIER_COLORS[entry.tier]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Snap% vs PPG scatter */}
        <div className="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
          <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400 mb-2">
            Snap % vs PPG (rookies highlighted)
          </h4>
          <ResponsiveContainer width="100%" height={180}>
            <ScatterChart>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.2)" />
              <XAxis
                type="number"
                dataKey="x"
                name="Snap %"
                unit="%"
                stroke="#94a3b8"
                fontSize={11}
                domain={[0, 100]}
              />
              <YAxis type="number" dataKey="y" name="PPG" stroke="#94a3b8" fontSize={11} />
              <ZAxis type="number" dataKey="z" range={[40, 240]} name="games" />
              <Tooltip
                cursor={{ strokeDasharray: '3 3' }}
                contentStyle={{ background: '#0f172a', border: '1px solid #1e293b', borderRadius: 6 }}
                labelStyle={{ color: '#f1f5f9' }}
                formatter={(_value, _name, item) => {
                  const d = item?.payload as
                    | { name?: string; team?: string; tier?: Tier; rookie?: boolean }
                    | undefined;
                  if (!d) return ['', ''];
                  return [`${d.name} (${d.team})${d.rookie ? ' R' : ''} • ${d.tier}`, ''];
                }}
              />
              <Scatter data={scatter}>
                {scatter.map((d, i) => (
                  <Cell
                    key={i}
                    fill={d.rookie ? '#a855f7' : TIER_COLORS[d.tier]}
                    stroke={d.rookie ? '#7e22ce' : 'none'}
                    strokeWidth={d.rookie ? 1.5 : 0}
                  />
                ))}
              </Scatter>
            </ScatterChart>
          </ResponsiveContainer>
        </div>

        {/* Radar of top elite */}
        <div className="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
          <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400 mb-2">
            Top S/A radar (percentile vs pool)
          </h4>
          {radarTop && radarTop.length > 0 ? (
            <ResponsiveContainer width="100%" height={180}>
              <RadarChart data={radarMerged} cx="50%" cy="50%" outerRadius="75%">
                <PolarGrid stroke="rgba(148,163,184,0.3)" />
                <PolarAngleAxis dataKey="axis" tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <PolarRadiusAxis angle={90} domain={[0, 100]} tick={false} axisLine={false} />
                {radarTop.map((p) => (
                  <Radar
                    key={p.name}
                    name={p.name}
                    dataKey={p.name}
                    stroke={p.color}
                    fill={p.color}
                    fillOpacity={0.18}
                  />
                ))}
                <Legend wrapperStyle={{ fontSize: 10 }} />
              </RadarChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex items-center justify-center h-[180px] text-xs text-slate-400 italic">
              Tier players to S/A to compare percentile profiles
            </div>
          )}
        </div>

        {/* Per-player percentile bars (recent form vs season) */}
        <div className="lg:col-span-3 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
          <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400 mb-2">
            Recent form vs season avg (top 12 in your tiers)
          </h4>
          <ResponsiveContainer width="100%" height={Math.max(240, 16 * Math.min(ranked.length, 12) + 40)}>
            <BarChart
              data={ranked.slice(0, 12).map((p) => ({
                name: p.player_name,
                season: p.stats.season_avg_pts,
                recent: p.stats.recent_avg_pts,
                tier: assignments[p.player_id],
              }))}
              layout="vertical"
              margin={{ left: 80 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.2)" />
              <XAxis type="number" stroke="#94a3b8" fontSize={11} />
              <YAxis dataKey="name" type="category" stroke="#94a3b8" fontSize={10} width={100} />
              <Tooltip
                contentStyle={{ background: '#0f172a', border: '1px solid #1e293b', borderRadius: 6 }}
                labelStyle={{ color: '#f1f5f9' }}
              />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <ReferenceLine x={summary.avgPpg} stroke="#a78bfa" strokeDasharray="3 3" />
              <Bar dataKey="season" fill="#3b82f6" name="Season avg" />
              <Bar dataKey="recent" fill="#f97316" name="Recent (last 4)" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
};

export default TierVisualizations;
