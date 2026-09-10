import React, { useMemo, useState } from 'react';
import { Maximize2, X } from 'lucide-react';
import {
  ResponsiveContainer,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Cell,
  LabelList,
} from 'recharts';
import type { HistoryEntry } from '../hooks/useNflData';

interface Props {
  rows: HistoryEntry[];
  position: string;
  season: number;
  teamColor: string;
}

interface RadarAxisDef {
  key: keyof HistoryEntry;
  label: string;
  max: number;
}

// Five-axis "skeleton" per position group — the stats that best describe
// that position's usage/production, normalized against a rough per-game ceiling.
const RADAR_AXES: Record<string, RadarAxisDef[]> = {
  QB: [
    { key: 'passing_yds', label: 'Pass Yds', max: 350 },
    { key: 'passing_tds', label: 'Pass TDs', max: 3 },
    { key: 'pass_attempts', label: 'Attempts', max: 40 },
    { key: 'rushing_yds', label: 'Rush Yds', max: 40 },
    { key: 'points', label: 'Fantasy Pts', max: 30 },
  ],
  RB: [
    { key: 'rushing_yds', label: 'Rush Yds', max: 130 },
    { key: 'carries', label: 'Carries', max: 22 },
    { key: 'receptions', label: 'Receptions', max: 8 },
    { key: 'receiving_yds', label: 'Rec Yds', max: 70 },
    { key: 'points', label: 'Fantasy Pts', max: 30 },
  ],
  WR: [
    { key: 'targets', label: 'Targets', max: 12 },
    { key: 'receptions', label: 'Receptions', max: 10 },
    { key: 'receiving_yds', label: 'Rec Yds', max: 130 },
    { key: 'touchdowns', label: 'TDs', max: 2 },
    { key: 'points', label: 'Fantasy Pts', max: 30 },
  ],
  TE: [
    { key: 'targets', label: 'Targets', max: 9 },
    { key: 'receptions', label: 'Receptions', max: 7 },
    { key: 'receiving_yds', label: 'Rec Yds', max: 90 },
    { key: 'touchdowns', label: 'TDs', max: 2 },
    { key: 'points', label: 'Fantasy Pts', max: 30 },
  ],
};

// Fixed vibrant accent for the charts — deliberately NOT team color, since a
// black/silver team (Raiders, etc.) would render an invisible-looking chart.
const ACCENT = '#ef4444'; // red-500

// Three-tier, relative-to-THIS-player's-own-season-average color, shared with
// the history table so "green" means the same thing in both views: a game
// meaningfully above their own baseline, not a fixed league-wide point total
// (a 6 pt/game player having a 9 pt week is a good week for them).
const GREEN = '#10b981';
const AMBER = '#f59e0b';
const RED = '#ef4444';

export function pointColor(points: number, avg: number): string {
  if (avg <= 0) return AMBER;
  if (points >= avg * 1.15) return GREEN;
  if (points <= avg * 0.85) return RED;
  return AMBER;
}

// ───────────────────────────────────────── Lightweight modal shell

const ChartModal: React.FC<{ title: string; onClose: () => void; children: React.ReactNode }> = ({
  title,
  onClose,
  children,
}) => (
  <div
    className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
    onClick={onClose}
  >
    <div
      className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-2xl w-full max-w-3xl p-4"
      onClick={(e) => e.stopPropagation()}
    >
      <div className="flex items-center justify-between mb-3">
        <h4 className="text-sm font-black uppercase tracking-widest text-slate-600 dark:text-slate-300">{title}</h4>
        <button
          onClick={onClose}
          className="w-7 h-7 rounded-md flex items-center justify-center text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-700"
        >
          <X size={16} />
        </button>
      </div>
      {children}
    </div>
  </div>
);

const ExpandButton: React.FC<{ onClick: () => void }> = ({ onClick }) => (
  <button
    onClick={onClick}
    className="w-6 h-6 rounded-md flex items-center justify-center text-slate-400 hover:text-blue-600 hover:bg-slate-100 dark:hover:bg-slate-700 transition"
    title="Expand"
  >
    <Maximize2 size={12} />
  </button>
);

const PlayerPerformanceCharts: React.FC<Props> = ({ rows, position, season }) => {
  const [expanded, setExpanded] = useState<'radar' | 'bar' | null>(null);
  const [selectedWeek, setSelectedWeek] = useState<HistoryEntry | null>(null);

  const playedRows = useMemo(() => rows.filter((r) => (r.snap_percentage || 0) > 0 || (r.points || 0) > 0), [rows]);

  const axes = RADAR_AXES[position] || RADAR_AXES.WR;

  const radarData = useMemo(() => {
    if (playedRows.length === 0) return axes.map((a) => ({ axis: a.label, value: 0, raw: 0 }));
    return axes.map((a) => {
      const total = playedRows.reduce((acc, r) => acc + (Number(r[a.key]) || 0), 0);
      const avg = total / playedRows.length;
      return { axis: a.label, value: Math.min(100, Math.round((avg / a.max) * 100)), raw: avg };
    });
  }, [playedRows, axes]);

  const avgPoints = useMemo(
    () => (playedRows.length > 0 ? playedRows.reduce((acc, r) => acc + (r.points || 0), 0) / playedRows.length : 0),
    [playedRows],
  );

  const barData = useMemo(
    () =>
      [...rows]
        .sort((a, b) => a.week - b.week)
        .map((r) => ({ week: `Wk ${r.week}`, points: r.points || 0, row: r })),
    [rows],
  );

  if (playedRows.length === 0) {
    return (
      <div className="p-16 text-center text-slate-400 dark:text-slate-500 font-bold">
        No {season} games to visualize yet.
      </div>
    );
  }

  // A low-volume player's polygon can look tiny against the fixed per-metric
  // ceiling (that's a true reading of light usage, not a bug) — printing the
  // raw number at each vertex keeps the chart legible even when the shape
  // itself is small, rather than relying on eyeballing a faint pentagon.
  const radarVertexLabel = (props: any) => {
    const { x, y, index } = props;
    const raw = radarData[index]?.raw;
    if (raw == null) return null;
    return (
      <text x={x} y={y} dy={-8} textAnchor="middle" fontSize={11} fontWeight={800} fill={ACCENT}>
        {raw.toFixed(1)}
      </text>
    );
  };

  const RadarViz = ({ height }: { height: number }) => (
    <ResponsiveContainer width="100%" height={height}>
      <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="82%">
        <PolarGrid stroke="rgba(148,163,184,0.3)" />
        <PolarAngleAxis dataKey="axis" tick={{ fill: '#94a3b8', fontSize: 12, fontWeight: 700 }} />
        <PolarRadiusAxis angle={90} domain={[0, 100]} tick={false} axisLine={false} />
        <Radar
          name="Per-game avg"
          dataKey="value"
          stroke={ACCENT}
          fill={ACCENT}
          fillOpacity={0.45}
          strokeWidth={2.5}
          dot={{ r: 3, fill: ACCENT, stroke: '#fff', strokeWidth: 1 }}
          label={radarVertexLabel}
        />
        <Tooltip
          contentStyle={{ background: '#0f172a', border: '1px solid #1e293b', borderRadius: 6 }}
          labelStyle={{ color: '#f1f5f9' }}
          formatter={(_value, _name, item) => {
            const d = item?.payload as { axis?: string; raw?: number } | undefined;
            return [d?.raw != null ? d.raw.toFixed(1) : '-', d?.axis || ''];
          }}
        />
      </RadarChart>
    </ResponsiveContainer>
  );

  const BarViz = ({ height }: { height: number }) => (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={barData} margin={{ top: 20, right: 8, left: -20, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.2)" vertical={false} />
        <XAxis dataKey="week" tick={{ fill: '#94a3b8', fontSize: 10 }} axisLine={false} tickLine={false} />
        <YAxis tick={{ fill: '#94a3b8', fontSize: 10 }} axisLine={false} tickLine={false} />
        <Tooltip
          cursor={{ fill: 'rgba(148,163,184,0.1)' }}
          contentStyle={{ background: '#0f172a', border: '1px solid #1e293b', borderRadius: 6 }}
          labelStyle={{ color: '#f1f5f9' }}
          formatter={(value) => [Number(value ?? 0).toFixed(1), 'Pts'] as [string, string]}
        />
        <Bar
          dataKey="points"
          radius={[4, 4, 0, 0]}
          onClick={(d: any) => d?.row && setSelectedWeek(d.row)}
          cursor="pointer"
        >
          <LabelList
            dataKey="points"
            position="top"
            formatter={(v) => { const n = Number(v ?? 0); return n > 0 ? n.toFixed(1) : ''; }}
            style={{ fill: '#64748b', fontSize: 10, fontWeight: 700 }}
          />
          {barData.map((d, i) => (
            <Cell key={i} fill={pointColor(d.points, avgPoints)} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );

  return (
    <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)] p-4">
      {/* Radar "skeleton" of the position's core production */}
      <div className="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
        <div className="flex items-center justify-between mb-2">
          <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
            {position} skeleton · {season} per-game average
          </h4>
          <ExpandButton onClick={() => setExpanded('radar')} />
        </div>
        <RadarViz height={300} />
      </div>

      {/* Weekly fantasy points across the season */}
      <div className="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
        <div className="flex items-center justify-between mb-2">
          <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
            Weekly fantasy points · {season}
          </h4>
          <ExpandButton onClick={() => setExpanded('bar')} />
        </div>
        <BarViz height={260} />
        <div className="flex items-center justify-center gap-3 mt-1 text-[10px] text-slate-400 dark:text-slate-500">
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full" style={{ background: GREEN }} /> Above avg</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full" style={{ background: AMBER }} /> Near avg</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full" style={{ background: RED }} /> Below avg</span>
        </div>
        <p className="text-[10px] text-slate-400 dark:text-slate-500 text-center italic mt-1">
          Click a bar for that week's box score
        </p>
      </div>

      {expanded === 'radar' && (
        <ChartModal title={`${position} skeleton · ${season}`} onClose={() => setExpanded(null)}>
          <RadarViz height={460} />
        </ChartModal>
      )}
      {expanded === 'bar' && (
        <ChartModal title={`Weekly fantasy points · ${season}`} onClose={() => setExpanded(null)}>
          <BarViz height={420} />
        </ChartModal>
      )}

      {selectedWeek && (
        <ChartModal title={`Week ${selectedWeek.week} vs ${selectedWeek.opponent}`} onClose={() => setSelectedWeek(null)}>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {[
              { label: 'Fantasy Pts', value: selectedWeek.points?.toFixed(1) ?? '0.0' },
              { label: 'Snap %', value: `${Math.round((selectedWeek.snap_percentage || 0) * 100)}%` },
              { label: 'Rec / Tgt', value: `${selectedWeek.receptions ?? 0} / ${selectedWeek.targets ?? 0}` },
              { label: 'Rush Yds / Att', value: `${selectedWeek.rushing_yds ?? '-'} / ${selectedWeek.carries ?? '-'}` },
              { label: 'Pass Yds', value: selectedWeek.passing_yds || '-' },
              { label: 'Rec Yds', value: selectedWeek.receiving_yds || '-' },
              { label: 'TDs', value: selectedWeek.touchdowns || 0 },
              { label: 'Opponent', value: selectedWeek.opponent },
            ].map((s) => (
              <div key={s.label} className="bg-slate-50 dark:bg-slate-900 rounded-lg p-3 border border-slate-200 dark:border-slate-700">
                <p className="text-[10px] font-bold uppercase tracking-wider text-slate-400 dark:text-slate-500">{s.label}</p>
                <p className="text-lg font-black text-slate-800 dark:text-slate-100 mt-0.5">{s.value}</p>
              </div>
            ))}
          </div>
        </ChartModal>
      )}
    </div>
  );
};

export default PlayerPerformanceCharts;
