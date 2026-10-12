import { useMemo, useState } from 'react';
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
  LabelList,
} from 'recharts';
import { ChartModal, ExpandButton, RADAR_AXES } from './PlayerPerformanceCharts';
import { seriesColor, type Series } from './visualSeries';

const TOOLTIP_STYLE = { background: '#0f172a', border: '1px solid #1e293b', borderRadius: 6 };
// A number in the player's colour, with a dark halo so it reads on any fill or theme.
const haloStyle = (color: string) => ({ fill: color, stroke: '#0f172a', strokeWidth: 3, paintOrder: 'stroke' as const, strokeLinejoin: 'round' as const });

const played = (s: Series) => s.rows.filter((r) => (r.snap_percentage || 0) > 0 || (r.points || 0) > 0);

/** Two or more players on one radar and one set of weekly bars, each in their own colour. */
export default function OverlayCharts({ series }: { series: Series[] }) {
  const [expanded, setExpanded] = useState<'radar' | 'bar' | null>(null);

  const lead = series[0];
  const axes = RADAR_AXES[lead.position] || RADAR_AXES.WR;
  const mixedPositions = series.some((s) => s.position !== lead.position);

  const radarData = useMemo(
    () =>
      axes.map((a) => {
        const point: Record<string, number | string> = { axis: a.label };
        series.forEach((s, i) => {
          const rows = played(s);
          const avg = rows.length ? rows.reduce((acc, r) => acc + (Number(r[a.key]) || 0), 0) / rows.length : 0;
          point[`v${i}`] = Math.min(100, Math.round((avg / a.max) * 100));
          point[`r${i}`] = avg;
        });
        return point;
      }),
    [axes, series],
  );

  const barData = useMemo(() => {
    const weeks = [...new Set(series.flatMap((s) => s.rows.map((r) => r.week)))].sort((a, b) => a - b);
    return weeks.map((w) => {
      const point: Record<string, number | string | undefined> = { week: `Wk ${w}` };
      series.forEach((s, i) => {
        const row = s.rows.find((r) => r.week === w);
        point[`p${i}`] = row && ((row.snap_percentage || 0) > 0 || (row.points || 0) > 0) ? row.points || 0 : undefined;
      });
      return point;
    });
  }, [series]);

  // Each axis is named, with every player's number for it underneath in that player's own
  // colour. Numbers on the polygon itself collide whenever players are close; down here
  // they never do, and the colour says whose is whose.
  const axisTick = (props: { x?: number | string; y?: number | string; textAnchor?: string; index?: number; payload?: { value?: string } }) => {
    const x = Number(props.x);
    const y = Number(props.y);
    const point = radarData[props.index ?? -1];
    return (
      <text x={x} y={y} textAnchor={(props.textAnchor as 'start' | 'middle' | 'end') ?? 'middle'} fontSize={12} fontWeight={700} fill="#94a3b8">
        <tspan x={x} dy={0}>{props.payload?.value}</tspan>
        <tspan x={x} dy={15}>
          {series.map((s, i) => {
            const raw = point?.[`r${i}`];
            return (
              <tspan key={s.id} dx={i === 0 ? 0 : 7} fontWeight={800} style={haloStyle(seriesColor(i))}>
                {typeof raw === 'number' ? raw.toFixed(1) : '-'}
              </tspan>
            );
          })}
        </tspan>
      </text>
    );
  };

  const radarViz = (height: number) => (
    <ResponsiveContainer width="100%" height={height}>
      <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="68%" margin={{ top: 20, right: 56, bottom: 20, left: 56 }}>
        <PolarGrid stroke="rgba(148,163,184,0.3)" />
        <PolarAngleAxis dataKey="axis" tick={axisTick} />
        <PolarRadiusAxis angle={90} domain={[0, 100]} tick={false} axisLine={false} />
        {series.map((s, i) => {
          const color = seriesColor(i);
          return (
            <Radar
              key={s.id}
              name={s.name}
              dataKey={`v${i}`}
              stroke={color}
              fill={color}
              fillOpacity={0.2}
              strokeWidth={2.5}
              dot={{ r: 3, fill: color, stroke: '#fff', strokeWidth: 1 }}
            />
          );
        })}
        <Tooltip
          contentStyle={TOOLTIP_STYLE}
          labelStyle={{ color: '#f1f5f9' }}
          formatter={(_value, name, item) => {
            const i = series.findIndex((s) => s.name === name);
            const raw = (item?.payload as Record<string, number> | undefined)?.[`r${i}`];
            return [raw != null ? raw.toFixed(1) : '-', String(name)];
          }}
        />
      </RadarChart>
    </ResponsiveContainer>
  );

  const barViz = (height: number) => (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={barData} margin={{ top: 20, right: 8, left: -20, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.2)" vertical={false} />
        <XAxis dataKey="week" tick={{ fill: '#94a3b8', fontSize: 10 }} axisLine={false} tickLine={false} />
        <YAxis tick={{ fill: '#94a3b8', fontSize: 10 }} axisLine={false} tickLine={false} />
        <Tooltip
          cursor={{ fill: 'rgba(148,163,184,0.1)' }}
          contentStyle={TOOLTIP_STYLE}
          labelStyle={{ color: '#f1f5f9' }}
          formatter={(value, name) => [Number(value ?? 0).toFixed(1), String(name)] as [string, string]}
        />
        {series.map((s, i) => (
          <Bar key={s.id} name={s.name} dataKey={`p${i}`} fill={seriesColor(i)} radius={[4, 4, 0, 0]} maxBarSize={28}>
            <LabelList
              dataKey={`p${i}`}
              position="top"
              formatter={(v) => (v == null ? '' : Number(v).toFixed(1))}
              style={{ ...haloStyle(seriesColor(i)), strokeWidth: 2, fontSize: 10, fontWeight: 800 }}
            />
          </Bar>
        ))}
      </BarChart>
    </ResponsiveContainer>
  );

  const seasons = [...new Set(series.map((s) => s.season).filter((x): x is number => x != null))];
  const seasonLabel = seasons.length === 1 ? String(seasons[0]) : seasons.join(' / ');

  return (
    <div className="p-4 space-y-3">
      {mixedPositions && (
        <p className="text-[11px] text-amber-600 dark:text-amber-400 font-bold">
          These players are at different positions, so every radar uses {lead.name}'s {lead.position} axes.
        </p>
      )}
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)]">
        <div className="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
          <div className="flex items-center justify-between mb-2">
            <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
              {lead.position} skeleton · {seasonLabel} per-game average
            </h4>
            <ExpandButton onClick={() => setExpanded('radar')} />
          </div>
          {radarViz(320)}
        </div>
        <div className="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
          <div className="flex items-center justify-between mb-2">
            <h4 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
              Weekly fantasy points · {seasonLabel}
            </h4>
            <ExpandButton onClick={() => setExpanded('bar')} />
          </div>
          {barViz(280)}
        </div>
      </div>

      {expanded === 'radar' && (
        <ChartModal title={`${lead.position} skeleton · ${seasonLabel}`} onClose={() => setExpanded(null)}>
          {radarViz(460)}
        </ChartModal>
      )}
      {expanded === 'bar' && (
        <ChartModal title={`Weekly fantasy points · ${seasonLabel}`} onClose={() => setExpanded(null)}>
          {barViz(420)}
        </ChartModal>
      )}
    </div>
  );
}
