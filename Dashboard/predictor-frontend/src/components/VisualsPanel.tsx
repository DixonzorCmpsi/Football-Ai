import { useEffect, useState } from 'react';
import { Layers, Columns2, X } from 'lucide-react';
import PlayerPerformanceCharts from './PlayerPerformanceCharts';
import OverlayCharts from './OverlayCharts';
import PlayerRankChips from './PlayerRankChips';
import { seriesColor, useSeries, type Series } from './visualSeries';

/** Loads one compared player and hands the result up, so the overlay can chart everyone at once. */
function Loader({ id, season, onSeries }: { id: string; season: number | null; onSeries: (s: Series) => void }) {
  const { series } = useSeries(id, season);
  useEffect(() => {
    if (series) onSeries(series);
  }, [series, onSeries]);
  return null;
}

function Heading({ s, color, onRemove }: { s: Series; color: string; onRemove?: (id: string) => void }) {
  return (
    <div className="flex flex-wrap items-center gap-3 px-4 pt-4">
      <span className="w-3 h-3 rounded-full shrink-0" style={{ background: color }} />
      <h3 className="text-lg font-black text-slate-800 dark:text-slate-100">{s.name}</h3>
      <span className="text-xs font-bold text-slate-500 dark:text-slate-400">
        {s.position} · {s.team}
        {s.season != null ? ` · ${s.season}` : ''}
      </span>
      <span className="rounded bg-slate-700/80 px-1">
        <PlayerRankChips playerId={s.id} />
      </span>
      {onRemove && (
        <button
          onClick={() => onRemove(s.id)}
          aria-label={`Remove ${s.name} from comparison`}
          className="ml-auto p-1 rounded-full text-slate-400 hover:text-slate-700 dark:hover:text-white hover:bg-slate-100 dark:hover:bg-slate-700 transition"
        >
          <X size={16} />
        </button>
      )}
    </div>
  );
}

/**
 * The Visual tab. Alone, a player gets his radar and weekly bars. With others picked for
 * comparison it adds a switch: each player's charts one under another, or every player
 * drawn over the same radar and bars, each in a colour of their own.
 */
export default function VisualsPanel({
  main,
  extraIds,
  onRemove,
}: {
  main: Series | null;
  extraIds: string[];
  onRemove: (id: string) => void;
}) {
  const [mode, setMode] = useState<'separate' | 'overlay'>('separate');
  const [loaded, setLoaded] = useState<Record<string, Series>>({});
  const season = main?.season ?? null;

  if (!main) {
    return <div className="p-16 text-center text-slate-400 animate-pulse font-bold">Loading...</div>;
  }

  const extras = extraIds.map((id) => loaded[id]).filter((s): s is Series => !!s);
  const all = [main, ...extras];

  const chartFor = (s: Series, i: number) => (
    <PlayerPerformanceCharts rows={s.rows} position={s.position || 'WR'} season={s.season ?? new Date().getFullYear()} color={seriesColor(i)} />
  );

  if (extraIds.length === 0) return <div>{chartFor(main, 0)}</div>;

  return (
    <div>
      {extraIds.map((id) => (
        <Loader
          key={id}
          id={id}
          season={season}
          onSeries={(s) => setLoaded((prev) => (prev[id] === s ? prev : { ...prev, [id]: s }))}
        />
      ))}

      <div className="flex flex-wrap items-center gap-3 px-4 pt-4">
        <div className="flex items-center gap-1 bg-slate-100 dark:bg-slate-800/60 p-1 rounded-lg border border-slate-200 dark:border-slate-700">
          {([
            ['separate', 'Side by side', Columns2],
            ['overlay', 'Overlay', Layers],
          ] as const).map(([m, label, Icon]) => (
            <button
              key={m}
              onClick={() => setMode(m)}
              className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-black transition ${
                mode === m
                  ? 'bg-white dark:bg-slate-700 text-blue-600 dark:text-blue-400 shadow-sm'
                  : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'
              }`}
            >
              <Icon size={13} /> {label}
            </button>
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-3 text-xs font-bold text-slate-600 dark:text-slate-300">
          {all.map((s, i) => (
            <span key={s.id} className="inline-flex items-center gap-1.5">
              <span className="w-3 h-3 rounded-full" style={{ background: seriesColor(i) }} />
              {s.name}
            </span>
          ))}
        </div>
      </div>

      {mode === 'overlay' ? (
        <>
          <OverlayCharts series={all} />
          <div className="flex flex-wrap gap-3 px-4 pb-4">
            {extras.map((s) => (
              <button
                key={s.id}
                onClick={() => onRemove(s.id)}
                className="inline-flex items-center gap-1 text-[11px] font-bold text-slate-500 hover:text-slate-800 dark:hover:text-white"
              >
                <X size={12} /> Remove {s.name}
              </button>
            ))}
          </div>
        </>
      ) : (
        <>
          <Heading s={main} color={seriesColor(0)} />
          {chartFor(main, 0)}
          {extras.map((s, i) => (
            <section key={s.id} className="border-t border-slate-200 dark:border-slate-700 mt-2">
              <Heading s={s} color={seriesColor(i + 1)} onRemove={onRemove} />
              {chartFor(s, i + 1)}
            </section>
          ))}
        </>
      )}
    </div>
  );
}
