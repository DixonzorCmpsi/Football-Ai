import { useEffect, useState } from 'react';
import { Check, ListOrdered, Plus } from 'lucide-react';
import { API_BASE_URL } from '../lib/api';

interface DepthPlayer {
  player_id: string | null;
  name: string;
  rank: number;
}
interface DepthSlot {
  pos: string;
  players: DepthPlayer[];
}
interface DepthChart {
  team: string;
  as_of: string | null;
  offense: DepthSlot[];
  defense: DepthSlot[];
}

const RANK_LABELS = ['Starter', '2nd', '3rd', '4th'];
const ordinal = (n: number) => (n === 1 ? 'starter' : n === 2 ? '2nd string' : n === 3 ? '3rd string' : `${n}th string`);

/** Every place the player appears on the chart, e.g. RB / 3rd string. */
function findSelf(chart: DepthChart, playerId: string): { pos: string; rank: number }[] {
  const hits: { pos: string; rank: number }[] = [];
  [...chart.offense, ...chart.defense].forEach((slot) =>
    slot.players.forEach((p) => {
      if (p.player_id === playerId) hits.push({ pos: slot.pos, rank: p.rank });
    }),
  );
  return hits;
}

function Section({
  title,
  slots,
  playerId,
  onSelect,
  teamColor,
  compareList,
  onToggleCompare,
}: {
  title: string;
  slots: DepthSlot[];
  playerId: string;
  onSelect: (id: string) => void;
  teamColor: string;
  compareList: string[];
  onToggleCompare: (id: string) => void;
}) {
  if (slots.length === 0) return null;
  const cols = Math.min(Math.max(...slots.map((s) => s.players.length), 3), RANK_LABELS.length);
  return (
    <div className="mb-6">
      <h3 className="px-4 pb-2 text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
        {title}
      </h3>
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left whitespace-nowrap">
          <thead className="bg-slate-50 dark:bg-slate-900/50 text-[10px] uppercase tracking-wider text-slate-500 dark:text-slate-400">
            <tr>
              <th className="px-3 py-2 w-14">Pos</th>
              {Array.from({ length: cols }, (_, i) => (
                <th key={i} className="px-3 py-2">{RANK_LABELS[i]}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100 dark:divide-slate-700/60">
            {slots.map((slot) => (
              <tr key={slot.pos}>
                <td className="px-3 py-2 font-black text-slate-500 dark:text-slate-400">{slot.pos}</td>
                {Array.from({ length: cols }, (_, i) => {
                  const p = slot.players[i];
                  if (!p) return <td key={i} />;
                  const isSelf = p.player_id === playerId;
                  const clickable = !!p.player_id && !isSelf;
                  const comparing = !!p.player_id && compareList.includes(p.player_id);
                  return (
                    <td key={i} className="px-3 py-2">
                      <div className="flex items-center gap-1.5">
                      {p.player_id && (
                        <button
                          type="button"
                          onClick={() => onToggleCompare(p.player_id as string)}
                          title={comparing ? 'Remove from comparison' : 'Add to comparison'}
                          aria-label={comparing ? `Remove ${p.name} from comparison` : `Add ${p.name} to comparison`}
                          className={`w-5 h-5 shrink-0 rounded-full flex items-center justify-center border transition ${
                            comparing
                              ? 'bg-blue-600 border-blue-500 text-white'
                              : 'border-slate-300 dark:border-slate-600 text-slate-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 hover:text-blue-600'
                          }`}
                        >
                          {comparing ? <Check size={11} strokeWidth={4} /> : <Plus size={11} strokeWidth={3} />}
                        </button>
                      )}
                      {clickable ? (
                        <button
                          type="button"
                          onClick={() => onSelect(p.player_id as string)}
                          className="font-bold text-blue-600 dark:text-blue-400 hover:underline"
                        >
                          {p.name}
                        </button>
                      ) : (
                        <span
                          className={isSelf ? 'font-black px-2 py-0.5 rounded text-white' : 'text-slate-500 dark:text-slate-400'}
                          style={isSelf ? { backgroundColor: teamColor } : undefined}
                        >
                          {p.name}
                        </span>
                      )}
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const PlayerDepthChart: React.FC<{
  team?: string;
  playerId: string;
  teamColor?: string;
  onSelectPlayer: (id: string) => void;
  compareList: string[];
  onToggleCompare: (id: string) => void;
}> = ({ team, playerId, teamColor = '#3b82f6', onSelectPlayer, compareList, onToggleCompare }) => {
  const [chart, setChart] = useState<DepthChart | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!team) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(`${API_BASE_URL}/team/${team}/depth-chart`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((d) => !cancelled && setChart(d))
      .catch((e) => !cancelled && setError(String(e.message || e)))
      .finally(() => !cancelled && setLoading(false));
    return () => { cancelled = true; };
  }, [team]);

  if (!team || loading) {
    return <div className="p-20 text-center text-slate-400 dark:text-slate-500 animate-pulse font-bold">Loading depth chart...</div>;
  }
  if (error) {
    return (
      <div className="p-10 text-center">
        <p className="text-sm font-black text-red-500">Couldn't load the depth chart</p>
        <p className="text-xs text-slate-400 mt-1">{error}</p>
      </div>
    );
  }
  if (!chart || (chart.offense.length === 0 && chart.defense.length === 0)) {
    return (
      <div className="p-12 text-center">
        <ListOrdered className="mx-auto text-slate-300 dark:text-slate-600" size={28} />
        <p className="text-sm font-black text-slate-400 dark:text-slate-500 mt-3">No depth chart on file for {team}</p>
      </div>
    );
  }

  const self = findSelf(chart, playerId);
  return (
    <div className="py-4">
      <div className="px-4 pb-4 flex flex-wrap items-center gap-2 text-xs">
        <span className="font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">{chart.team} depth chart</span>
        {self.length > 0 ? (
          self.map((h) => (
            <span key={h.pos} className="px-2 py-0.5 rounded font-black text-white" style={{ backgroundColor: teamColor }}>
              {h.pos} · {ordinal(h.rank)}
            </span>
          ))
        ) : (
          <span className="px-2 py-0.5 rounded font-bold bg-slate-100 dark:bg-slate-700 text-slate-500 dark:text-slate-300">
            Not on the depth chart
          </span>
        )}
        {chart.as_of && (
          <span className="ml-auto font-mono text-[10px] text-slate-400 dark:text-slate-500">
            feed as of {chart.as_of.slice(0, 10)}
          </span>
        )}
      </div>
      <Section title="Offense" slots={chart.offense} playerId={playerId} onSelect={onSelectPlayer} teamColor={teamColor} compareList={compareList} onToggleCompare={onToggleCompare} />
      <Section title="Defense" slots={chart.defense} playerId={playerId} onSelect={onSelectPlayer} teamColor={teamColor} compareList={compareList} onToggleCompare={onToggleCompare} />
    </div>
  );
};

export default PlayerDepthChart;
