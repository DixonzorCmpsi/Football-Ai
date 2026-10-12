import { useEffect, useMemo, useState } from 'react';
import { API_BASE_URL } from '../lib/api';
import type { PlayerData } from '../types';
import { formatOdds, groupProps } from '../utils/playerProps';

const fmtSigned = (n: number | null | undefined) => (n === null || n === undefined ? '-' : n > 0 ? `+${n}` : `${n}`);

/** The betting market for one player: this game's line, then every prop Bovada has posted. */
const PlayerVegas: React.FC<{ playerId: string }> = ({ playerId }) => {
  const [player, setPlayer] = useState<PlayerData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showAlternates, setShowAlternates] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setPlayer(null);
    setError(null);
    fetch(`${API_BASE_URL}/player/${playerId}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((d) => !cancelled && setPlayer(d))
      .catch((e) => !cancelled && setError(String(e.message || e)));
    return () => { cancelled = true; };
  }, [playerId]);

  const markets = useMemo(() => groupProps(player?.props), [player?.props]);
  const alternates = markets.filter((m) => m.alternate).length;
  const shownMarkets = showAlternates ? markets : markets.filter((m) => !m.alternate);
  const hasUnders = markets.some((m) => m.under);

  if (error) {
    return (
      <div className="p-10 text-center">
        <p className="text-sm font-black text-red-500">Couldn't load the betting lines</p>
        <p className="text-xs text-slate-400 mt-1">{error}</p>
      </div>
    );
  }
  if (!player) {
    return <div className="p-20 text-center text-slate-400 dark:text-slate-500 animate-pulse font-bold">Loading Vegas data...</div>;
  }

  return (
    <div className="p-4 sm:p-6 space-y-5" data-testid="player-modal-vegas">
      <div>
        <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-500 mb-2">
          Game line · {player.team} vs {player.opponent || '?'}
        </h3>
        {player.overunder == null && player.spread == null ? (
          <p className="text-sm text-slate-500">No line posted for this game yet.</p>
        ) : (
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            {[
              { label: 'Spread', value: fmtSigned(player.spread) },
              { label: 'Over/Under', value: player.overunder ?? '-' },
              { label: 'Team total', value: player.implied_total ?? '-' },
              { label: 'Moneyline', value: formatOdds(player.moneyline) },
            ].map((c) => (
              <div key={c.label} className="rounded-xl border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900 p-3">
                <div className="text-[10px] font-bold uppercase text-slate-400">{c.label}</div>
                <div className="text-xl font-black font-mono text-slate-900 dark:text-white">{c.value}</div>
              </div>
            ))}
          </div>
        )}
        {player.lines_source === 'schedule' && (
          <p className="text-[10px] text-slate-400 mt-1.5">Bovada has no board for this game, so this is the consensus line from the NFL schedule feed.</p>
        )}
      </div>

      <div>
        <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-500 mb-2">Player props (Bovada)</h3>
        {markets.length === 0 ? (
          <p className="text-sm text-slate-500">
            No props for {player.player_name} this week. Bovada posts them a few days before kickoff and takes them down once a game starts.
          </p>
        ) : (
          <div className="overflow-x-auto rounded-xl border border-slate-200 dark:border-slate-800">
            <table className="w-full text-sm whitespace-nowrap">
              <thead className="text-[10px] uppercase bg-slate-50 dark:bg-slate-900 text-slate-500 font-bold">
                <tr>
                  <th className="px-3 py-2 text-left">Market</th>
                  <th className="px-3 py-2 text-right">Line</th>
                  <th className="px-3 py-2 text-right">Over</th>
                  {hasUnders && <th className="px-3 py-2 text-right">Under</th>}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                {shownMarkets.map((m) => (
                  <tr key={`${m.prop_type}-${m.line}`} data-testid="player-modal-prop">
                    <td className="px-3 py-2 font-bold">{m.prop_type}</td>
                    <td className="px-3 py-2 text-right font-mono">{m.yes ? '-' : m.line ?? '-'}</td>
                    {m.yes ? (
                      <td className="px-3 py-2 text-right font-mono" colSpan={hasUnders ? 2 : 1}>
                        {formatOdds(m.yes.odds)} <span className="text-slate-400">· {m.yes.prob ?? '-'}%</span>
                      </td>
                    ) : (
                      <>
                        <td className="px-3 py-2 text-right font-mono">
                          {m.over ? <>{formatOdds(m.over.odds)} <span className="text-slate-400">· {m.over.prob ?? '-'}%</span></> : '-'}
                        </td>
                        {hasUnders && (
                          <td className="px-3 py-2 text-right font-mono">
                            {m.under ? <>{formatOdds(m.under.odds)} <span className="text-slate-400">· {m.under.prob ?? '-'}%</span></> : '-'}
                          </td>
                        )}
                      </>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {alternates > 0 && (
          <button
            type="button"
            onClick={() => setShowAlternates((v) => !v)}
            className="mt-2 text-xs font-bold text-blue-600 dark:text-blue-400 hover:underline"
          >
            {showAlternates ? 'Hide alternate lines' : `Show ${alternates} alternate line${alternates === 1 ? '' : 's'}`}
          </button>
        )}
        {markets.length > 0 && !hasUnders && (
          <p className="text-[10px] text-slate-400 mt-1.5">Bovada only posted the over side for these markets.</p>
        )}
      </div>
    </div>
  );
};

export default PlayerVegas;
