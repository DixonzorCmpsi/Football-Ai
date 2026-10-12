import { useEffect, useMemo, useState } from 'react';
import { API_BASE_URL } from '../lib/api';
import type { PlayerData } from '../types';
import { formatOdds, groupProps, type PropMarket } from '../utils/playerProps';
import { usePlayerHistory, type HistoryEntry } from '../hooks/useNflData';

const fmtSigned = (n: number | null | undefined) => (n === null || n === undefined ? '-' : n > 0 ? `+${n}` : `${n}`);

interface KeyMarket {
  label: string;
  /** Matches the prop_type Bovada sends. */
  match: RegExp;
  /** What he actually did in a game, to compare with the line. */
  stat: (g: HistoryEntry) => number;
  scorer?: boolean;
}

const num = (v: number | null | undefined) => Number(v) || 0;
const RUSH_YDS: KeyMarket = { label: 'Rushing yards', match: /^rushing yards/i, stat: (g) => num(g.rushing_yds) };
const RUSH_ATT: KeyMarket = { label: 'Rush attempts', match: /rush(ing)? att/i, stat: (g) => num(g.carries) };
const RECS: KeyMarket = { label: 'Receptions', match: /^receptions/i, stat: (g) => num(g.receptions) };
const REC_YDS: KeyMarket = { label: 'Receiving yards', match: /^receiving yards/i, stat: (g) => num(g.receiving_yds) };
const ANYTIME: KeyMarket = { label: 'Anytime TD', match: /anytime/i, stat: (g) => num(g.touchdowns), scorer: true };

/** The markets that matter for each position, in the order a bettor reads them. */
const KEY_MARKETS: Record<string, KeyMarket[]> = {
  RB: [RUSH_YDS, RUSH_ATT, RECS, REC_YDS, ANYTIME],
  WR: [RECS, REC_YDS, ANYTIME],
  TE: [RECS, REC_YDS, ANYTIME],
  QB: [
    { label: 'Passing yards', match: /^passing yards/i, stat: (g) => num(g.passing_yds) },
    { label: 'Passing TDs', match: /^passing t(ouch)?d/i, stat: (g) => num(g.passing_tds) },
    { label: 'Pass attempts', match: /pass(ing)? att/i, stat: (g) => num(g.pass_attempts) },
    { label: 'Completions', match: /^completions/i, stat: (g) => num(g.completions) },
    RUSH_YDS,
    ANYTIME,
  ],
};

const mean = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const oneDecimal = (n: number | null) => (n == null ? '–' : n.toFixed(1));

/** The betting market for one player: this game's line, then every prop Bovada has posted. */
const PlayerVegas: React.FC<{ playerId: string }> = ({ playerId }) => {
  const [player, setPlayer] = useState<PlayerData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showAlternates, setShowAlternates] = useState(false);
  const { history } = usePlayerHistory(playerId);

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

  // His current season (the latest with games), to set each line against what he really does.
  const seasonRows = useMemo(() => {
    const played = history.filter((g) => g.season_type !== 'POST');
    const season = Math.max(0, ...played.map((g) => g.season));
    return played.filter((g) => g.season === season).sort((a, b) => a.week - b.week);
  }, [history]);

  const keyRows = useMemo(() => {
    const defs = KEY_MARKETS[player?.position ?? ''] ?? [];
    const taken = new Set<PropMarket>();
    const rows = defs.map((d) => {
      const candidates = markets.filter((m) => d.match.test(m.prop_type));
      const main = candidates.find((m) => !m.alternate) ?? candidates[0];
      candidates.forEach((m) => taken.add(m));
      const values = seasonRows.map(d.stat);
      const line = d.scorer ? null : main?.line ?? null;
      return {
        def: d,
        market: main,
        season: d.scorer ? mean(values.map((v) => (v > 0 ? 1 : 0))) : mean(values),
        last4: d.scorer ? null : mean(values.slice(-4)),
        hit: line != null && values.length ? values.filter((v) => v > line).length : null,
        games: values.length,
        line,
      };
    });
    return { rows, other: markets.filter((m) => !taken.has(m)) };
  }, [markets, player?.position, seasonRows]);

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

      {keyRows.rows.length > 0 && (
        <div>
          <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-500 mb-2">
            Key markets · {player.position}
          </h3>
          <div className="overflow-x-auto rounded-xl border border-slate-200 dark:border-slate-800">
            <table className="w-full text-sm whitespace-nowrap" data-testid="vegas-key-markets">
              <thead className="text-[10px] uppercase bg-slate-50 dark:bg-slate-900 text-slate-500 font-bold">
                <tr>
                  <th className="px-3 py-2 text-left">Market</th>
                  <th className="px-3 py-2 text-right">Line</th>
                  <th className="px-3 py-2 text-right">Over / Yes</th>
                  {hasUnders && <th className="px-3 py-2 text-right">Under</th>}
                  <th className="px-3 py-2 text-right" title="His average per game this season (for Anytime TD, the share of games he scored)">Season</th>
                  <th className="px-3 py-2 text-right">Last 4</th>
                  <th className="px-3 py-2 text-right" title="Games this season he went over the line">Over line</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                {keyRows.rows.map((r) => {
                  const m = r.market;
                  const price = m?.yes ?? m?.over;
                  const diff = r.line != null && r.season != null ? r.season - r.line : null;
                  return (
                    <tr key={r.def.label} data-testid="vegas-key-market">
                      <td className="px-3 py-2 font-bold">{r.def.label}</td>
                      <td className="px-3 py-2 text-right font-mono">{r.line ?? '–'}</td>
                      <td className="px-3 py-2 text-right font-mono">
                        {price ? <>{formatOdds(price.odds)} <span className="text-slate-400">· {price.prob ?? '-'}%</span></> : <span className="text-slate-400">not posted</span>}
                      </td>
                      {hasUnders && (
                        <td className="px-3 py-2 text-right font-mono">
                          {m?.under ? <>{formatOdds(m.under.odds)} <span className="text-slate-400">· {m.under.prob ?? '-'}%</span></> : '–'}
                        </td>
                      )}
                      <td className="px-3 py-2 text-right font-mono">
                        {r.def.scorer ? (r.season == null ? '–' : `${Math.round(r.season * 100)}%`) : oneDecimal(r.season)}
                        {diff != null && (
                          <span className={`ml-1.5 text-[10px] font-black ${diff >= 0 ? 'text-emerald-500' : 'text-amber-500'}`}>
                            {diff >= 0 ? '+' : ''}{diff.toFixed(1)}
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-right font-mono">{r.def.scorer ? '–' : oneDecimal(r.last4)}</td>
                      <td className="px-3 py-2 text-right font-mono">{r.hit != null ? `${r.hit}/${r.games}` : '–'}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <p className="text-[10px] text-slate-400 mt-1.5">
            Season and Last 4 are his per-game averages in {seasonRows[0]?.season ?? 'this season'}, with the gap to the line beside the season average. Over line counts the games he cleared it.
          </p>
        </div>
      )}

      <div>
        <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-500 mb-2">
          {keyRows.rows.length > 0 ? 'Other markets (Bovada)' : 'Player props (Bovada)'}
        </h3>
        {(keyRows.rows.length > 0 ? keyRows.other : markets).length === 0 ? (
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
                {(keyRows.rows.length > 0 ? keyRows.other.filter((m) => showAlternates || !m.alternate) : shownMarkets).map((m) => (
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
