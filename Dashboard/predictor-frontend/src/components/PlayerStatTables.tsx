import type { HistoryEntry } from '../hooks/useNflData';
import { weekLabel } from '../utils/playerSeasons';

// FantasyData-style tables: grouped Rushing / Receiving / Fumbles headers, with a
// Passing group added only when the player actually threw the ball.

interface Line {
  gp: number;
  fpts: number;
  pass_cmp: number;
  pass_att: number;
  pass_yds: number;
  pass_td: number;
  int: number;
  rush_att: number;
  rush_yds: number;
  rush_td: number;
  rec: number;
  tgts: number;
  rec_yds: number;
  rec_td: number;
  fum: number;
  lost: number;
}

const blank = (): Line => ({
  gp: 0, fpts: 0, pass_cmp: 0, pass_att: 0, pass_yds: 0, pass_td: 0, int: 0,
  rush_att: 0, rush_yds: 0, rush_td: 0, rec: 0, tgts: 0, rec_yds: 0, rec_td: 0, fum: 0, lost: 0,
});

function add(a: Line, r: HistoryEntry): Line {
  return {
    gp: a.gp + 1,
    fpts: a.fpts + (r.points || 0),
    pass_cmp: a.pass_cmp + (r.completions || 0),
    pass_att: a.pass_att + (r.pass_attempts || 0),
    pass_yds: a.pass_yds + (r.passing_yds || 0),
    pass_td: a.pass_td + (r.passing_tds || 0),
    int: a.int + (r.interceptions || 0),
    rush_att: a.rush_att + (r.carries || 0),
    rush_yds: a.rush_yds + (r.rushing_yds || 0),
    rush_td: a.rush_td + (r.rushing_tds || 0),
    rec: a.rec + (r.receptions || 0),
    tgts: a.tgts + (r.targets || 0),
    rec_yds: a.rec_yds + (r.receiving_yds || 0),
    rec_td: a.rec_td + (r.receiving_tds || 0),
    fum: a.fum + (r.fumbles || 0),
    lost: a.lost + (r.fumbles_lost || 0),
  };
}

const avg = (yds: number, att: number) => (att > 0 ? (yds / att).toFixed(1) : '');
const FPTS_BG = 'bg-amber-100 dark:bg-amber-500/20';
const th = 'px-2 py-2 text-center text-[11px] font-black uppercase tracking-wide text-blue-900 dark:text-blue-200 bg-sky-100 dark:bg-slate-900/60';
const group = 'px-2 py-1.5 text-center text-[11px] font-black uppercase tracking-wide text-slate-600 dark:text-slate-300 bg-sky-50 dark:bg-slate-800';
const td = 'px-2 py-2 text-center tabular-nums text-slate-700 dark:text-slate-200';

function Head({ passing, first }: { passing: boolean; first: React.ReactNode }) {
  return (
    <thead>
      <tr>
        <th colSpan={passing ? 4 : 3} className="" />
        <th />
        {passing && <th colSpan={5} className={group}>Passing</th>}
        <th colSpan={4} className={group}>Rushing</th>
        <th colSpan={4} className={group}>Receiving</th>
        <th colSpan={2} className={group}>Fumbles</th>
      </tr>
      <tr>
        {first}
        {passing && ['CMP', 'ATT', 'YDS', 'TD', 'INT'].map((h) => <th key={`p${h}`} className={th}>{h}</th>)}
        {['ATT', 'YDS', 'AVG', 'TD'].map((h) => <th key={`ru${h}`} className={th}>{h}</th>)}
        {['REC', 'TGTS', 'YDS', 'TD'].map((h) => <th key={`re${h}`} className={th}>{h}</th>)}
        {['FUM', 'LOST'].map((h) => <th key={`f${h}`} className={th}>{h}</th>)}
      </tr>
    </thead>
  );
}

function Cells({ l, passing, decimals = false }: { l: Line; passing: boolean; decimals?: boolean }) {
  // Per-game rows show averages, so counting stats get a decimal; totals stay whole.
  const n = (v: number) => (decimals ? v.toFixed(1) : v);
  return (
    <>
      {passing && (
        <>
          <td className={td}>{n(l.pass_cmp)}</td>
          <td className={td}>{n(l.pass_att)}</td>
          <td className={td}>{n(l.pass_yds)}</td>
          <td className={td}>{n(l.pass_td)}</td>
          <td className={td}>{n(l.int)}</td>
        </>
      )}
      <td className={td}>{n(l.rush_att)}</td>
      <td className={td}>{n(l.rush_yds)}</td>
      <td className={td}>{avg(l.rush_yds, l.rush_att)}</td>
      <td className={td}>{n(l.rush_td)}</td>
      <td className={td}>{n(l.rec)}</td>
      <td className={td}>{n(l.tgts)}</td>
      <td className={td}>{n(l.rec_yds)}</td>
      <td className={td}>{n(l.rec_td)}</td>
      <td className={td}>{n(l.fum)}</td>
      <td className={td}>{n(l.lost)}</td>
    </>
  );
}

const threw = (rows: HistoryEntry[]) => rows.some((r) => (r.pass_attempts || 0) > 0);

/** One row per regular season plus a Career row. Click a season to open its game log. */
export function SeasonStatsTable({
  history,
  onOpenSeason,
}: {
  history: HistoryEntry[];
  onOpenSeason: (season: number) => void;
}) {
  const reg = history.filter((r) => (r.season_type || 'REG') === 'REG');
  const bySeason = new Map<number, { team: string; line: Line }>();
  [...reg].sort((a, b) => a.season - b.season || a.week - b.week).forEach((r) => {
    const cur = bySeason.get(r.season) ?? { team: r.team || '', line: blank() };
    cur.line = add(cur.line, r);
    if (r.team) cur.team = r.team;
    bySeason.set(r.season, cur);
  });
  const seasons = [...bySeason.entries()].sort((a, b) => a[0] - b[0]);
  if (seasons.length === 0) {
    return <div className="p-20 text-center text-slate-400 dark:text-slate-500 font-bold">No regular-season stats yet.</div>;
  }
  const passing = threw(reg);
  const career = seasons.reduce((acc, [, s]) => {
    const a = acc;
    (Object.keys(a) as (keyof Line)[]).forEach((k) => { a[k] += s.line[k]; });
    return a;
  }, blank());

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm whitespace-nowrap">
        <Head
          passing={passing}
          first={['SEASON', 'TEAM', 'GP', 'FPTS/G', 'FPTS'].map((h) => <th key={h} className={th}>{h}</th>)}
        />
        <tbody className="divide-y divide-slate-100 dark:divide-slate-700/60">
          {seasons.map(([season, s]) => (
            <tr key={season} className="odd:bg-white even:bg-slate-50/60 dark:odd:bg-slate-800 dark:even:bg-slate-800/60">
              <td className={td}>
                <button type="button" onClick={() => onOpenSeason(season)} className="font-bold text-blue-600 dark:text-blue-400 hover:underline">
                  {season}
                </button>
              </td>
              <td className={td}>{s.team}</td>
              <td className={td}>{s.line.gp}</td>
              <td className={`${td} ${FPTS_BG} font-bold`}>{(s.line.fpts / Math.max(s.line.gp, 1)).toFixed(1)}</td>
              <td className={td}>{s.line.fpts.toFixed(1)}</td>
              <Cells l={s.line} passing={passing} />
            </tr>
          ))}
          <tr className="bg-slate-100 dark:bg-slate-900/60 font-black">
            <td className={`${td} text-left`}>Career</td>
            <td />
            <td className={td}>{career.gp}</td>
            <td className={`${td} ${FPTS_BG}`}>{(career.fpts / Math.max(career.gp, 1)).toFixed(1)}</td>
            <td className={td}>{career.fpts.toFixed(1)}</td>
            <Cells l={career} passing={passing} />
          </tr>
        </tbody>
      </table>
    </div>
  );
}

const fmtDate = (iso?: string | null) => {
  if (!iso) return '';
  const d = new Date(`${iso.slice(0, 10)}T12:00:00`);
  return Number.isNaN(d.getTime()) ? '' : d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
};

/** Every game of one season, in week order, with the full stat line. */
export function GameLogTable({
  rows,
  pointColor,
  avgPoints,
}: {
  rows: HistoryEntry[];
  pointColor: (points: number, avg: number) => string;
  avgPoints: number;
}) {
  const passing = threw(rows);
  const total = rows.reduce((acc, g) => add(acc, g), blank());
  const gp = Math.max(rows.length, 1);
  const perGame = (Object.keys(total) as (keyof Line)[]).reduce((acc, k) => ({ ...acc, [k]: total[k] / gp }), blank());
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm whitespace-nowrap">
        <Head
          passing={passing}
          first={['', 'DATE', 'TEAM', 'OPP', 'FPTS'].map((h, i) => <th key={i} className={th}>{h}</th>)}
        />
        <tbody className="divide-y divide-slate-100 dark:divide-slate-700/60">
          {rows.map((g) => {
            const l = add(blank(), g);
            const post = (g.season_type || 'REG') !== 'REG';
            return (
              <tr key={`${g.season}-${g.week}`} className="odd:bg-white even:bg-slate-50/60 dark:odd:bg-slate-800 dark:even:bg-slate-800/60">
                <td className={`${td} text-left font-bold text-blue-600 dark:text-blue-400`} data-testid="player-modal-week">{post ? weekLabel(g.week, g.season) : `Week ${g.week}`}</td>
                <td className={td}>{fmtDate(g.date)}</td>
                <td className={td}>{g.team}</td>
                <td className={td}>{g.opponent}</td>
                <td className={`${td} ${FPTS_BG} font-bold`} style={{ color: pointColor(g.points || 0, avgPoints) }}>
                  {(g.points || 0).toFixed(1)}
                </td>
                <Cells l={l} passing={passing} />
              </tr>
            );
          })}
        </tbody>
        <tfoot>
          <tr className="bg-slate-100 dark:bg-slate-900/60 font-black border-t-2 border-slate-200 dark:border-slate-700">
            <td className={`${td} text-left`}>Total</td>
            <td />
            <td />
            <td className={td}>{rows.length} G</td>
            <td className={`${td} ${FPTS_BG}`}>{total.fpts.toFixed(1)}</td>
            <Cells l={total} passing={passing} />
          </tr>
          <tr className="bg-slate-50 dark:bg-slate-900/40 font-bold">
            <td className={`${td} text-left`}>Per game</td>
            <td />
            <td />
            <td />
            <td className={`${td} ${FPTS_BG}`}>{perGame.fpts.toFixed(1)}</td>
            <Cells l={perGame} passing={passing} decimals />
          </tr>
        </tfoot>
      </table>
    </div>
  );
}
