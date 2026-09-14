import type { PlayerProp } from '../types';

export type PriceSide = { odds: string | number | null; prob: number | null };

export type PropMarket = {
  prop_type: string;
  line: number | null;
  over?: PriceSide;
  under?: PriceSide;
  /** Scorer markets (Anytime TD, 2+ TDs) have one "Yes" price and no line. */
  yes?: PriceSide;
  /** One of several lines for the same market that isn't the main (closest-to-even) line. */
  alternate?: boolean;
};

// Most-asked first; anything else follows alphabetically.
const ORDER = [
  'Anytime TD', 'Passing Yards', 'Passing Touchdowns', 'Rushing Yards', 'Receiving Yards',
  'Receptions', 'Passing Attempts', 'Completions', 'Rush Attempts',
];

/**
 * Bovada sends one row per side. Pair the over with the under of the same line,
 * so the card shows both prices instead of whichever row happened to be first.
 */
export function groupProps(props: PlayerProp[] | null | undefined): PropMarket[] {
  const markets = new Map<string, PropMarket>();
  for (const p of props || []) {
    const side = String(p.side || '').toLowerCase();
    const isYes = side === 'yes' || (!side && /td|touchdown scorer/i.test(p.prop_type));
    const key = `${p.prop_type}|${isYes ? '' : p.line ?? ''}`;
    const market = markets.get(key) || { prop_type: p.prop_type, line: isYes ? null : p.line };
    const price = { odds: p.odds, prob: p.implied_prob };
    if (isYes) market.yes = price;
    else if (side === 'under') market.under = price;
    else market.over = price;
    markets.set(key, market);
  }
  // Bovada lists alternate lines (Passing Yards 190.5 ... 250.5). The main line is
  // the one priced closest to a coin flip; the rest are alternates.
  const byType = new Map<string, PropMarket[]>();
  for (const m of markets.values()) {
    if (m.yes) continue;
    byType.set(m.prop_type, [...(byType.get(m.prop_type) || []), m]);
  }
  for (const list of byType.values()) {
    if (list.length < 2) continue;
    const distance = (m: PropMarket) => Math.abs((m.over?.prob ?? m.under?.prob ?? 0) - 50);
    const main = list.reduce((best, m) => (distance(m) < distance(best) ? m : best));
    for (const m of list) m.alternate = m !== main;
  }

  const rank = (t: string) => {
    const i = ORDER.findIndex((o) => t.toLowerCase().startsWith(o.toLowerCase()));
    return i === -1 ? ORDER.length : i;
  };
  return [...markets.values()].sort(
    (a, b) => rank(a.prop_type) - rank(b.prop_type) || a.prop_type.localeCompare(b.prop_type) || (a.line ?? 0) - (b.line ?? 0),
  );
}

export function formatOdds(odds: string | number | null | undefined): string {
  if (odds === null || odds === undefined || odds === '') return '-';
  const text = String(odds).trim();
  if (/^even$/i.test(text)) return 'EVEN';
  const n = Number(text);
  return Number.isFinite(n) && n > 0 && !text.startsWith('+') ? `+${n}` : text;
}
