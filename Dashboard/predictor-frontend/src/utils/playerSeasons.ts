/**
 * A player's game log arrives newest-first across seasons (2026 W1, then 2025
 * W22, W21 ...). Read as one list that looks like week 1 was followed by the
 * Super Bowl. These helpers split it by season and put each season in week order.
 */

export type SeasonRow = { season?: number | null; week: number; points?: number | null };

export type SeasonGroup<T extends SeasonRow> = { season: number; rows: T[] };

/** Seasons newest first; weeks within a season in playing order. */
export function groupBySeason<T extends SeasonRow>(rows: T[]): SeasonGroup<T>[] {
  const map = new Map<number, T[]>();
  for (const row of rows) {
    const season = row.season ?? 0;
    if (!map.has(season)) map.set(season, []);
    map.get(season)!.push(row);
  }
  return [...map.entries()]
    .map(([season, list]) => ({ season, rows: [...list].sort((a, b) => a.week - b.week) }))
    .sort((a, b) => b.season - a.season);
}

/** The season to open on: the newest one with any points scored, else the newest. */
export function defaultSeason<T extends SeasonRow>(groups: SeasonGroup<T>[]): number | null {
  if (!groups.length) return null;
  return (groups.find((g) => g.rows.some((r) => (r.points || 0) > 0)) || groups[0]).season;
}

/**
 * "W7", or the playoff round. Since 2021 the regular season is 18 weeks, so
 * weeks 19-22 are Wild Card, Divisional, Conference and the Super Bowl; before
 * that it was 17 weeks and every round sits one week earlier.
 */
export function weekLabel(week: number, season?: number | null): string {
  const lastRegular = season != null && season > 0 && season < 2021 ? 17 : 18;
  const round = week - lastRegular;
  if (round === 1) return 'WC';
  if (round === 2) return 'DIV';
  if (round === 3) return 'CONF';
  if (round >= 4) return 'SB';
  return `W${week}`;
}
