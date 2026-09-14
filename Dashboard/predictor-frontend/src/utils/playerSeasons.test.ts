import { describe, expect, it } from 'vitest';
import { defaultSeason, groupBySeason, weekLabel } from './playerSeasons';

// The order /player/history returns: season desc, week desc.
const API_ORDER = [
  { season: 2026, week: 1, points: 26.2 },
  { season: 2025, week: 22, points: 6.7 },
  { season: 2025, week: 21, points: 31.3 },
  { season: 2025, week: 2, points: 10 },
  { season: 2025, week: 1, points: 12 },
];

describe('groupBySeason', () => {
  it('separates seasons and puts weeks in playing order', () => {
    const groups = groupBySeason(API_ORDER);
    expect(groups.map((g) => g.season)).toEqual([2026, 2025]);
    expect(groups[1].rows.map((r) => r.week)).toEqual([1, 2, 21, 22]);
  });

  it('does not reorder the caller\'s array', () => {
    const copy = [...API_ORDER];
    groupBySeason(copy);
    expect(copy).toEqual(API_ORDER);
  });
});

describe('defaultSeason', () => {
  it('opens on the newest season with points', () => {
    expect(defaultSeason(groupBySeason(API_ORDER))).toBe(2026);
    const preseason = groupBySeason([{ season: 2026, week: 1, points: 0 }, ...API_ORDER.slice(1)]);
    expect(defaultSeason(preseason)).toBe(2025);
    expect(defaultSeason([])).toBeNull();
  });
});

describe('weekLabel', () => {
  it('names playoff rounds after an 18-week season', () => {
    expect([18, 19, 20, 21, 22].map((w) => weekLabel(w, 2025))).toEqual(['W18', 'WC', 'DIV', 'CONF', 'SB']);
  });
  it('uses the 17-week calendar before 2021', () => {
    expect(weekLabel(18, 2020)).toBe('WC');
    expect(weekLabel(21, 2020)).toBe('SB');
  });
});
