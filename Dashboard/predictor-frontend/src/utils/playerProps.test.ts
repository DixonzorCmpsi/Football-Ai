import { describe, expect, it } from 'vitest';
import { formatOdds, groupProps } from './playerProps';

describe('groupProps', () => {
  it('pairs the over and under of one line, whichever row comes first', () => {
    const markets = groupProps([
      { prop_type: 'Receiving Yards', line: 82.5, odds: '-120', implied_prob: 54.55, side: 'under' },
      { prop_type: 'Receiving Yards', line: 82.5, odds: '-110', implied_prob: 52.38, side: 'over' },
      { prop_type: 'Anytime TD', line: 1, odds: '+140', implied_prob: 41.67, side: 'Yes' },
      { prop_type: 'Receptions', line: 6.5, odds: '+105', implied_prob: 48.78, side: 'over' },
    ]);
    expect(markets.map((m) => m.prop_type)).toEqual(['Anytime TD', 'Receiving Yards', 'Receptions']);
    const yards = markets[1];
    expect(yards.over?.prob).toBe(52.38);
    expect(yards.under?.prob).toBe(54.55);
    expect(markets[0].yes?.odds).toBe('+140');
    expect(markets[0].line).toBeNull();
  });

  it('keeps alternate lines apart and marks the one nearest even as main', () => {
    const markets = groupProps([
      { prop_type: 'Receiving Yards', line: 70.5, odds: '-200', implied_prob: 66.67, side: 'over' },
      { prop_type: 'Receiving Yards', line: 82.5, odds: '-110', implied_prob: 52, side: 'over' },
      { prop_type: 'Receiving Yards', line: 99.5, odds: '+150', implied_prob: 40, side: 'over' },
    ]);
    expect(markets.map((m) => m.line)).toEqual([70.5, 82.5, 99.5]);
    expect(markets.map((m) => !!m.alternate)).toEqual([true, false, true]);
  });

  it('handles nothing', () => {
    expect(groupProps(undefined)).toEqual([]);
  });
});

describe('formatOdds', () => {
  it('signs positive prices and keeps EVEN', () => {
    expect(formatOdds(140)).toBe('+140');
    expect(formatOdds('+140')).toBe('+140');
    expect(formatOdds('-110')).toBe('-110');
    expect(formatOdds('even')).toBe('EVEN');
    expect(formatOdds(null)).toBe('-');
  });
});
