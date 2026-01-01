import { describe, it, expect } from 'vitest';
import { normalizeMetric } from './normalize';

describe('normalizeMetric', () => {
  it('returns 0 for non-positive value', () => {
    expect(normalizeMetric(0, 10)).toBe(0);
    expect(normalizeMetric(-5, 10)).toBe(0);
  });

  it('returns 0 for non-positive cap', () => {
    expect(normalizeMetric(5, 0)).toBe(0);
    expect(normalizeMetric(5, -1)).toBe(0);
  });

  it('normalizes proportionally and clamps at 100', () => {
    expect(normalizeMetric(5, 10)).toBe(50);
    expect(normalizeMetric(10, 10)).toBe(100);
    expect(normalizeMetric(20, 10)).toBe(100);
  });
});
