// Small helper to normalize a metric to 0-100 range using a cap
export const normalizeMetric = (value: number, cap: number) => {
  if (!isFinite(value) || value <= 0) return 0;
  if (!isFinite(cap) || cap <= 0) return 0;
  return Math.min(Math.max((value / cap) * 100, 0), 100);
};
