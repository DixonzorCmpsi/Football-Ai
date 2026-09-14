/** A side rail's width, remembered in this browser and kept within usable bounds. */

import { useCallback, useState } from 'react';

export const RAIL_DEFAULT = 320;
export const RAIL_MIN = 240;
export const RAIL_MAX = 640;

const clamp = (w: number) => Math.round(Math.min(RAIL_MAX, Math.max(RAIL_MIN, w)));

function readWidth(key: string): number {
  try {
    const n = Number(localStorage.getItem(key));
    return Number.isFinite(n) && n > 0 ? clamp(n) : RAIL_DEFAULT;
  } catch {
    return RAIL_DEFAULT; // storage blocked: fall back, still resizable for this visit
  }
}

/** A rail width that survives reloads, bounded to something the page can live with. */
export function useRailWidth(storageKey: string) {
  const [width, setWidthState] = useState(() => readWidth(storageKey));
  const setWidth = useCallback((next: number) => {
    const w = clamp(next);
    setWidthState(w);
    try { localStorage.setItem(storageKey, String(w)); } catch { /* not remembered, still applied */ }
  }, [storageKey]);
  return [width, setWidth] as const;
}
