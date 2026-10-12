import { useMemo } from 'react';
import { usePlayerHistory, usePlayerProfileById, type HistoryEntry } from '../hooks/useNflData';

/**
 * One colour per compared player, used for their radar, bars, labels and legend so a
 * number is always the colour of whoever it belongs to. Chosen to stay distinct from
 * each other on a dark or light card and to avoid red/green, which the single-player
 * bars already use to mean below/above that player's own average.
 */
export const SERIES_COLORS = ['#38bdf8', '#facc15', '#c084fc', '#2dd4bf', '#fb923c'] as const;

export const seriesColor = (index: number) => SERIES_COLORS[index % SERIES_COLORS.length];

export interface Series {
  id: string;
  name: string;
  position: string;
  team: string;
  season: number | null;
  rows: HistoryEntry[];
}

/** The season to show for a player: the wanted one if he played in it, else his latest with games. */
export function pickSeasonRows(history: HistoryEntry[], wanted: number | null): { season: number | null; rows: HistoryEntry[] } {
  const seasons = [...new Set(history.map((r) => r.season))].sort((a, b) => b - a);
  const played = (s: number) => history.some((r) => r.season === s && (r.points || 0) !== 0);
  const season = wanted != null && played(wanted) ? wanted : seasons.find(played) ?? seasons[0] ?? null;
  return {
    season,
    rows: history.filter((r) => r.season === season).sort((a, b) => a.week - b.week),
  };
}

/** Loads a player's name, position and season rows. `series` is null until both have arrived. */
export function useSeries(id: string, wantedSeason: number | null): { series: Series | null; loading: boolean } {
  const { history, loadingHistory } = usePlayerHistory(id);
  const { cardData } = usePlayerProfileById(id);
  const series = useMemo<Series | null>(() => {
    if (!cardData || loadingHistory) return null;
    const { season, rows } = pickSeasonRows(history, wantedSeason);
    return { id, name: cardData.name, position: cardData.position, team: cardData.team, season, rows };
  }, [id, cardData, history, loadingHistory, wantedSeason]);
  return { series, loading: loadingHistory || !cardData };
}
