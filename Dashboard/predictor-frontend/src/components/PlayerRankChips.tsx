import { useEffect, useState } from 'react';
import { API_BASE_URL } from '../lib/api';

interface Ranks {
  ranked: boolean;
  position?: string;
  team?: string;
  games?: number;
  total?: number;
  ppg?: number;
  nfl_rank?: number;
  nfl_of?: number;
  team_rank?: number;
  team_of?: number;
  ppg_rank?: number;
  ppg_of?: number;
}

/**
 * "QB3 NFL" and "QB1 BUF": where he ranks at his position this season, league-wide and
 * on his own team, by fantasy points actually scored. Hover for the per-game rank.
 */
const PlayerRankChips: React.FC<{ playerId: string; className?: string }> = ({ playerId, className = '' }) => {
  const [ranks, setRanks] = useState<Ranks | null>(null);

  useEffect(() => {
    let cancelled = false;
    setRanks(null);
    fetch(`${API_BASE_URL}/player/${playerId}/ranks`)
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => !cancelled && setRanks(d))
      .catch(() => !cancelled && setRanks(null));
    return () => { cancelled = true; };
  }, [playerId]);

  if (!ranks?.ranked || !ranks.position) return null;

  const chip = (label: string, rank?: number, of?: number, title?: string) =>
    rank ? (
      <span
        title={title}
        className="inline-flex items-center gap-1.5 bg-black/30 border border-white/10 rounded px-2 py-0.5 text-xs font-black text-white"
      >
        <span>{ranks.position}{rank}</span>
        <span className="text-white/60 font-bold">{label}</span>
        {of ? <span className="text-white/40 font-mono text-[10px]">of {of}</span> : null}
      </span>
    ) : null;

  const perGame = ranks.ppg_rank
    ? `${ranks.ppg} pts/game, ${ranks.position}${ranks.ppg_rank} per game`
    : undefined;
  const totals = `${ranks.total} pts in ${ranks.games} games`;

  return (
    <div className={`flex flex-wrap items-center gap-2 ${className}`}>
      {chip('NFL', ranks.nfl_rank, ranks.nfl_of, `${totals}. ${perGame ?? ''}`)}
      {chip(ranks.team ?? 'Team', ranks.team_rank, ranks.team_of, `${totals} (rank on ${ranks.team})`)}
    </div>
  );
};

export default PlayerRankChips;
