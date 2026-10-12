import { useEffect, useState } from 'react';
import { API_BASE_URL } from '../lib/api';

interface Bio {
  name: string | null;
  position: string | null;
  team: string | null;
  jersey_number: string | null;
  height_in: number | null;
  weight_lb: number | null;
  birth_date: string | null;
  age: number | null;
  experience: number | null;
  college: string | null;
  draft_year: number | null;
  draft_round: number | null;
  draft_pick: number | null;
  draft_team: string | null;
  status: string | null;
}

const ordinalRound = (n: number) => `${n}${n === 1 ? 'st' : n === 2 ? 'nd' : n === 3 ? 'rd' : 'th'} Round`;

function rows(b: Bio): [string, string][] {
  const out: [string, string][] = [];
  if (b.height_in || b.weight_lb) {
    const h = b.height_in ? `${Math.floor(b.height_in / 12)}'${b.height_in % 12}"` : '-';
    out.push(['Height / Weight', `${h}  /  ${b.weight_lb ?? '-'} lbs`]);
  }
  if (b.birth_date) {
    const d = new Date(`${b.birth_date}T12:00:00`);
    const label = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    out.push(['Date of Birth (Age)', `${label}${b.age != null ? ` (${b.age})` : ''}`]);
  }
  if (b.experience != null) out.push(['Experience', b.experience === 0 ? 'Rookie' : `${b.experience} season${b.experience === 1 ? '' : 's'}`]);
  if (b.draft_year) {
    const pick = [
      b.draft_round ? ordinalRound(b.draft_round) : null,
      b.draft_pick ? `Pick #${b.draft_pick}` : null,
    ].filter(Boolean).join(', ');
    out.push(['Drafted', `${b.draft_year} NFL Draft${pick ? `, ${pick}` : ''}${b.draft_team ? ` (${b.draft_team})` : ''}`]);
  } else {
    out.push(['Drafted', 'Undrafted']);
  }
  if (b.college) out.push(['College', b.college]);
  if (b.jersey_number) out.push(['Jersey', `#${b.jersey_number}`]);
  if (b.status) out.push(['Status', b.status]);
  return out;
}

const PlayerBio: React.FC<{ playerId: string }> = ({ playerId }) => {
  const [bio, setBio] = useState<Bio | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(`${API_BASE_URL}/player/${playerId}/bio`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((d) => !cancelled && setBio(d))
      .catch((e) => !cancelled && setError(String(e.message || e)))
      .finally(() => !cancelled && setLoading(false));
    return () => { cancelled = true; };
  }, [playerId]);

  if (loading) return <div className="p-20 text-center text-slate-400 dark:text-slate-500 animate-pulse font-bold">Loading bio...</div>;
  if (error || !bio) {
    return (
      <div className="p-10 text-center">
        <p className="text-sm font-black text-red-500">Couldn't load the bio</p>
        {error && <p className="text-xs text-slate-400 mt-1">{error}</p>}
      </div>
    );
  }
  return (
    <dl className="grid grid-cols-[10rem_1fr] gap-y-3 p-6 text-sm">
      {rows(bio).map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="text-slate-500 dark:text-slate-400">{k}</dt>
          <dd className="font-bold text-slate-800 dark:text-slate-100">{v}</dd>
        </div>
      ))}
    </dl>
  );
};

export default PlayerBio;
