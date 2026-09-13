import { useEffect, useState } from 'react';
import { Newspaper, RefreshCw, Sparkles } from 'lucide-react';
import { API_BASE_URL } from '../lib/api';
import StorylineModal from './StorylineModal';

export interface Storyline {
  article_id: string;
  headline: string;
  description: string;
  published: string;
  url: string;
  image: string;
  story_type: string;
  fetched_at: string;
}

/**
 * "3h ago" / "2d ago" - storylines are only useful with their age attached.
 * Past two weeks a date reads better: a player with little recent coverage now
 * shows older items (from their own ESPN feed), and "143d ago" makes you do math.
 */
function timeAgo(iso: string): string {
  const then = Date.parse(iso);
  if (Number.isNaN(then)) return '';
  const mins = Math.max(0, Math.round((Date.now() - then) / 60000));
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.round(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.round(hrs / 24);
  if (days <= 14) return `${days}d ago`;
  const date = new Date(then);
  const sameYear = date.getFullYear() === new Date().getFullYear();
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', ...(sameYear ? {} : { year: 'numeric' }) });
}

const PlayerStorylines: React.FC<{ playerId: string; playerName?: string; teamColor?: string }> = ({
  playerId,
  playerName,
  teamColor = '#3b82f6',
}) => {
  const [items, setItems] = useState<Storyline[]>([]);
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [openStory, setOpenStory] = useState<Storyline | null>(null);

  useEffect(() => {
    if (!playerId) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(`${API_BASE_URL}/player/${playerId}/storylines?limit=5`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((d) => {
        if (cancelled) return;
        setItems(d.storylines || []);
        setUpdatedAt(d.updated_at || null);
      })
      .catch((e) => !cancelled && setError(String(e.message || e)))
      .finally(() => !cancelled && setLoading(false));
    return () => { cancelled = true; };
  }, [playerId]);

  if (loading) {
    return (
      <div className="p-6 space-y-3">
        {[0, 1, 2].map((i) => (
          <div key={i} className="flex gap-3 animate-pulse">
            <div className="w-24 h-16 rounded-lg bg-slate-200 dark:bg-slate-700/60 shrink-0" />
            <div className="flex-1 space-y-2 py-1">
              <div className="h-3 w-3/4 rounded bg-slate-200 dark:bg-slate-700/60" />
              <div className="h-2.5 w-full rounded bg-slate-200 dark:bg-slate-700/60" />
              <div className="h-2.5 w-2/5 rounded bg-slate-200 dark:bg-slate-700/60" />
            </div>
          </div>
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-10 text-center">
        <p className="text-sm font-black text-red-500">Couldn't load storylines</p>
        <p className="text-xs text-slate-400 mt-1">{error}</p>
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="p-12 text-center">
        <Newspaper className="mx-auto text-slate-300 dark:text-slate-600" size={28} />
        <p className="text-sm font-black text-slate-400 dark:text-slate-500 mt-3">
          No storylines yet{playerName ? ` for ${playerName}` : ''}
        </p>
        <p className="text-xs text-slate-400 dark:text-slate-500 mt-1 max-w-md mx-auto">
          ESPN has no news on file for this player yet. Deep reserves and recent signings often
          have none until they see the field.
        </p>
      </div>
    );
  }

  return (
    <div className="divide-y divide-slate-100 dark:divide-slate-700/60">
      <div className="px-4 py-2 flex items-center gap-2 text-[10px] font-bold uppercase tracking-widest text-slate-400 dark:text-slate-500">
        <Newspaper size={12} />
        <span>Latest storylines</span>
        {updatedAt && (
          <span className="ml-auto inline-flex items-center gap-1 font-mono normal-case tracking-normal">
            <RefreshCw size={10} /> feed {timeAgo(updatedAt)}
          </span>
        )}
      </div>

      {items.map((s) => {
        const body = (
          <>
            {s.image ? (
              <img
                src={s.image}
                alt=""
                loading="lazy"
                decoding="async"
                className="w-24 h-16 rounded-lg object-cover bg-slate-100 dark:bg-slate-700 shrink-0"
              />
            ) : (
              <div
                className="w-24 h-16 rounded-lg shrink-0 flex items-center justify-center"
                style={{ backgroundColor: `${teamColor}22` }}
              >
                <Newspaper size={16} style={{ color: teamColor }} />
              </div>
            )}
            <div className="min-w-0 flex-1">
              <div className="flex items-start gap-2">
                <h4 className="text-sm font-black text-slate-800 dark:text-slate-100 leading-snug">
                  {s.headline}
                </h4>
                <Sparkles
                  size={12}
                  aria-label="Open summary"
                  className="shrink-0 mt-1 text-slate-300 dark:text-slate-600 group-hover:text-blue-500"
                />
              </div>
              {s.description && (
                <p className="text-xs text-slate-500 dark:text-slate-400 mt-1 line-clamp-2">
                  {s.description}
                </p>
              )}
              <div className="flex items-center gap-2 mt-1.5 text-[10px] font-bold uppercase tracking-wider text-slate-400 dark:text-slate-500">
                <span>{timeAgo(s.published)}</span>
                {s.story_type && (
                  <>
                    <span className="text-slate-300 dark:text-slate-600">·</span>
                    <span>{s.story_type}</span>
                  </>
                )}
              </div>
            </div>
          </>
        );

        // Opens in the app with a summary; the ESPN link lives inside the popup.
        return (
          <button
            key={s.article_id}
            type="button"
            onClick={() => setOpenStory(s)}
            data-testid="storyline-item"
            className="group w-full text-left flex gap-3 p-4 transition-colors hover:bg-slate-50 dark:hover:bg-slate-700/40"
          >
            {body}
          </button>
        );
      })}

      {openStory && (
        <StorylineModal
          playerId={playerId}
          playerName={playerName}
          story={openStory}
          teamColor={teamColor}
          onClose={() => setOpenStory(null)}
        />
      )}
    </div>
  );
};

export default PlayerStorylines;