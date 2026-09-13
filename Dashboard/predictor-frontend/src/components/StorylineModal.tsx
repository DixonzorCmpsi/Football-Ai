/**
 * A storyline opened in the app: what it's about, and what it means for the player.
 *
 * The summary comes from the backend, which uses the user's own key if they set
 * one, else the free tier. With no model available it shows the article's own
 * sentences about the player, and says so. Reading the whole piece is still one
 * click away, but no longer the only way to find out what a headline means.
 */

import { useEffect, useState } from 'react';
import { ExternalLink, Loader2, MessageSquare, Newspaper, Sparkles, X } from 'lucide-react';
import { API_BASE_URL } from '../lib/api';
import { agentHeaders, byokPayload } from '../lib/agentIdentity';
import { useAgentChatContext } from '../contexts/AgentChatContext';
import type { Storyline } from './PlayerStorylines';

type Summary = {
  text: string;
  key_points: string[];
  fantasy_impact: string | null;
  source: 'ai' | 'extract';
  model?: string;
  tier?: 'house' | 'byok';
  note?: string | null;
};

type SummaryResponse = {
  headline: string;
  excerpt: string;
  url: string;
  summary: Summary;
};

export default function StorylineModal({
  playerId,
  playerName,
  story,
  teamColor,
  onClose,
}: {
  playerId: string;
  playerName?: string;
  story: Storyline;
  teamColor: string;
  onClose: () => void;
}) {
  const { settings, ask, openDock, refreshQuota } = useAgentChatContext();
  const [data, setData] = useState<SummaryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showExcerpt, setShowExcerpt] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setData(null);
    setError(null);
    fetch(`${API_BASE_URL}/player/${playerId}/storylines/${encodeURIComponent(story.article_id)}/summary`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...agentHeaders() },
      body: JSON.stringify({ byok: byokPayload(settings) ?? null }),
    })
      .then(async (r) => {
        const body = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(body.detail || `HTTP ${r.status}`);
        return body as SummaryResponse;
      })
      .then((d) => {
        if (cancelled) return;
        setData(d);
        if (d.summary.source === 'ai' && d.summary.tier === 'house') refreshQuota();
      })
      .catch((e) => !cancelled && setError(String(e.message || e)));
    return () => { cancelled = true; };
    // Settings are read once per open; changing keys mid-read shouldn't refetch.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [playerId, story.article_id]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const summary = data?.summary;
  const askAssistant = () => {
    const who = playerName || 'this player';
    ask(`About ${who}: the storyline "${story.headline}". What does it mean for their fantasy outlook this week?`);
    openDock('chat');
    onClose();
  };

  return (
    <div
      className="fixed inset-0 z-[60] flex items-end sm:items-center justify-center bg-slate-900/50 backdrop-blur-sm p-0 sm:p-6"
      onClick={onClose}
      data-testid="storyline-modal"
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label={story.headline}
        onClick={(e) => e.stopPropagation()}
        className="w-full sm:max-w-xl max-h-[90vh] overflow-y-auto rounded-t-2xl sm:rounded-2xl bg-white dark:bg-slate-800 shadow-2xl border border-slate-200 dark:border-slate-700"
      >
        <div className="h-1.5 rounded-t-2xl" style={{ backgroundColor: teamColor }} />
        <div className="p-5 space-y-4">
          <div className="flex items-start gap-3">
            {story.image ? (
              <img src={story.image} alt="" className="w-20 h-14 rounded-lg object-cover shrink-0 bg-slate-100 dark:bg-slate-700" />
            ) : (
              <div className="w-20 h-14 rounded-lg shrink-0 flex items-center justify-center" style={{ backgroundColor: `${teamColor}22` }}>
                <Newspaper size={18} style={{ color: teamColor }} />
              </div>
            )}
            <div className="min-w-0 flex-1">
              <div className="text-[10px] font-bold uppercase tracking-widest text-slate-400">
                {story.story_type || 'Storyline'}
                {story.published && <> · {new Date(story.published).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })}</>}
              </div>
              <h3 className="text-base font-black leading-snug text-slate-800 dark:text-slate-100 mt-0.5">{story.headline}</h3>
            </div>
            <button type="button" onClick={onClose} aria-label="Close" className="p-1 rounded-lg text-slate-400 hover:text-slate-700 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-700">
              <X size={16} />
            </button>
          </div>

          {error ? (
            <p className="text-sm text-red-500">Couldn't open this storyline: {error}</p>
          ) : !summary ? (
            <div className="flex items-center gap-2 text-sm text-slate-500 py-6 justify-center" data-testid="storyline-summary-loading">
              <Loader2 size={16} className="animate-spin" /> Reading the story…
            </div>
          ) : (
            <div className="space-y-3" data-testid="storyline-summary" data-source={summary.source}>
              <div className="flex items-center gap-1.5 text-[10px] font-black uppercase tracking-widest text-blue-600 dark:text-blue-400">
                {summary.source === 'ai' ? <Sparkles size={12} /> : <Newspaper size={12} />}
                {summary.source === 'ai' ? 'Summary' : 'From the article'}
              </div>
              <p className="text-[14px] leading-relaxed text-slate-700 dark:text-slate-200">
                {summary.text || 'This item has no text beyond its headline.'}
              </p>

              {summary.key_points.length > 0 && (
                <ul className="space-y-1">
                  {summary.key_points.map((point) => (
                    <li key={point} className="text-[13px] text-slate-600 dark:text-slate-300 flex gap-2">
                      <span className="mt-2 w-1 h-1 rounded-full bg-slate-400 shrink-0" />
                      {point}
                    </li>
                  ))}
                </ul>
              )}

              {summary.fantasy_impact && (
                <div className="rounded-lg border border-emerald-200 dark:border-emerald-900 bg-emerald-50 dark:bg-emerald-900/20 p-3">
                  <div className="text-[10px] font-black uppercase tracking-widest text-emerald-700 dark:text-emerald-400 mb-0.5">
                    Fantasy impact{playerName ? ` · ${playerName}` : ''}
                  </div>
                  <p className="text-[13px] text-emerald-900 dark:text-emerald-100">{summary.fantasy_impact}</p>
                </div>
              )}

              <p className="text-[10px] text-slate-400">
                {summary.source === 'ai'
                  ? `Written by ${summary.tier === 'byok' ? 'your model' : 'the free assistant'} (${summary.model}) from the article. Check the source for anything that matters.`
                  : summary.note || 'The article’s own sentences about the player.'}
              </p>
            </div>
          )}

          {data?.excerpt && (
            <div>
              <button type="button" onClick={() => setShowExcerpt((v) => !v)} className="text-[11px] font-bold text-slate-500 hover:text-blue-600">
                {showExcerpt ? 'Hide the opening' : 'Read the opening'}
              </button>
              {showExcerpt && (
                <p className="mt-2 text-[12px] leading-relaxed text-slate-500 dark:text-slate-400 whitespace-pre-line border-l-2 border-slate-200 dark:border-slate-700 pl-3">
                  {data.excerpt}
                </p>
              )}
            </div>
          )}

          <div className="flex flex-wrap items-center gap-2 pt-1 border-t border-slate-100 dark:border-slate-700">
            <button
              type="button"
              onClick={askAssistant}
              className="mt-3 inline-flex items-center gap-1.5 text-[12px] font-bold px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700"
            >
              <MessageSquare size={13} /> Ask the assistant about this
            </button>
            {story.url && (
              <a
                href={story.url}
                target="_blank"
                rel="noopener noreferrer"
                className="mt-3 inline-flex items-center gap-1.5 text-[12px] font-bold px-3 py-2 rounded-lg border border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-200 hover:border-blue-400"
              >
                Full story on ESPN <ExternalLink size={12} />
              </a>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
