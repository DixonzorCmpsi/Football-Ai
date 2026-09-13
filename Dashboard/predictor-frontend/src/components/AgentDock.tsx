/**
 * The floating entry point to the agent, bottom-right on every view.
 *
 * Collapsed it is one button. Open it is a composer with the latest answer in a
 * small box directly above the input -- close to the question, no navigation.
 * When the answer outgrows that box (or the user wants the thread), "See more"
 * hands off to the side panel, which takes the right rail's place.
 */

import { useEffect, useRef, useState } from 'react';
import { ArrowUp, ChevronsRight, Loader2, Sparkles, Square, Trash2, X } from 'lucide-react';
import { useAgentChatContext } from '../contexts/AgentChatContext';
import { toolLabel } from '../utils/agentLabels';

/** Answers taller than this get clipped with a "See more" affordance. */
const PEEK_MAX_HEIGHT = 168;

const SUGGESTIONS = [
  'Who should I start here?',
  'Why is this projection low?',
  'What does the line say?',
];

export default function AgentDock({ offsetClass = '' }: { offsetClass?: string }) {
  const { ask, stop, reset, streaming, activeTool, lastAnswer, turns, panelOpen, openPanel } =
    useAgentChatContext();
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState('');
  const [clipped, setClipped] = useState(false);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const peekRef = useRef<HTMLDivElement>(null);
  const peekInnerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  // While the panel has the thread, the dock is just the composer for it.
  const showPeek = !panelOpen && !!lastAnswer;

  // Whether to offer "See more" at all: only when there is more to see.
  //
  // Observed rather than measured once per render: the clipping box has a fixed
  // max-height, so its own size never changes as text streams in. The inner
  // element grows, and that is what tells us the answer has outgrown the box.
  useEffect(() => {
    const box = peekRef.current;
    const inner = peekInnerRef.current;
    if (!box || !inner) return;
    const observer = new ResizeObserver(() => {
      setClipped(inner.scrollHeight > box.clientHeight + 4);
    });
    observer.observe(inner);
    return () => observer.disconnect();
  }, [open, showPeek]);

  const submit = () => {
    const text = draft.trim();
    if (!text || streaming) return;
    ask(text);
    setDraft('');
  };

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        data-testid="agent-dock-button"
        aria-label="Ask the AI about this page"
        className={`fixed z-40 bottom-20 right-4 md:bottom-6 ${offsetClass} h-12 w-12 rounded-full bg-gradient-to-br from-blue-600 to-indigo-700 text-white shadow-lg shadow-blue-900/20 flex items-center justify-center transition-transform hover:scale-105 active:scale-95`}
      >
        {streaming ? <Loader2 size={20} className="animate-spin" /> : <Sparkles size={20} />}
      </button>
    );
  }

  return (
    <div
      data-testid="agent-dock"
      className={`fixed z-40 bottom-20 right-4 md:bottom-6 ${offsetClass} w-[22rem] max-w-[calc(100vw-2rem)] rounded-xl bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 shadow-2xl shadow-slate-900/10 overflow-hidden`}
    >
      <div className="flex items-center justify-between px-3 py-2 border-b border-slate-100 dark:border-slate-700 bg-slate-50/60 dark:bg-slate-900/40">
        <div className="flex items-center gap-1.5 text-blue-600 dark:text-blue-400">
          <Sparkles size={13} />
          <span className="text-[10px] font-black uppercase tracking-widest">Ask the spot</span>
        </div>
        <div className="flex items-center gap-1">
          {turns.length > 0 && (
            <>
              <button
                type="button"
                onClick={openPanel}
                data-testid="agent-open-panel"
                title="Open the full conversation"
                className="p-1 rounded text-slate-400 hover:text-blue-600 hover:bg-slate-100 dark:hover:bg-slate-700"
              >
                <ChevronsRight size={14} />
              </button>
              <button
                type="button"
                onClick={reset}
                title="Clear this conversation"
                className="p-1 rounded text-slate-400 hover:text-red-600 hover:bg-slate-100 dark:hover:bg-slate-700"
              >
                <Trash2 size={13} />
              </button>
            </>
          )}
          <button
            type="button"
            onClick={() => setOpen(false)}
            aria-label="Close"
            className="p-1 rounded text-slate-400 hover:text-slate-700 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-700"
          >
            <X size={14} />
          </button>
        </div>
      </div>

      {showPeek && (
        <div className="px-3 pt-3">
          <div className="relative rounded-lg bg-slate-50 dark:bg-slate-900/50 border border-slate-200 dark:border-slate-700 p-3">
            <div
              ref={peekRef}
              style={{ maxHeight: PEEK_MAX_HEIGHT }}
              className={`text-[13px] leading-relaxed whitespace-pre-wrap break-words [overflow-wrap:anywhere] overflow-hidden ${
                lastAnswer.error ? 'text-red-600 dark:text-red-400' : 'text-slate-700 dark:text-slate-200'
              }`}
            >
              <div ref={peekInnerRef}>{lastAnswer.text || (streaming ? 'Thinking…' : '')}</div>
            </div>

            {clipped && (
              <>
                {/* Fade, so a clipped answer reads as clipped rather than as finished. */}
                <div className="pointer-events-none absolute inset-x-0 bottom-8 h-8 bg-gradient-to-t from-slate-50 dark:from-slate-900/50 to-transparent" />
                <button
                  type="button"
                  onClick={openPanel}
                  data-testid="agent-see-more"
                  className="mt-2 text-[11px] font-bold text-blue-600 dark:text-blue-400 hover:underline"
                >
                  See more →
                </button>
              </>
            )}
          </div>

          {(streaming || (lastAnswer.tools?.length ?? 0) > 0) && (
            <div className="flex flex-wrap items-center gap-1 mt-2">
              {streaming && (
                <span className="inline-flex items-center gap-1 text-[10px] font-bold text-slate-400">
                  <Loader2 size={10} className="animate-spin" />
                  {activeTool ? toolLabel(activeTool) : 'thinking'}
                </span>
              )}
              {!streaming &&
                lastAnswer.tools?.map((tool) => (
                  <span
                    key={tool}
                    className="text-[10px] font-bold px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-700 text-slate-500 dark:text-slate-300"
                  >
                    {toolLabel(tool)}
                  </span>
                ))}
            </div>
          )}
        </div>
      )}

      {turns.length === 0 && (
        <div className="px-3 pt-3 flex flex-wrap gap-1.5">
          {SUGGESTIONS.map((s) => (
            <button
              key={s}
              type="button"
              onClick={() => ask(s)}
              className="text-[11px] px-2 py-1 rounded-full border border-slate-200 dark:border-slate-600 text-slate-500 dark:text-slate-300 hover:border-blue-400 hover:text-blue-600"
            >
              {s}
            </button>
          ))}
        </div>
      )}

      <div className="p-3">
        <div className="flex items-end gap-2">
          <textarea
            ref={inputRef}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => {
              // Enter sends; Shift+Enter is a newline. Matches every chat box
              // the user already knows.
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                submit();
              }
            }}
            rows={2}
            placeholder="Ask about what's on this page…"
            data-testid="agent-input"
            className="flex-1 resize-none text-[13px] bg-slate-50 dark:bg-slate-900/50 border border-slate-200 dark:border-slate-700 rounded-lg px-2.5 py-2 outline-none focus:border-blue-400 dark:focus:border-blue-500 placeholder:text-slate-400"
          />
          {streaming ? (
            <button
              type="button"
              onClick={stop}
              aria-label="Stop"
              className="h-9 w-9 shrink-0 rounded-lg bg-slate-200 dark:bg-slate-700 text-slate-600 dark:text-slate-200 flex items-center justify-center hover:bg-slate-300"
            >
              <Square size={13} />
            </button>
          ) : (
            <button
              type="button"
              onClick={submit}
              disabled={!draft.trim()}
              aria-label="Send"
              data-testid="agent-send"
              className="h-9 w-9 shrink-0 rounded-lg bg-blue-600 text-white flex items-center justify-center disabled:opacity-40 hover:bg-blue-700"
            >
              <ArrowUp size={15} />
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
