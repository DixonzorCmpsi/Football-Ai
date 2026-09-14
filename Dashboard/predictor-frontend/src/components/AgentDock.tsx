/**
 * The floating entry point to the agent, bottom-center on every view.
 *
 * Collapsed it is one button (or press / or Ctrl/Cmd+K). Open it is a centered
 * composer with the latest answer in a small box directly above the input --
 * close to the question, no navigation. When the answer outgrows that box (or
 * the user wants the thread), "See more" hands off to the side panel.
 *
 * While the assistant is operating the screen it steps aside: the composer
 * shrinks in place to a small status pill saying what it's doing, so the page
 * it is working on stays visible (the top of the page holds the nav it clicks). Esc (a real key press, not one the
 * assistant sent) stops the run. When the run ends the composer comes back
 * with the answer.
 */

import { useEffect, useRef, useState } from 'react';
import { ArrowUp, ChevronsRight, KeyRound, Loader2, Settings2, Sparkles, Square, Trash2, X } from 'lucide-react';
import { useAgentChatContext } from '../contexts/AgentChatContext';
import { byokReady } from '../lib/agentIdentity';
import { isScreenTool, toolLabel } from '../utils/agentLabels';
import AgentSettings from './AgentSettings';

/** Answers taller than this get clipped with a "See more" affordance. */
const PEEK_MAX_HEIGHT = 168;

const SUGGESTIONS = [
  'Who should I start here?',
  'Why is this projection low?',
  'What does the line say?',
];

/** Whether a key press landed in something the user is typing into. */
function typingInto(target: EventTarget | null): boolean {
  const el = target as HTMLElement | null;
  return !!el && (el.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName));
}

export default function AgentDock() {
  const {
    ask, stop, reset, streaming, activeTool, lastAnswer, turns, panelOpen, openPanel,
    settings, updateSettings, quota, houseConfigured, dockRequest, applyAgentAction,
  } = useAgentChatContext();
  const [open, setOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);

  // Opened from elsewhere: the header's "AI keys" button, or a page handing over a
  // question. Applied during render (React's "adjust state on a prop change"), not
  // in an effect, so the dock opens in the same paint.
  const [handledRequest, setHandledRequest] = useState<number | null>(null);
  if (dockRequest && dockRequest.nonce !== handledRequest) {
    setHandledRequest(dockRequest.nonce);
    setOpen(true);
    setSettingsOpen(dockRequest.view === 'settings');
  }
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

  // Once this run has touched the screen, keep out of its way until it ends.
  // Tied to the run rather than the current tool, so the dock doesn't flicker
  // back between steps while the model thinks.
  const steppingAside = streaming && (lastAnswer?.tools ?? []).some(isScreenTool);
  const lastMove = lastAnswer?.actions?.[lastAnswer.actions.length - 1];

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      // Keys the assistant dispatches while driving the page are untrusted;
      // only the person at the keyboard can stop it or summon the dock.
      if (!e.isTrusted) return;
      if (steppingAside && e.key === 'Escape') {
        e.preventDefault();
        stop();
        return;
      }
      const shortcut = (e.key.toLowerCase() === 'k' && (e.metaKey || e.ctrlKey)) || (e.key === '/' && !typingInto(e.target));
      if (shortcut) {
        e.preventDefault();
        setOpen(true);
        inputRef.current?.focus();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [steppingAside, stop]);

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

  // What stands between the user and asking, if anything. Shown in place of a
  // mystery failure after they hit send.
  const blocker =
    settings.mode === 'byok'
      ? byokReady(settings) ? null : 'Finish setting up your key'
      : houseConfigured === false
        ? 'Free assistant not set up here. Add your own key'
        : quota && !quota.owner && quota.remaining <= 0
          ? 'No free questions left today. Add your own key'
          : null;

  const submit = () => {
    const text = draft.trim();
    if (!text || streaming || blocker) return;
    ask(text);
    setDraft('');
  };

  const status =
    settings.mode === 'byok'
      ? `${settings.provider === 'ollama-local' ? 'Local Ollama' : 'Your key'} · ${settings.model || 'no model'}`
      : quota?.owner
        ? 'Free · owner, unlimited'
        : quota
          ? `Free · ${quota.remaining} of ${quota.limit} left today`
          : 'Free';

  if (steppingAside) {
    const label = activeTool ? toolLabel(activeTool) : 'thinking';
    const step = label.charAt(0).toUpperCase() + label.slice(1);
    return (
      <div
        data-testid="agent-working"
        data-agent-ignore
        role="status"
        aria-live="polite"
        className="fixed z-[70] bottom-20 md:bottom-6 left-1/2 -translate-x-1/2 max-w-[calc(100vw-2rem)] flex items-center gap-2 rounded-full bg-slate-900/95 dark:bg-slate-100/95 text-white dark:text-slate-900 pl-3 pr-1.5 py-1.5 shadow-xl shadow-slate-900/25 backdrop-blur"
      >
        <span className="relative flex h-2.5 w-2.5 shrink-0">
          <span className="absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-60 motion-safe:animate-ping" />
          <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-blue-500" />
        </span>
        <span className="text-[12px] font-bold truncate">
          {step}
          {lastMove?.label && <span className="font-medium opacity-70"> · {lastMove.label}</span>}
        </span>
        <button
          type="button"
          onClick={stop}
          data-testid="agent-working-stop"
          title="Stop the assistant (Esc)"
          className="ml-1 shrink-0 inline-flex items-center gap-1 rounded-full bg-white/15 dark:bg-slate-900/10 hover:bg-white/25 dark:hover:bg-slate-900/20 px-2 py-0.5 text-[11px] font-bold"
        >
          <Square size={9} /> Esc
        </button>
      </div>
    );
  }

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        data-testid="agent-dock-button"
        data-agent-ignore
        aria-label="Ask the AI about this page"
        title="Ask about this page ( / )"
        className="fixed z-[60] bottom-20 md:bottom-6 left-1/2 -translate-x-1/2 h-12 w-12 rounded-full bg-gradient-to-br from-blue-600 to-indigo-700 text-white shadow-lg shadow-blue-900/20 flex items-center justify-center transition-transform hover:scale-105 active:scale-95"
      >
        {streaming ? <Loader2 size={20} className="animate-spin" /> : <Sparkles size={20} />}
      </button>
    );
  }

  return (
    <div
      data-testid="agent-dock"
      // The assistant can't see or touch its own dock: this is where API keys are typed.
      data-agent-ignore
      className={`fixed z-[60] bottom-20 md:bottom-6 left-1/2 -translate-x-1/2 w-[34rem] max-w-[calc(100vw-2rem)] rounded-2xl bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 shadow-2xl shadow-slate-900/10 overflow-hidden`}
    >
      <div className="flex items-center justify-between px-3 py-2 border-b border-slate-100 dark:border-slate-700 bg-slate-50/60 dark:bg-slate-900/40">
        <div className="flex items-center gap-1.5 text-blue-600 dark:text-blue-400">
          <Sparkles size={13} />
          <span className="text-[10px] font-black uppercase tracking-widest">Ask the spot</span>
        </div>
        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={() => setSettingsOpen((v) => !v)}
            data-testid="agent-open-settings"
            title="Assistant settings: free tier or your own API key"
            className={`p-1 rounded hover:bg-slate-100 dark:hover:bg-slate-700 ${settingsOpen ? 'text-blue-600' : 'text-slate-400 hover:text-blue-600'}`}
          >
            <Settings2 size={13} />
          </button>
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

      {settingsOpen ? (
        <AgentSettings
          settings={settings}
          quota={quota}
          houseConfigured={houseConfigured}
          onSave={(next) => {
            updateSettings(next);
            setSettingsOpen(false);
          }}
          onClose={() => setSettingsOpen(false)}
        />
      ) : (
      <>
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

          {/* Screen-action buttons: reopen the page the agent moved the user to.
              A click is the user asking, so it works regardless of the
              allowNavigation setting. */}
          {!streaming && (lastAnswer.actions?.length ?? 0) > 0 && (
            <div className="flex flex-wrap gap-1 mt-2">
              {lastAnswer.actions!.map((action, i) => (
                <button
                  key={`${action.url}-${i}`}
                  type="button"
                  onClick={() => applyAgentAction(action)}
                  data-testid="agent-action-button"
                  className="text-[10px] font-bold px-2 py-1 rounded-md bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 hover:bg-blue-200 dark:hover:bg-blue-900/50 transition-colors"
                >
                  {action.label} ↗
                </button>
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
              disabled={!draft.trim() || !!blocker}
              aria-label="Send"
              data-testid="agent-send"
              className="h-9 w-9 shrink-0 rounded-lg bg-blue-600 text-white flex items-center justify-center disabled:opacity-40 hover:bg-blue-700"
            >
              <ArrowUp size={15} />
            </button>
          )}
        </div>
        <button
          type="button"
          onClick={() => setSettingsOpen(true)}
          data-testid="agent-status"
          className={`mt-1.5 w-full text-left text-[10px] font-bold inline-flex items-center gap-1 ${
            blocker ? 'text-amber-600 dark:text-amber-400' : 'text-slate-400 hover:text-blue-600'
          }`}
        >
          <KeyRound size={10} />
          {blocker ?? status}
        </button>
      </div>
      </>
      )}
    </div>
  );
}
