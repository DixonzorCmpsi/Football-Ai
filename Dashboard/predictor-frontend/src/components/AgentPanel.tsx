/**
 * The full conversation, rendered in the right rail in place of Trending Up.
 *
 * Slotting into the existing rail rather than overlaying it keeps the page the
 * user was reading fully visible -- the point of asking about the screen is that
 * the screen still matters. Closing restores the trending list untouched.
 */

import { useEffect, useRef } from 'react';
import type { ReactNode } from 'react';
import { Loader2, Sparkles, Trash2, X } from 'lucide-react';
import { useAgentChatContext } from '../contexts/AgentChatContext';
import { toolLabel } from '../utils/agentLabels';

/**
 * `headerExtra` carries the controls that normally live in this rail's header
 * (theme toggle, week chip). Taking the rail's place must not cost the user
 * access to them.
 */
export default function AgentPanel({ headerExtra }: { headerExtra?: ReactNode }) {
  const { turns, streaming, activeTool, reset, closePanel } = useAgentChatContext();
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ block: 'end' });
  }, [turns, streaming]);

  return (
    <>
      <div className="p-4 border-b border-slate-100 dark:border-slate-700 bg-slate-50/50 dark:bg-slate-800/50 backdrop-blur flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2 text-blue-600 dark:text-blue-400 mb-1">
            <Sparkles size={16} />
            <h2 className="text-xs font-black uppercase tracking-widest">Ask the Spot</h2>
          </div>
          <p className="text-xs text-slate-400 dark:text-slate-500">Grounded in this app's models</p>
        </div>
        <div className="flex items-center gap-1">
          {headerExtra}
          <button
            type="button"
            onClick={reset}
            title="Clear this conversation"
            className="p-1.5 rounded-md text-slate-400 hover:text-red-600 hover:bg-slate-100 dark:hover:bg-slate-700"
          >
            <Trash2 size={14} />
          </button>
          <button
            type="button"
            onClick={closePanel}
            data-testid="agent-close-panel"
            aria-label="Close the assistant and show trending players"
            className="p-1.5 rounded-md text-slate-400 hover:text-slate-700 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-700"
          >
            <X size={14} />
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-3 scrollbar-thin dark:scrollbar-thumb-slate-600 dark:scrollbar-track-slate-800">
        {turns.length === 0 && (
          <p className="text-xs text-slate-400 text-center mt-10">
            Ask a question from the button at the bottom of the page.
          </p>
        )}

        {turns.map((turn, i) => (
          <div
            key={i}
            className={turn.role === 'user' ? 'flex justify-end' : 'flex justify-start'}
          >
            <div
              className={`max-w-[92%] rounded-lg px-3 py-2 text-[13px] leading-relaxed whitespace-pre-wrap break-words [overflow-wrap:anywhere] ${
                turn.role === 'user'
                  ? 'bg-blue-600 text-white'
                  : turn.error
                    ? 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-800'
                    : 'bg-slate-100 dark:bg-slate-700/60 text-slate-800 dark:text-slate-100'
              }`}
            >
              {turn.text || (streaming && i === turns.length - 1 ? 'Thinking…' : '')}

              {turn.role === 'agent' && (turn.tools?.length ?? 0) > 0 && (
                <div className="flex flex-wrap gap-1 mt-2 pt-2 border-t border-slate-200/60 dark:border-slate-600/60">
                  {turn.tools!.map((tool) => (
                    <span
                      key={tool}
                      className="text-[10px] font-bold px-1.5 py-0.5 rounded bg-white/70 dark:bg-slate-800/70 text-slate-500 dark:text-slate-300"
                    >
                      {toolLabel(tool)}
                    </span>
                  ))}
                </div>
              )}
            </div>
          </div>
        ))}

        {streaming && (
          <div className="flex items-center gap-1.5 text-[11px] font-bold text-slate-400">
            <Loader2 size={11} className="animate-spin" />
            {activeTool ? toolLabel(activeTool) : 'thinking'}
          </div>
        )}
        <div ref={endRef} />
      </div>
    </>
  );
}
