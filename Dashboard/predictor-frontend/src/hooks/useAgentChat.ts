/**
 * Conversation state for the in-app agent.
 *
 * Uses fetch + a streamed body rather than EventSource: the prompt carries the
 * screen descriptor, so it has to be a POST, and EventSource is GET-only.
 *
 * The transcript lives here and in sessionStorage, not on the server. The
 * backend keeps pi's own context per conversation id; this is what the user
 * sees, and it should survive a tab reload without outliving the tab.
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import { API_BASE_URL } from '../lib/api';
import type { ScreenDescriptor } from '../contexts/AgentScreenContext';

export type AgentTurn = {
  role: 'user' | 'agent';
  text: string;
  /** Tools the agent called for this answer, in order, deduped. */
  tools?: string[];
  error?: boolean;
};

const STORE_KEY = 'spotai.agent.session.v1';

type Stored = { conversationId: string; turns: AgentTurn[] };

function newConversationId(): string {
  try {
    return crypto.randomUUID();
  } catch {
    return `c-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  }
}

function load(): Stored {
  try {
    const raw = sessionStorage.getItem(STORE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as Stored;
      if (parsed.conversationId) return { conversationId: parsed.conversationId, turns: parsed.turns || [] };
    }
  } catch {
    /* ignore */
  }
  return { conversationId: newConversationId(), turns: [] };
}

export function useAgentChat() {
  const initial = useRef<Stored>(load());
  const [conversationId, setConversationId] = useState(initial.current.conversationId);
  const [turns, setTurns] = useState<AgentTurn[]>(initial.current.turns);
  const [streaming, setStreaming] = useState(false);
  const [activeTool, setActiveTool] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    try {
      sessionStorage.setItem(STORE_KEY, JSON.stringify({ conversationId, turns }));
    } catch {
      /* quota or private mode; the transcript is still in memory */
    }
  }, [conversationId, turns]);

  /** Replace the trailing agent turn, which is the one being streamed. */
  const patchLast = useCallback((patch: (turn: AgentTurn) => AgentTurn) => {
    setTurns((prev) => {
      if (prev.length === 0) return prev;
      const next = prev.slice();
      next[next.length - 1] = patch(next[next.length - 1]);
      return next;
    });
  }, []);

  const send = useCallback(
    async (message: string, screen: ScreenDescriptor | null) => {
      const text = message.trim();
      if (!text || streaming) return;

      setTurns((prev) => [...prev, { role: 'user', text }, { role: 'agent', text: '', tools: [] }]);
      setStreaming(true);
      setActiveTool(null);

      const controller = new AbortController();
      abortRef.current = controller;

      try {
        const response = await fetch(`${API_BASE_URL}/agent/chat`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ conversation_id: conversationId, message: text, screen }),
          signal: controller.signal,
        });

        if (!response.ok || !response.body) {
          const detail = response.ok ? 'no response body' : `HTTP ${response.status}`;
          patchLast((t) => ({ ...t, text: `Could not reach the agent (${detail}).`, error: true }));
          return;
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        // SSE frames are separated by a blank line. Chunk boundaries land
        // anywhere, so hold the tail until the next read completes it.
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });

          let split: number;
          while ((split = buffer.indexOf('\n\n')) !== -1) {
            const frame = buffer.slice(0, split);
            buffer = buffer.slice(split + 2);
            const line = frame.split('\n').find((l) => l.startsWith('data:'));
            if (!line) continue;

            let event: { type: string; text?: string; name?: string; state?: string; message?: string };
            try {
              event = JSON.parse(line.slice(5).trim());
            } catch {
              continue;
            }

            if (event.type === 'delta' && event.text) {
              patchLast((t) => ({ ...t, text: t.text + event.text }));
            } else if (event.type === 'tool' && event.state === 'start' && event.name) {
              setActiveTool(event.name);
              patchLast((t) => ({
                ...t,
                tools: t.tools?.includes(event.name!) ? t.tools : [...(t.tools || []), event.name!],
              }));
            } else if (event.type === 'tool' && event.state === 'end') {
              setActiveTool(null);
            } else if (event.type === 'done') {
              // Authoritative: deltas can be dropped, this is the whole answer.
              if (event.text) patchLast((t) => ({ ...t, text: event.text! }));
            } else if (event.type === 'error') {
              patchLast((t) => ({ ...t, text: event.message || 'The agent failed.', error: true }));
            }
          }
        }
      } catch (err) {
        if ((err as Error)?.name !== 'AbortError') {
          patchLast((t) => ({ ...t, text: `Could not reach the agent (${(err as Error).message}).`, error: true }));
        }
      } finally {
        setStreaming(false);
        setActiveTool(null);
        abortRef.current = null;
        // An aborted or empty run must not leave a blank bubble behind.
        patchLast((t) => (t.role === 'agent' && !t.text ? { ...t, text: 'Stopped.', error: true } : t));
      }
    },
    [conversationId, streaming, patchLast],
  );

  const stop = useCallback(() => {
    abortRef.current?.abort();
    // Tell the backend too, or pi keeps burning tokens on an answer nobody reads.
    fetch(`${API_BASE_URL}/agent/abort`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ conversation_id: conversationId }),
    }).catch(() => {});
  }, [conversationId]);

  const reset = useCallback(() => {
    abortRef.current?.abort();
    const previous = conversationId;
    const fresh = newConversationId();
    setTurns([]);
    setConversationId(fresh);
    fetch(`${API_BASE_URL}/agent/reset`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ conversation_id: previous }),
    }).catch(() => {});
  }, [conversationId]);

  const lastAnswer = [...turns].reverse().find((t) => t.role === 'agent') || null;

  return { turns, send, stop, reset, streaming, activeTool, lastAnswer };
}
