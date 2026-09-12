/**
 * What the user is looking at, so the agent can resolve "him" and "this game".
 *
 * App.tsx publishes the coarse view (which page, which week). A mounted view
 * that knows more -- the game page knows the two teams, the ranks board knows
 * the filtered position -- refines it with `useAgentScreen`. The descriptor is
 * sent with every prompt and rendered into one bracketed line server-side.
 *
 * Kept in a ref as well as state: the dock reads it at submit time, and a
 * re-render of App on every keystroke of a question would be wasteful.
 */

import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';

export type ScreenEntity = {
  type: 'player' | 'team' | 'game';
  name: string;
  id?: string;
  /** Short qualifier the model can use verbatim, e.g. "QB, SF" or "Out". */
  detail?: string;
};

export type ScreenDescriptor = {
  view: string;
  title?: string;
  week?: number | null;
  facts?: string[];
  entities?: ScreenEntity[];
};

type AgentScreenValue = {
  /** Current descriptor, base merged with whatever a view contributed. */
  screen: ScreenDescriptor;
  /** Read the latest without subscribing to re-renders. */
  read: () => ScreenDescriptor;
  setBase: (next: ScreenDescriptor) => void;
  setDetail: (next: ScreenDescriptor | null) => void;
};

const EMPTY: ScreenDescriptor = { view: '' };

const AgentScreenContext = createContext<AgentScreenValue | null>(null);

function merge(base: ScreenDescriptor, detail: ScreenDescriptor | null): ScreenDescriptor {
  if (!detail) return base;
  return {
    view: detail.view || base.view,
    title: detail.title || base.title,
    week: detail.week ?? base.week,
    facts: [...(base.facts || []), ...(detail.facts || [])],
    entities: [...(detail.entities || []), ...(base.entities || [])],
  };
}

export function AgentScreenProvider({ children }: { children: ReactNode }) {
  const [base, setBase] = useState<ScreenDescriptor>(EMPTY);
  const [detail, setDetail] = useState<ScreenDescriptor | null>(null);

  const screen = useMemo(() => merge(base, detail), [base, detail]);
  // Mirrored into a ref so the dock can read the descriptor at submit time
  // without every keystroke of a question re-rendering App. Written in an
  // effect, which lands before any user event can read it.
  const latest = useRef(screen);
  useEffect(() => {
    latest.current = screen;
  }, [screen]);

  const read = useCallback(() => latest.current, []);

  const value = useMemo<AgentScreenValue>(
    () => ({ screen, read, setBase, setDetail }),
    [screen, read],
  );

  return <AgentScreenContext.Provider value={value}>{children}</AgentScreenContext.Provider>;
}

export function useAgentScreenContext(): AgentScreenValue {
  const ctx = useContext(AgentScreenContext);
  if (!ctx) {
    // Rendering a view outside the provider should not crash the page; the
    // agent just loses screen awareness.
    return { screen: EMPTY, read: () => EMPTY, setBase: () => {}, setDetail: () => {} };
  }
  return ctx;
}

/**
 * Register what this view is showing. Pass null while the view has nothing
 * specific to add (still loading, nothing selected).
 *
 * Serialize the descriptor in the caller's dependency list, not the object
 * itself -- a fresh object literal every render would loop.
 */
export function useAgentScreen(descriptor: ScreenDescriptor | null) {
  const { setDetail } = useAgentScreenContext();
  const serialized = JSON.stringify(descriptor);

  useEffect(() => {
    setDetail(descriptor ? (JSON.parse(serialized) as ScreenDescriptor) : null);
    return () => setDetail(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [serialized, setDetail]);
}
