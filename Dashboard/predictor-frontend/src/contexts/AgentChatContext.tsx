/**
 * One conversation, shared by the floating dock and the side panel.
 *
 * Both surfaces show the same exchange -- the dock shows the latest answer in a
 * small box, the panel shows the whole thread -- so the state has to live above
 * both. Panel visibility lives here too, because App needs it to decide whether
 * the right rail shows the panel or the trending list. So do the provider
 * settings (free tier or the user's own key) and today's free allowance, which
 * the dock displays and every question depends on.
 */

import { createContext, useCallback, useContext, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { useAgentChat } from '../hooks/useAgentChat';
import type { AgentQuota, AgentTurn } from '../hooks/useAgentChat';
import { loadSettings, saveSettings } from '../lib/agentIdentity';
import type { AgentSettings } from '../lib/agentIdentity';
import { useAgentScreenContext } from './AgentScreenContext';

type AgentChatValue = {
  turns: AgentTurn[];
  streaming: boolean;
  activeTool: string | null;
  lastAnswer: AgentTurn | null;
  /** Sends with the current screen descriptor and provider settings attached. */
  ask: (message: string) => void;
  stop: () => void;
  reset: () => void;
  panelOpen: boolean;
  openPanel: () => void;
  closePanel: () => void;
  settings: AgentSettings;
  updateSettings: (next: AgentSettings) => void;
  quota: AgentQuota | null;
  /** null until the server has answered; false when the free tier has no key. */
  houseConfigured: boolean | null;
};

const AgentChatContext = createContext<AgentChatValue | null>(null);

export function AgentChatProvider({ children }: { children: ReactNode }) {
  const chat = useAgentChat();
  const { read } = useAgentScreenContext();
  const [panelOpen, setPanelOpen] = useState(false);
  const [settings, setSettings] = useState<AgentSettings>(() => loadSettings());
  // Read at send time, like the screen descriptor, so ask() stays stable.
  const settingsRef = useRef(settings);

  const updateSettings = useCallback((next: AgentSettings) => {
    settingsRef.current = next;
    setSettings(next);
    saveSettings(next);
  }, []);

  const ask = useCallback(
    (message: string) => {
      // read(), not the rendered value: the descriptor at submit time is the
      // one that matches what the user is looking at.
      void chat.send(message, read(), settingsRef.current);
    },
    [chat, read],
  );

  const value = useMemo<AgentChatValue>(
    () => ({
      turns: chat.turns,
      streaming: chat.streaming,
      activeTool: chat.activeTool,
      lastAnswer: chat.lastAnswer,
      ask,
      stop: chat.stop,
      reset: chat.reset,
      panelOpen,
      openPanel: () => setPanelOpen(true),
      closePanel: () => setPanelOpen(false),
      settings,
      updateSettings,
      quota: chat.quota,
      houseConfigured: chat.houseConfigured,
    }),
    [
      chat.turns, chat.streaming, chat.activeTool, chat.lastAnswer, chat.stop, chat.reset,
      chat.quota, chat.houseConfigured, ask, panelOpen, settings, updateSettings,
    ],
  );

  return <AgentChatContext.Provider value={value}>{children}</AgentChatContext.Provider>;
}

export function useAgentChatContext(): AgentChatValue {
  const ctx = useContext(AgentChatContext);
  if (!ctx) throw new Error('useAgentChatContext must be used inside AgentChatProvider');
  return ctx;
}
