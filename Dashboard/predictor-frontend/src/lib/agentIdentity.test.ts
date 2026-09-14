import { describe, it, expect, beforeEach, vi } from 'vitest';
import { DEFAULT_SETTINGS, loadSettings, saveSettings, type AgentSettings } from './agentIdentity';

// vitest runs in Node, where localStorage/sessionStorage don't exist. The
// agentIdentity module handles their absence gracefully (try/catch), but the
// round-trip test needs them to actually store. A minimal in-memory mock is
// enough — the real API surface used is getItem/setItem/removeItem.
function makeStorage(): Storage {
  const map = new Map<string, string>();
  return {
    get length() { return map.size; },
    key: (i: number) => [...map.keys()][i] ?? null,
    getItem: (k: string) => map.get(k) ?? null,
    setItem: (k: string, v: string) => { map.set(k, v); },
    removeItem: (k: string) => { map.delete(k); },
    clear: () => { map.clear(); },
  };
}

describe('agentIdentity allowNavigation', () => {
  beforeEach(() => {
    const ls = makeStorage();
    const ss = makeStorage();
    vi.stubGlobal('localStorage', ls);
    vi.stubGlobal('sessionStorage', ss);
  });

  it('defaults to true', () => {
    expect(DEFAULT_SETTINGS.allowNavigation).toBe(true);
  });

  it('persists through save/load round-trip', () => {
    const custom: AgentSettings = { ...DEFAULT_SETTINGS, allowNavigation: false, remember: true };
    saveSettings(custom);
    const loaded = loadSettings();
    expect(loaded.allowNavigation).toBe(false);
  });

  it('defaults to true when loading settings without the field (backwards compat)', () => {
    // Simulate an old saved settings object that predates allowNavigation.
    const oldShape = {
      mode: 'free',
      provider: 'openrouter',
      apiKey: '',
      model: '',
      baseUrl: '',
      remember: true,
    };
    localStorage.setItem('spotai.agent.settings.v1', JSON.stringify(oldShape));
    const loaded = loadSettings();
    expect(loaded.allowNavigation).toBe(true);
  });
});