/**
 * Who this browser is, as far as the assistant's limits are concerned, and the
 * provider settings a user chose for it.
 *
 * There are no accounts. The free tier's daily allowance is counted per browser id,
 * a random value kept in localStorage. Clearing site data resets it, which is why
 * the backend also caps each IP.
 *
 * The owner unlocks unlimited questions by opening the app once with
 * `?owner=<AGENT_OWNER_TOKEN>`. The token is moved into localStorage and removed
 * from the address bar, so it doesn't sit in history or get shared in a copied link.
 */

const CLIENT_KEY = 'spotai.agent.client.v1';
const OWNER_KEY = 'spotai.agent.owner.v1';
const SETTINGS_KEY = 'spotai.agent.settings.v1';

function randomId(): string {
  try {
    return crypto.randomUUID();
  } catch {
    return `b-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
  }
}

function safeGet(storage: Storage | undefined, key: string): string | null {
  try {
    return storage?.getItem(key) ?? null;
  } catch {
    return null;
  }
}

function safeSet(storage: Storage | undefined, key: string, value: string | null): void {
  try {
    if (value === null) storage?.removeItem(key);
    else storage?.setItem(key, value);
  } catch {
    /* private mode or storage disabled: the app still works, just unremembered */
  }
}

let memoryClientId: string | null = null;

export function getClientId(): string {
  const stored = safeGet(globalThis.localStorage, CLIENT_KEY);
  if (stored) return stored;
  const fresh = memoryClientId ?? randomId();
  memoryClientId = fresh;
  safeSet(globalThis.localStorage, CLIENT_KEY, fresh);
  return fresh;
}

/** Captures `?owner=` once, then serves the stored token. */
export function getOwnerToken(): string | null {
  try {
    const url = new URL(window.location.href);
    const fromUrl = url.searchParams.get('owner');
    if (fromUrl !== null) {
      safeSet(globalThis.localStorage, OWNER_KEY, fromUrl || null);
      url.searchParams.delete('owner');
      window.history.replaceState(window.history.state, '', url.toString());
    }
  } catch {
    /* not in a browser */
  }
  return safeGet(globalThis.localStorage, OWNER_KEY);
}

/** Headers every assistant request carries. */
export function agentHeaders(): Record<string, string> {
  const headers: Record<string, string> = { 'x-client-id': getClientId() };
  const owner = getOwnerToken();
  if (owner) headers['x-owner-token'] = owner;
  return headers;
}

// --- provider settings -------------------------------------------------------

export type AgentMode = 'free' | 'byok';

export type AgentSettings = {
  mode: AgentMode;
  provider: string;
  apiKey: string;
  model: string;
  baseUrl: string;
  /**
   * Keep the key after the tab closes. Off by default: a key in localStorage is
   * readable by any script that ever runs on this origin, so persisting it is the
   * user's call. Off means sessionStorage, which is gone when the tab is.
   */
  remember: boolean;
};

export const DEFAULT_SETTINGS: AgentSettings = {
  mode: 'free',
  provider: 'openrouter',
  apiKey: '',
  model: '',
  baseUrl: '',
  remember: false,
};

export function loadSettings(): AgentSettings {
  const raw = safeGet(globalThis.localStorage, SETTINGS_KEY) ?? safeGet(globalThis.sessionStorage, SETTINGS_KEY);
  if (!raw) return DEFAULT_SETTINGS;
  try {
    return { ...DEFAULT_SETTINGS, ...(JSON.parse(raw) as Partial<AgentSettings>) };
  } catch {
    return DEFAULT_SETTINGS;
  }
}

export function saveSettings(settings: AgentSettings): void {
  const serialized = JSON.stringify(settings);
  if (settings.remember) {
    safeSet(globalThis.localStorage, SETTINGS_KEY, serialized);
    safeSet(globalThis.sessionStorage, SETTINGS_KEY, null);
  } else {
    safeSet(globalThis.sessionStorage, SETTINGS_KEY, serialized);
    // Turning "remember" off must also delete a key persisted earlier.
    safeSet(globalThis.localStorage, SETTINGS_KEY, null);
  }
}

/** The `byok` body field for a chat request, or undefined on the free tier. */
export function byokPayload(settings: AgentSettings) {
  if (settings.mode !== 'byok') return undefined;
  return {
    provider: settings.provider,
    api_key: settings.apiKey,
    model: settings.model,
    base_url: settings.baseUrl || null,
  };
}

export function byokReady(settings: AgentSettings): boolean {
  return settings.mode === 'byok' && !!settings.apiKey.trim() && !!settings.model.trim()
    && (settings.provider !== 'custom' || !!settings.baseUrl.trim());
}
