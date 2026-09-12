/**
 * Where a user picks how the assistant runs: our free tier, or their own provider key.
 *
 * Model lists come live from each provider (via the backend, which can reach them
 * without CORS trouble) rather than a hardcoded list that goes stale within weeks.
 * "Test" sends one tiny request with a tool attached, so a wrong key, a mistyped
 * model or a model without tool calling fails here instead of on the first question.
 */

import { useEffect, useMemo, useState } from 'react';
import { Check, ExternalLink, Eye, EyeOff, Loader2, RefreshCw, X } from 'lucide-react';
import { API_BASE_URL } from '../lib/api';
import { agentHeaders, byokReady } from '../lib/agentIdentity';
import type { AgentSettings as Settings } from '../lib/agentIdentity';
import type { AgentQuota } from '../hooks/useAgentChat';

type Provider = {
  id: string;
  label: string;
  base_url: string | null;
  requires_base_url: boolean;
  key_help_url: string;
  public_model_list: boolean;
};

type TestResult = { ok: boolean; detail?: string; latency_ms?: number } | null;

function formatReset(iso?: string): string {
  if (!iso) return 'midnight ET';
  try {
    return new Date(iso).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }) + ' your time';
  } catch {
    return 'midnight ET';
  }
}

export default function AgentSettings({
  settings,
  quota,
  houseConfigured,
  onSave,
  onClose,
}: {
  settings: Settings;
  quota: AgentQuota | null;
  houseConfigured: boolean | null;
  onSave: (next: Settings) => void;
  onClose: () => void;
}) {
  const [draft, setDraft] = useState<Settings>(settings);
  const [providers, setProviders] = useState<Provider[]>([]);
  const [models, setModels] = useState<string[]>([]);
  const [modelsState, setModelsState] = useState<'idle' | 'loading' | 'error'>('idle');
  const [modelsError, setModelsError] = useState('');
  const [showKey, setShowKey] = useState(false);
  const [testing, setTesting] = useState(false);
  const [test, setTest] = useState<TestResult>(null);

  const provider = useMemo(() => providers.find((p) => p.id === draft.provider), [providers, draft.provider]);
  const set = (patch: Partial<Settings>) => {
    setDraft((d) => ({ ...d, ...patch }));
    setTest(null);
  };

  useEffect(() => {
    fetch(`${API_BASE_URL}/agent/providers`)
      .then((r) => r.json())
      .then((d: { providers: Provider[] }) => setProviders(d.providers))
      .catch(() => setProviders([]));
  }, []);

  const loadModels = async () => {
    if (!provider) return;
    setModelsState('loading');
    setModelsError('');
    try {
      const response = await fetch(`${API_BASE_URL}/agent/byok/models`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...agentHeaders() },
        body: JSON.stringify({ provider: provider.id, api_key: draft.apiKey || null, base_url: draft.baseUrl || null }),
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`);
      setModels(data.models || []);
      setModelsState('idle');
    } catch (err) {
      setModels([]);
      setModelsState('error');
      setModelsError((err as Error).message);
    }
  };

  // Providers that publish their catalog get a list straight away; the rest once a key is in.
  useEffect(() => {
    setModels([]);
    if (draft.mode === 'byok' && provider?.public_model_list) void loadModels();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [draft.mode, provider?.id]);

  const runTest = async () => {
    setTesting(true);
    setTest(null);
    try {
      const response = await fetch(`${API_BASE_URL}/agent/byok/test`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...agentHeaders() },
        body: JSON.stringify({ provider: draft.provider, api_key: draft.apiKey, model: draft.model, base_url: draft.baseUrl || null }),
      });
      setTest(await response.json());
    } catch (err) {
      setTest({ ok: false, detail: (err as Error).message });
    } finally {
      setTesting(false);
    }
  };

  const canSave = draft.mode === 'free' || byokReady(draft);
  const input =
    'w-full text-[12px] bg-slate-50 dark:bg-slate-900/50 border border-slate-200 dark:border-slate-700 rounded-md px-2 py-1.5 outline-none focus:border-blue-400 dark:focus:border-blue-500';
  const label = 'block text-[10px] font-bold uppercase tracking-wider text-slate-400 mb-1';

  return (
    <div className="p-3 space-y-3" data-testid="agent-settings">
      <div className="flex items-center justify-between">
        <span className="text-[11px] font-black uppercase tracking-widest text-slate-500">Assistant settings</span>
        <button type="button" onClick={onClose} aria-label="Close settings" className="p-1 rounded text-slate-400 hover:text-slate-700">
          <X size={13} />
        </button>
      </div>

      <div className="grid grid-cols-2 gap-1 p-1 rounded-lg bg-slate-100 dark:bg-slate-900/60">
        {(['free', 'byok'] as const).map((mode) => (
          <button
            key={mode}
            type="button"
            onClick={() => set({ mode })}
            data-testid={`agent-mode-${mode}`}
            className={`text-[11px] font-bold py-1.5 rounded-md transition-colors ${
              draft.mode === mode ? 'bg-white dark:bg-slate-700 shadow text-blue-600 dark:text-blue-400' : 'text-slate-500 hover:text-slate-700'
            }`}
          >
            {mode === 'free' ? 'Free' : 'Your own key'}
          </button>
        ))}
      </div>

      {draft.mode === 'free' ? (
        <div className="text-[12px] leading-relaxed text-slate-600 dark:text-slate-300 space-y-1">
          {houseConfigured === false ? (
            <p className="text-amber-600 dark:text-amber-400">
              The free assistant isn't set up on this server. Use your own key for now.
            </p>
          ) : quota?.owner ? (
            <p>Owner access: no daily limit.</p>
          ) : quota ? (
            <p>
              <span className="font-bold">{quota.remaining}</span> of {quota.limit} free questions left today. Resets at{' '}
              {formatReset(quota.resets_at)}.
            </p>
          ) : (
            <p>A daily allowance of free questions, on free models.</p>
          )}
          <p className="text-[11px] text-slate-400">Free models are slower and less sharp than paid ones.</p>
        </div>
      ) : (
        <div className="space-y-2.5">
          <div>
            <label className={label} htmlFor="agent-provider">Provider</label>
            <select
              id="agent-provider"
              value={draft.provider}
              onChange={(e) => set({ provider: e.target.value, model: '' })}
              className={input}
              data-testid="agent-provider"
            >
              {providers.map((p) => (
                <option key={p.id} value={p.id}>{p.label}</option>
              ))}
            </select>
          </div>

          {provider?.requires_base_url && (
            <div>
              <label className={label} htmlFor="agent-base-url">Base URL</label>
              <input
                id="agent-base-url"
                value={draft.baseUrl}
                onChange={(e) => set({ baseUrl: e.target.value })}
                placeholder="https://your-litellm.example.com/v1"
                className={input}
              />
              <p className="mt-1 text-[10px] text-slate-400">Must be https and publicly reachable. OpenAI-compatible.</p>
            </div>
          )}

          <div>
            <div className="flex items-center justify-between">
              <label className={label} htmlFor="agent-api-key">API key</label>
              {provider && (
                <a href={provider.key_help_url} target="_blank" rel="noreferrer" className="text-[10px] text-blue-600 hover:underline inline-flex items-center gap-0.5 mb-1">
                  Get a key <ExternalLink size={9} />
                </a>
              )}
            </div>
            <div className="relative">
              <input
                id="agent-api-key"
                type={showKey ? 'text' : 'password'}
                autoComplete="off"
                spellCheck={false}
                value={draft.apiKey}
                onChange={(e) => set({ apiKey: e.target.value })}
                onBlur={() => { if (draft.apiKey && !provider?.public_model_list) void loadModels(); }}
                placeholder="sk-..."
                className={`${input} pr-8`}
                data-testid="agent-api-key"
              />
              <button type="button" onClick={() => setShowKey((v) => !v)} aria-label={showKey ? 'Hide key' : 'Show key'} className="absolute right-1.5 top-1/2 -translate-y-1/2 text-slate-400">
                {showKey ? <EyeOff size={13} /> : <Eye size={13} />}
              </button>
            </div>
          </div>

          <div>
            <div className="flex items-center justify-between">
              <label className={label} htmlFor="agent-model">Model</label>
              <button type="button" onClick={() => void loadModels()} className="text-[10px] text-blue-600 hover:underline inline-flex items-center gap-0.5 mb-1">
                <RefreshCw size={9} className={modelsState === 'loading' ? 'animate-spin' : ''} /> Load models
              </button>
            </div>
            <input
              id="agent-model"
              list="agent-model-options"
              value={draft.model}
              onChange={(e) => set({ model: e.target.value })}
              placeholder={models.length ? `${models.length} available, start typing` : 'model id'}
              className={input}
              data-testid="agent-model"
            />
            <datalist id="agent-model-options">
              {models.map((m) => <option key={m} value={m} />)}
            </datalist>
            {modelsState === 'error' && <p className="mt-1 text-[10px] text-red-500">{modelsError}</p>}
            {provider?.id === 'openrouter' && models.length > 0 && (
              <p className="mt-1 text-[10px] text-slate-400">Showing models that support tool calling, which the assistant needs.</p>
            )}
          </div>

          <label className="flex items-start gap-2 text-[11px] text-slate-600 dark:text-slate-300">
            <input type="checkbox" checked={draft.remember} onChange={(e) => set({ remember: e.target.checked })} className="mt-0.5" />
            <span>
              Remember my key on this device
              <span className="block text-[10px] text-slate-400">
                Otherwise it's forgotten when this tab closes. Your key goes only to this app's server, which uses it for your
                questions and never stores it.
              </span>
            </span>
          </label>

          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => void runTest()}
              disabled={!byokReady(draft) || testing}
              className="text-[11px] font-bold px-2.5 py-1.5 rounded-md border border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-200 disabled:opacity-40 inline-flex items-center gap-1"
              data-testid="agent-test"
            >
              {testing ? <Loader2 size={11} className="animate-spin" /> : null} Test
            </button>
            {test && (
              <span className={`text-[11px] ${test.ok ? 'text-green-600' : 'text-red-500'}`} data-testid="agent-test-result">
                {test.ok ? `Works (${test.latency_ms} ms)` : test.detail || 'Failed'}
              </span>
            )}
          </div>
        </div>
      )}

      <button
        type="button"
        disabled={!canSave}
        onClick={() => onSave(draft)}
        className="w-full text-[12px] font-bold py-2 rounded-lg bg-blue-600 text-white disabled:opacity-40 hover:bg-blue-700 inline-flex items-center justify-center gap-1"
        data-testid="agent-settings-save"
      >
        <Check size={13} /> Save
      </button>
    </div>
  );
}
