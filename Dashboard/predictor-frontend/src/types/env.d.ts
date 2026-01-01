// Type declarations for runtime-injected env (window.__env)
// This avoids TypeScript/IDE complaints when reading `window.__env`.

declare global {
  interface Window {
    __env?: {
      API_BASE_URL?: string;
      [key: string]: unknown;
    };
  }
}

export {};
