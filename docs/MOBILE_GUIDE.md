# Mobile / Platform-agnostic checklist

This repository aims to be platform-agnostic and mobile-friendly. Follow these steps when validating mobile support:

1. Frontend config
   - `src/lib/api.ts` reads `window.__env.API_BASE_URL` and falls back to `/api`. Use this to point the app to your backend in mobile builds or CDN deployments.
   - Ensure `index.html` contains the viewport meta tag (already present).

2. Visual checks
   - Use device emulator (Chrome DevTools) or a real phone to load the app.
   - Verify `CompareView`, `PlayerHistory`, and player cards render correctly on small screens (< 420px wide).
   - Check charts are readable and not overflowed; `ResponsiveContainer` is used for charts and chart min-heights were reduced for mobile.

3. Performance and payloads
   - If mobile bandwidth is a concern, consider adding an optional `?compact=true` param to player endpoints to return smaller payloads (TODO).
   - Verify large images are served with responsive sizes or compressed.

4. Interaction / Accessibility
   - Ensure touch targets (buttons, add/remove) are at least ~44x44 CSS pixels.
   - Navigation should be reachable and not rely on hover-only states.

5. Tests
   - Add visual regression tests (Percy / Playwright) and run on mobile emulators (TODO).

Notes:
- Current changes: normalized Total TD metric and reduced chart/card min-heights for better mobile support.
- Follow-up work: add E2E tests and optional API compact responses for low-bandwidth clients.
