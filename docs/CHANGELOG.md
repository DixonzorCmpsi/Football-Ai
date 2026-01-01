# Changelog

## Unreleased

- 🔧 Fix: Normalized Total TDs metric in `CompareView` and `ComparisonHistory` so touchdowns are scaled relative to the observed max between selected players (prevents misleading radar percentages).
- 📱 Improvement: Better mobile/responsive support for the frontend (smaller player card widths, reduced chart min-heights, improved layout for small screens).
- ✅ Note: Add unit/e2e tests for calculation and responsive layout (TODO).

---

Please run the frontend locally and verify on an emulator or mobile device; follow-up PR will add automated visual/responsive checks.
