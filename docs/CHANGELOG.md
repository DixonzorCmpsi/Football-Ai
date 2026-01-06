# Changelog

## v2.1 - Performance & Mobile Polish (2026-01-02)

### 🚀 Features
- **Playoff Bracket**: Added a new `PlayoffView` to visualize the road to the Super Bowl.
- **My Picks**: Added a dedicated view to track your saved prop bets and predictions.
- **Mobile Experience**:
  - Added a **Side Panel Drawer** for easier navigation on small screens.
  - Added a **Bottom Navigation Bar** for quick access to key views (Schedule, Trending, Picks, Compare, Lookup).
  - Improved responsive layouts for Game Cards and Player Cards.

### ⚡ Performance & Fixes
- **Backend Optimization**: Refactored `get_team_roster_cards` to use `asyncio.gather`, reducing matchup load times by ~70%.
- **Data Integrity**:
  - Fixed duplicate players appearing in injury reports.
  - Fixed "zero snap counts" bug by implementing a robust rolling average fallback.
  - Added **Snap Percentage** display to clarify why O-Line/Defense players often have identical snap counts.
- **Build System**: Updated frontend build output to `build/` to resolve permission issues with Docker-owned `dist/` folders.

## Unreleased

- 🔧 Fix: Normalized Total TDs metric in `CompareView` and `ComparisonHistory` so touchdowns are scaled relative to the observed max between selected players (prevents misleading radar percentages).
- 📱 Improvement: Better mobile/responsive support for the frontend (smaller player card widths, reduced chart min-heights, improved layout for small screens).
- ✅ Note: Add unit/e2e tests for calculation and responsive layout (TODO).

---

Please run the frontend locally and verify on an emulator or mobile device; follow-up PR will add automated visual/responsive checks.
