# Football-Ai

NFL prediction stack: FastAPI backend (`backend/applications/api/`) + Vite/React frontend (`Dashboard/predictor-frontend/`). The backend loads play-by-play, weekly stats, and depth charts via `nfl_data_py` and serves predictions, tier lists, and team rollups. The frontend renders interactive views: player detail modals, team offense modals, tier list, and a team builder.

## Skill routing

When the user's request matches an available skill, invoke it via the Skill tool. When in doubt, invoke the skill.

Key routing rules:
- Product ideas/brainstorming → invoke /office-hours
- Strategy/scope → invoke /plan-ceo-review
- Architecture → invoke /plan-eng-review
- Design system/plan review → invoke /design-consultation or /plan-design-review
- Full review pipeline → invoke /autoplan
- Bugs/errors → invoke /investigate
- QA/testing site behavior → invoke /qa or /qa-only
- Code review/diff check → invoke /review
- Visual polish → invoke /design-review
- Ship/deploy/PR → invoke /ship or /land-and-deploy
- Save progress → invoke /context-save
- Resume context → invoke /context-restore
