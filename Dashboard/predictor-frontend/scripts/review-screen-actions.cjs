// Review check for feat/agent-screen-actions: URL sync, browser Back, agent actions
// across two answers, deep links into My Team tabs, and the header Back button.
// The agent stream is faked, so no model is needed.
const { chromium } = require('@playwright/test');
const BASE = process.env.BASE || 'http://localhost:5401';

const sse = (events) => events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join('');

(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1600, height: 1000 } });
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));

  let answer = 0;
  await p.route('**/api/agent/quota', (r) => r.fulfill({ json: { quota: { allowed: true, used: 0, limit: 25, remaining: 25, resets_at: '', owner: true, blocked_by: null }, house_configured: true } }));
  await p.route('**/api/agent/chat', (r) => {
    const body = r.request().postDataJSON() || {};
    const q = String(body.message || '');
    const url = q.includes('league') ? '/my-team/league' : (answer % 2 === 0 ? '/ranks' : '/tiers');
    answer += 1;
    r.fulfill({ status: 200, headers: { 'content-type': 'text/event-stream' }, body: sse([
      { type: 'tool', name: 'open_screen', state: 'start' },
      { type: 'tool', name: 'open_screen', state: 'end' },
      { type: 'screen_action', url, label: `answer ${answer}`, tool: 'open_screen' },
      { type: 'done', text: `Opened ${url}.` },
      { type: 'end' },
    ]) });
  });

  const where = async () => p.evaluate(() => location.pathname + location.search);
  const hist = async () => p.evaluate(() => history.length);

  // 1. Direct load of a deep link.
  await p.goto(`${BASE}/ranks`, { waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(4000);
  console.log('1. load /ranks ->', await where(), '| history.length', await hist());

  // 2. Click through three views, then browser Back three times.
  await p.goto(`${BASE}/`, { waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(3000);
  const start = await hist();
  for (const name of ['Ranks', 'My team', 'Tiers']) {
    await p.locator(`button[title="${name}"]`).first().click();
    await p.waitForTimeout(1200);
  }
  console.log('2. after 3 clicks ->', await where(), '| history entries added', (await hist()) - start);
  for (let i = 1; i <= 3; i++) {
    await p.goBack({ waitUntil: 'commit' }).catch(() => {});
    await p.waitForTimeout(1200);
    console.log(`   Back ${i} ->`, await where());
  }
  await p.goForward({ waitUntil: 'commit' }).catch(() => {});
  await p.waitForTimeout(1200);
  console.log('   Forward ->', await where());

  // 3. Two agent answers, each with one action.
  await p.goto(`${BASE}/`, { waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(3000);
  await p.getByTestId('agent-dock-button').click();
  for (const q of ['take me to ranks', 'now the tiers']) {
    await p.getByTestId('agent-input').fill(q);
    await p.getByTestId('agent-send').click();
    await p.waitForTimeout(2500);
    console.log(`3. after "${q}" ->`, await where());
  }

  // 4. Reload on the schedule with that transcript still in sessionStorage.
  await p.evaluate(() => history.pushState({}, '', '/'));
  await p.reload({ waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(4000);
  console.log('4. reload at / with old transcript ->', await where());

  // 5. A faked screen_action into My Team → League, with a saved Sleeper
  //    session so the tab can load. The League tab must render content.
  //    Predicates, not globs: query strings make '**/x/**' matching unreliable.
  const j = (r, obj) => r.fulfill({ status: 200, headers: { 'content-type': 'application/json' }, body: JSON.stringify(obj) });
  const mkTeam = (id, name, wins, pf, pa) => ({
    roster_id: id, team_name: name, wins, losses: 1 - wins, ties: 0,
    points_for: pf, points_against: pa, max_points_for: pf, efficiency: 1.0,
    all_play: { wins: 2, losses: 0, ties: 0, pct: 1.0 }, luck: 0.5, streak: null,
    projected_total: pf,
    by_group: { QB: 15, RB: 20, WR: 25, TE: 10, FLEX: 12 },
    position_ranks: { QB: id, RB: id, WR: id, TE: id, FLEX: id },
    starters: [{ slot: 'QB', name: 'Starter QB', position: 'QB', team: 'KC', projection: 15 }],
    bench_top3: 20, power_score: 50, power_rank: id, standing_rank: id,
  });
  await p.route((u) => u.pathname.includes('/api/sleeper/user/'), (r) => j(r, { user: { user_id: 'u1', username: 'tester', display_name: 'tester' }, leagues: [{ league_id: 'L1', name: 'Test League', season: '2026', total_rosters: 2, scoring_type: 'ppr' }] }));
  await p.route((u) => u.pathname.endsWith('/api/sleeper/league/L1'), (r) => j(r, { league: { league_id: 'L1', name: 'Test League', season: '2026', total_rosters: 2, roster_positions: ['QB', 'RB', 'WR', 'FLEX'], scoring_settings: {} }, teams: [{ roster_id: 1, team_name: 'Mine', wins: 1, losses: 0, ties: 0, player_count: 16 }, { roster_id: 2, team_name: 'Theirs', wins: 0, losses: 1, ties: 0, player_count: 16 }] }));
  await p.route((u) => u.pathname.includes('/roster/1/analysis'), (r) => j(r, {
    league_id: 'L1', roster_id: 1, week: 1, team_name: 'Mine',
    projected_total: 90.5, special_teams_projected_total: 0,
    recommended_starters: [], bench_but_should_start: [], start_but_should_sit: [],
    special_teams: [], unmatched_sleeper_ids: [],
  }));
  await p.route((u) => u.pathname.includes('/api/sleeper/league/L1/insights'), (r) => j(r, {
    league: { league_id: 'L1', name: 'Test League', week: 1, weeks_played: 1, scoring_type: 'ppr', total_rosters: 2 },
    teams: [mkTeam(1, 'Mine', 1, 90.5, 70), mkTeam(2, 'Theirs', 0, 70, 90.5)],
    position_groups: ['QB', 'RB', 'WR', 'TE'],
    matchups: [{ matchup_id: 1, teams: [{ roster_id: 1, team_name: 'Mine', projected: 90.5, live_points: 0, win_probability: 0.6 }, { roster_id: 2, team_name: 'Theirs', projected: 70, live_points: 0, win_probability: 0.4 }] }],
    you: { roster_id: 1, standing_rank: 1, power_rank: 1, projected_rank: 1, teams: 2, strongest: 'RB', weakest: null, opponent: { roster_id: 2, team_name: 'Theirs', projected: 70, win_probability: 0.4 } },
    method: { power_weights: { projection: 0.5, all_play: 0.25, points_for: 0.25 }, notes: [] },
  }));
  // Save the connected Sleeper session the restore path looks for.
  await p.goto(`${BASE}/`, { waitUntil: 'domcontentloaded' });
  await p.evaluate(() => {
    localStorage.setItem('spotai.sleeper.session.v1', JSON.stringify({
      username: 'tester', season: 2026, leagueId: 'L1', rosterId: 1, savedAt: Date.now(),
    }));
  });
  await p.reload({ waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(4000);
  await p.evaluate(() => history.pushState({}, '', '/'));
  await p.reload({ waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(4000);
  // Fresh conversation so the scripted answer (answer 3) streams again.
  await p.evaluate(() => sessionStorage.clear());
  await p.reload({ waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(4000);
  await p.getByTestId('agent-dock-button').click();
  await p.getByTestId('agent-input').fill('take me to my league rankings');
  await p.getByTestId('agent-send').click();
  await p.waitForTimeout(2500);
  const leagueShown = await p.getByTestId('league-insights').isVisible().catch(() => false);
  console.log('5. after league action ->', await where(), '| league-insights rendered:', leagueShown);

  // 6. After an agent action, the header Back button returns to the previous view.
  await p.getByTestId('header-back').click();
  await p.waitForTimeout(1500);
  console.log('6. header Back after agent action ->', await where());

  console.log('page errors:', errors.length ? errors : 'none');
  await b.close();
})();