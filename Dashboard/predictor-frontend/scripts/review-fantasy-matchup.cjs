// My Team → Matchup against the real backend and a real Sleeper league.
// LEAGUE, ROSTER and USER come from the environment; the saved session is seeded
// the way the page stores it, so no typing is needed.
const { chromium } = require('@playwright/test');
const BASE = process.env.BASE || 'http://localhost:5401';
const OUT = process.env.OUT || '.';
const { LEAGUE, ROSTER, USER } = process.env;

(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1500, height: 1000 } });
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  await p.addInitScript(([user, league, roster]) => {
    localStorage.setItem('spotai.sleeper.session.v1', JSON.stringify({ username: user, season: 2026, leagueId: league, rosterId: Number(roster), savedAt: Date.now() }));
  }, [USER, LEAGUE, ROSTER]);

  await p.goto(`${BASE}/my-team/matchup`, { waitUntil: 'domcontentloaded' });
  await p.getByTestId('fantasy-matchup').waitFor({ timeout: 90000 }).catch(() => {});
  const shown = await p.getByTestId('fantasy-matchup').isVisible().catch(() => false);
  console.log('1. deep link /my-team/matchup ->', shown, '| url:', await p.evaluate(() => location.pathname));
  if (shown) {
    console.log('   win probability:', await p.getByTestId('matchup-win-probability').innerText());
    console.log('   slot rows:', await p.getByTestId('matchup-slot-row').count(), '| edges:', await p.getByTestId('matchup-edge').count(),
      '| players:', await p.getByTestId('matchup-player').count());
    console.log('   first row:', (await p.getByTestId('matchup-slot-row').first().innerText()).replace(/\s+/g, ' ').slice(0, 220));
    await p.screenshot({ path: `${OUT}/matchup-desktop.png`, fullPage: false });
    await p.getByTestId('matchup-slot-row').first().scrollIntoViewIfNeeded();
    await p.screenshot({ path: `${OUT}/matchup-rows.png` });

    await p.getByTestId('sleeper-tab-lineup').click();
    await p.getByTestId('sleeper-tab-matchup').click();
    console.log('2. tab back and forth ->', await p.evaluate(() => location.pathname), await p.getByTestId('fantasy-matchup').isVisible());

    await p.setViewportSize({ width: 400, height: 900 });
    await p.waitForTimeout(500);
    const wide = await p.evaluate(() => document.documentElement.scrollWidth);
    console.log('3. phone width, page scroll width:', wide);
    await p.screenshot({ path: `${OUT}/matchup-phone.png` });
  }
  console.log('page errors:', errors.length ? errors : 'none');
  await b.close();
})();
