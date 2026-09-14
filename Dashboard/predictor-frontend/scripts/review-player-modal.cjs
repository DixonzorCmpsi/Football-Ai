// The roster player card: weeks in order per season, Visuals and Storylines tabs,
// and a Vegas tab with the game line and both sides of every prop. Real backend.
const { chromium } = require('@playwright/test');
const BASE = process.env.BASE || 'http://localhost:5401';

async function openCard(p, path, name) {
  await p.goto(`${BASE}${path}`, { waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(9000);
  // Click the card itself, bottom-left: the name is its own link to the team page.
  const card = p.locator(`[data-testid="player-card"][data-player-name*="${name}"]`).first();
  await card.scrollIntoViewIfNeeded();
  const box = await card.boundingBox();
  await p.mouse.click(box.x + 12, box.y + box.height - 12);
  await p.getByTestId('player-modal').waitFor({ timeout: 15000 });
  await p.waitForTimeout(2500);
}

(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1500, height: 1000 } });
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));

  // A finished game: Jaxon Smith-Njigba, NE @ SEA week 1.
  await openCard(p, '/game/NE/SEA', 'Smith-Njigba');
  const seasons = await p.getByTestId('player-modal-season').allTextContents();
  const weeks = await p.getByTestId('player-modal-week').allTextContents();
  console.log('1. seasons:', seasons.join(' '), '| opened season weeks:', weeks.join(' '));
  if (seasons.length > 1) {
    await p.getByTestId('player-modal-season').nth(1).click();
    await p.waitForTimeout(500);
    const older = await p.getByTestId('player-modal-week').allTextContents();
    console.log(`   ${seasons[1]} weeks:`, older.join(' '));
  }
  await p.screenshot({ path: 'scripts/player-modal-log.png' });

  await p.getByTestId('player-modal-tab-visuals').click();
  await p.waitForTimeout(1500);
  console.log('2. visuals svg charts:', await p.getByTestId('player-modal').locator('svg.recharts-surface').count());
  await p.getByTestId('player-modal-tab-storylines').click();
  await p.waitForTimeout(4000);
  console.log('3. storylines items:', await p.getByTestId('storyline-item').count());
  await p.getByTestId('player-modal-tab-vegas').click();
  await p.waitForTimeout(500);
  console.log('4. vegas (finished game):', (await p.getByTestId('player-modal-vegas').innerText()).replace(/\s+/g, ' ').slice(0, 400));
  await p.keyboard.press('Escape');
  await p.waitForTimeout(500);
  console.log('   Escape closes:', !(await p.getByTestId('player-modal').isVisible().catch(() => false)));

  // An upcoming game with a live Bovada board: DEN @ KC, Monday night.
  await openCard(p, '/game/DEN/KC', 'Mahomes');
  await p.getByTestId('player-modal-tab-vegas').click();
  await p.waitForTimeout(500);
  console.log('5. vegas (upcoming):', (await p.getByTestId('player-modal-vegas').innerText()).replace(/\s+/g, ' ').slice(0, 700));
  console.log('   prop rows:', await p.getByTestId('player-modal-prop').count());
  await p.screenshot({ path: 'scripts/player-modal-vegas.png' });

  // Phone width.
  await p.setViewportSize({ width: 400, height: 850 });
  await p.waitForTimeout(800);
  const overflow = await p.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1);
  console.log('6. phone width horizontal page scroll:', overflow);

  console.log('page errors:', errors.length ? errors : 'none');
  await b.close();
})();
