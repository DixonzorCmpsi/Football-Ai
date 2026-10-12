// The roster player popup: game log first, depth chart with an add-to-compare "+", season
// stats, and the Visual tab with players side by side and overlaid. Real backend.
//   BASE=http://localhost:5274 node scripts/review-player-popup.cjs
const { chromium } = require('@playwright/test');
const BASE = process.env.BASE || 'http://localhost:5273';
const OUT = process.env.OUT || 'scripts';

(async () => {
  const b = await chromium.launch({ channel: process.env.PW_CHANNEL || undefined });
  const p = await b.newPage({ viewport: { width: 1500, height: 1000 } });
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));

  await p.goto(`${BASE}/game/LAC/DEN?week=5`, { waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(9000);
  const card = p.locator('[data-testid="player-card"][data-player-name*="Omarion"]').first();
  await card.scrollIntoViewIfNeeded();
  const box = await card.boundingBox();
  await p.mouse.click(box.x + 12, box.y + box.height - 12);
  const popup = p.getByTestId('player-modal');
  await popup.waitFor({ timeout: 15000 });
  await p.waitForTimeout(2500);
  console.log('1. popup open; url =', p.url().replace(BASE, ''));
  const tabs = await popup.locator('button').allTextContents();
  console.log('   tabs:', tabs.filter((t) => /Game Log|Depth|Season|Visual|Storylines|Bio|Vegas/.test(t)).map((t) => t.trim()).join(' | '));
  const box2 = await popup.locator('> div').first().boundingBox();
  console.log('   popup box:', JSON.stringify(box2), '(centered x:', Math.round(box2.x * 2 + box2.width), 'vs 1500)');
  await p.screenshot({ path: `${OUT}/popup-1-gamelog.png` });

  await popup.getByRole('button', { name: /Depth Chart/ }).click();
  await p.waitForTimeout(1500);
  console.log('2. depth tab url =', p.url().replace(BASE, ''));
  const adds = popup.getByRole('button', { name: /^Add .* to comparison/ });
  console.log('   add buttons:', await adds.count());
  await popup.getByRole('button', { name: /Add Keaton Mitchell to comparison/ }).click();
  await popup.getByRole('button', { name: /Add Kimani Vidal to comparison/ }).click();
  await p.screenshot({ path: `${OUT}/popup-2-depth.png` });

  await popup.getByRole('button', { name: /Season Stats/ }).click();
  await p.waitForTimeout(1200);
  await p.screenshot({ path: `${OUT}/popup-3-seasons.png` });

  await popup.getByRole('button', { name: /^Visual/ }).click();
  await p.waitForTimeout(3500);
  await p.screenshot({ path: `${OUT}/popup-4-visual-separate.png`, fullPage: false });
  await popup.getByRole('button', { name: /Overlay/ }).click();
  await p.waitForTimeout(1500);
  await p.screenshot({ path: `${OUT}/popup-5-visual-overlay.png` });
  console.log('3. overlay radars:', await popup.locator('.recharts-radar').count(), '| bar series:', await popup.locator('.recharts-bar').count());

  await p.keyboard.press('Escape');
  await p.waitForTimeout(500);
  console.log('4. closed on Escape:', (await p.getByTestId('player-modal').count()) === 0);
  console.log(errors.length ? 'PAGE ERRORS:\n' + errors.join('\n') : 'no page errors');
  await b.close();
})();
