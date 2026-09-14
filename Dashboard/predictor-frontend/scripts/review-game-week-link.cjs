// A game link carries its week: /game/DET/BUF?week=2 opens week 2 even while the
// current week is 1, keeps ?week in the address bar, and survives a reload.
const { chromium } = require('@playwright/test');
const BASE = process.env.BASE || 'http://localhost:5401';

(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1500, height: 1000 } });
  const errors = [];
  const matchups = [];
  p.on('pageerror', (e) => errors.push(e.message));
  p.on('request', (r) => { if (/\/api\/matchup\/\d+\//.test(r.url())) matchups.push(r.url().split('/api')[1]); });

  for (const pass of ['load', 'reload']) {
    if (pass === 'load') await p.goto(`${BASE}/game/DET/BUF?week=2`, { waitUntil: 'domcontentloaded' });
    else { matchups.length = 0; await p.reload({ waitUntil: 'domcontentloaded' }); }
    await p.waitForTimeout(10000);
    const url = await p.evaluate(() => location.pathname + location.search);
    const byes = await p.locator('text=/vs BYE/i').count();
    console.log(`${pass}: url=${url} | matchup requests=${matchups.join(', ')} | "vs BYE" on page=${byes}`);
  }
  console.log('page errors:', errors.length ? errors : 'none');
  await b.close();
})();
