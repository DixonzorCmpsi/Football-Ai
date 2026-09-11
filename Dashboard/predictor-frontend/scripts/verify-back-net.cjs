const { chromium } = require('@playwright/test');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1700,height:1000} });
  const calls = [];
  p.on('request', r => { const u = r.url(); if (/\/api\/matchup\//.test(u)) calls.push(u.split('/api')[1]); });
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  await p.waitForTimeout(9000);

  const cards = p.locator('[data-testid="schedule-game"]');
  const picked = await cards.nth(6).getAttribute('data-game');
  await cards.nth(6).click(); await p.waitForTimeout(7000);
  console.log('1. clicked schedule game :', picked, '| fetched:', calls.at(-1));

  await p.getByRole('button',{name:/^RANKS$/i}).first().click(); await p.waitForTimeout(9000);
  const sel = p.locator('select').first();
  console.log('2. RANKS opened on       :', (await sel.locator('option:checked').textContent()).trim());

  const opts = await sel.locator('option').allTextContents();
  const other = opts.findIndex(o => o.trim() !== (opts[6]||'').trim() && o.trim() !== '');
  await sel.selectOption({ index: 0 });
  await p.waitForTimeout(9000);
  const switched = (await sel.locator('option:checked').textContent()).trim();
  console.log('3. switched in RANKS to  :', switched);

  calls.length = 0;
  await p.locator('button[title^="Back to"]').first().click();
  await p.waitForTimeout(8000);
  console.log('4. after Back, GAME fetched:', calls.at(-1) || '(no new fetch)');
  console.log('   expected matchup for   :', switched);
  await b.close();
})();
