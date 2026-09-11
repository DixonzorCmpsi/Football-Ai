const { chromium } = require('@playwright/test');
const fs = require('fs');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1700,height:1000}, acceptDownloads:true });
  p.on('pageerror', e => console.log('PAGE ERROR:', e.message));
  await p.addInitScript(() => { try { localStorage.removeItem('gameRanks.state.v1'); } catch {} });
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  await p.getByRole('button',{name:/RANKS/i}).first().click({timeout:30000});
  await p.waitForSelector('[data-testid="tier-row"]', { timeout:60000 });
  await p.waitForFunction(() => !/Loading rosters/i.test(document.body.innerText), {timeout:60000});
  await p.waitForTimeout(800);

  await p.getByRole('button',{name:/Auto-tier/i}).click();
  await p.waitForTimeout(2000);
  await p.locator('select').first().selectOption({ index: 1 });
  await p.waitForSelector('[data-testid="tier-row"]', { timeout:60000 });
  await p.waitForFunction(() => !/Loading rosters/i.test(document.body.innerText), {timeout:60000});
  await p.waitForTimeout(1000);
  await p.getByRole('button',{name:/Auto-tier/i}).click();
  await p.waitForTimeout(2000);

  const [dl] = await Promise.all([
    p.waitForEvent('download', { timeout: 30000 }),
    p.getByRole('button',{name:/Export week CSV/i}).click(),
  ]);
  const path = 'scripts/' + dl.suggestedFilename();
  await dl.saveAs(path);
  const csv = fs.readFileSync(path, 'utf8');
  const lines = csv.split('\n');
  console.log('filename :', dl.suggestedFilename());
  console.log('rows     :', lines.length - 1, '(excl header)');
  console.log('header   :', lines[0]);
  lines.slice(1, 6).forEach(l => console.log('   ', l));
  const teams = new Set(lines.slice(1).filter(Boolean).map(l => l.split(',')[5]));
  console.log('teams in export :', [...teams].join(','));
  fs.unlinkSync(path);
  await b.close();
})();
