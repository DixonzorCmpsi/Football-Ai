const { chromium } = require('@playwright/test');
const gameOf = async (p) => p.evaluate(() => {
  const t = document.body.innerText;
  const h = document.querySelector('h2, h1');
  const m = t.match(/^([A-Z]{2,3})\s*\n?@\s*\n?([A-Z]{2,3})/m);
  return m ? (m[1] + ' @ ' + m[2]) : (t.split('\n').find(l=>/^[A-Z]{2,3}\s*@\s*[A-Z]{2,3}$/.test(l.trim())) || '?');
});
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1700,height:1000} });
  p.on('pageerror', e => console.log('PAGE ERROR:', e.message));
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  await p.waitForTimeout(9000);

  const cards = p.locator('[data-testid="schedule-game"]');
  const idx = Math.min(6, (await cards.count()) - 1);
  const picked = await cards.nth(idx).getAttribute('data-game');
  console.log('1. click schedule game    :', picked);
  await cards.nth(idx).click();
  await p.waitForTimeout(7000);

  await p.getByRole('button',{name:/^RANKS$/i}).first().click();
  await p.waitForTimeout(9000);
  const sel = p.locator('select').first();
  let opened = (await sel.locator('option:checked').textContent() || '').trim();
  console.log('2. RANKS opened on        :', opened, opened.replace(/\s/g,'') === picked.replace('@','@') ? '<- MATCHES' : '<- MISMATCH');

  // now switch to a different game inside RANKS
  const opts = await sel.locator('option').allTextContents();
  const other = opts.findIndex(o => o.trim() !== opened);
  await sel.selectOption({ index: other });
  await p.waitForTimeout(9000);
  const switched = (await sel.locator('option:checked').textContent() || '').trim();
  console.log('3. switched inside RANKS  :', switched);

  const back = p.locator('button[title^="Back to"]').first();
  console.log('   back button says       :', await back.getAttribute('title'));
  await back.click();
  await p.waitForTimeout(8000);
  const landed = await gameOf(p);
  const isGame = await p.evaluate(() => /ROSTER/i.test(document.body.innerText));
  console.log('4. Back landed on         :', isGame ? 'GAME view' : 'NOT game view', '| showing:', landed);
  console.log('   expected               :', switched);
  console.log(landed.replace(/\s/g,'') === switched.replace(/\s/g,'') ? '   PASS - returned to the game being ranked' : '   (differs)');
  await b.close();
})();

