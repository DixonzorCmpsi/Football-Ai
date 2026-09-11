const { chromium } = require('@playwright/test');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1700,height:1000} });
  p.on('pageerror', e => console.log('PAGE ERROR:', e.message));
  await p.addInitScript(() => localStorage.removeItem('gameRanks.state.v1'));
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  await p.getByRole('button',{name:/RANKS/i}).first().click({timeout:20000});
  await p.waitForTimeout(9000);
  const sel = p.locator('select').first();
  const games = await sel.locator('option').allTextContents();

  await p.getByRole('button',{name:/Auto-tier/i}).click();
  await p.waitForTimeout(1500);
  await sel.selectOption({ index: 1 });
  await p.waitForTimeout(8000);

  // Read the tier column: every card's player name + team badge.
  const tiers = await p.evaluate(() => {
    const out = {};
    document.querySelectorAll('[data-tier-row]').forEach(row => {
      out[row.getAttribute('data-tier-row')] = [...row.querySelectorAll('[data-player-team]')]
        .map(e => e.getAttribute('data-player-team'));
    });
    if (Object.keys(out).length) return out;
    // fallback: scrape the right-hand tier column by text
    const cols = [...document.querySelectorAll('div')].filter(d => /MUST START|Must Start/i.test(d.innerText||'') && d.innerText.length < 4000);
    const col = cols[cols.length-1];
    return { raw: col ? col.innerText.slice(0, 1200) : 'not found' };
  });
  console.log('game 1 =', games[0], '| now viewing =', games[1]);
  console.log('TIER COLUMN WHILE VIEWING GAME 2:');
  console.log(JSON.stringify(tiers, null, 1).slice(0, 1800));
  await b.close();
})();
