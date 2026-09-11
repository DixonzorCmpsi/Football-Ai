const { chromium } = require('@playwright/test');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1700,height:1000} });
  await p.addInitScript(() => localStorage.removeItem('gameRanks.state.v1'));
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  await p.getByRole('button',{name:/RANKS/i}).first().click({timeout:20000});
  await p.waitForTimeout(9000);
  await p.getByRole('button',{name:/Auto-tier/i}).click();
  await p.waitForTimeout(2000);
  const st = await p.evaluate(() => {
    const raw = JSON.parse(localStorage.getItem('gameRanks.state.v1') || '{}');
    const out = {};
    for (const [k,v] of Object.entries(raw)) {
      out[k] = {
        assignments: Object.keys(v.assignments||{}).length,
        playerMeta: Object.keys(v.playerMeta||{}).length,
        sampleMeta: Object.values(v.playerMeta||{})[0] || null,
        tierOrderKeys: Object.keys(v.tierOrder||{}),
      };
    }
    return out;
  });
  console.log(JSON.stringify(st, null, 1).slice(0, 1500));
  await b.close();
})();
