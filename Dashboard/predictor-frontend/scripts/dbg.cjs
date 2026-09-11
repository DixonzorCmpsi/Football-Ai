const { chromium } = require('@playwright/test');
const dump = async (p, label) => {
  const r = await p.evaluate(() => {
    const counter = [...document.querySelectorAll('span')].map(s=>s.innerText).find(t=>/ranked in week/.test(t||'')) || '?';
    // tier rows: the small mono count under each tier chip
    const counts = [...document.querySelectorAll('div')]
      .filter(d => /^(1|2|3|4|5)$/.test((d.querySelector('span')||{}).innerText||''))
      .map(d => d.innerText.replace(/\n/g,'|'))
      .filter(t => /MUST|FEELS|FLEX|RATHER/i.test(t));
    return { counter, counts };
  });
  console.log(label, '\n   counter:', r.counter, '\n   tiers  :', JSON.stringify(r.counts));
};
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1700,height:1000} });
  p.on('pageerror', e => console.log('PAGE ERROR:', e.message));
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  await p.evaluate(() => localStorage.removeItem('gameRanks.state.v1'));
  await p.reload({ waitUntil:'domcontentloaded' });
  await p.getByRole('button',{name:/RANKS/i}).first().click({timeout:20000});
  await p.waitForTimeout(9000);
  await p.getByRole('button',{name:/Auto-tier/i}).click();
  await p.waitForTimeout(2000);
  await dump(p, 'AFTER AUTO-TIER GAME 1:');
  await p.locator('select').first().selectOption({ index: 1 });
  await p.waitForTimeout(8000);
  await dump(p, 'AFTER SWITCH TO GAME 2:');
  await b.close();
})();
