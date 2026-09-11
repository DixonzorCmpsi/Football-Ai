const { chromium } = require('@playwright/test');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1700,height:1000} });
  p.on('pageerror', e => console.log('PAGE ERROR:', e.message));
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  await p.waitForTimeout(9000);
  // find the SF @ LA game
  const cards = p.locator('[data-testid="schedule-game"]');
  const n = await cards.count();
  let idx = 0;
  for (let i=0;i<n;i++){ const g = await cards.nth(i).getAttribute('data-game'); if (g === 'SF@LA') { idx = i; break; } }
  console.log('opening:', await cards.nth(idx).getAttribute('data-game'));
  await cards.nth(idx).click();
  await p.waitForTimeout(9000);

  const order = await p.evaluate(() => {
    // roster cards render name then a "POS · TEAM vs OPP" line
    const out = [];
    document.querySelectorAll('h4').forEach(h => {
      const card = h.closest('div[class*="rounded-xl"]');
      if (!card) return;
      const pos = card.querySelector('span[class*="font-mono"]');
      out.push(`${(pos?pos.innerText:'?').trim().padEnd(3)} ${h.innerText.trim()}`);
    });
    return out.slice(0, 14);
  });
  console.log('--- roster order as rendered ---');
  order.forEach((l,i) => console.log(`  ${String(i+1).padStart(2)}. ${l}`));
  await p.screenshot({ path: 'scripts/roster-order.png' });
  await b.close();
})();
