const { chromium } = require('@playwright/test');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1700,height:1000} });
  p.on('pageerror', e => console.log('PAGE ERROR:', e.message));
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  await p.waitForTimeout(9000);

  await p.getByRole('button', { name: /^LOOKUP$/i }).first().click();
  await p.waitForTimeout(2500);
  await p.getByPlaceholder(/Search Player/i).first().fill('Mike Evans');
  await p.waitForTimeout(5000);
  await p.locator('div:visible', { hasText: /^Mike Evans$/ }).first().click();
  await p.waitForTimeout(6000);

  // the whole PlayerCard is the history trigger, not a labelled button
  await p.locator('h4:visible', { hasText: /Mike Evans/i }).first().click();
  await p.waitForTimeout(7000);
  console.log('on history view:', await p.locator('button:visible', { hasText: /Storylines/i }).count() > 0);

  const tab = p.locator('button:visible', { hasText: /Storylines/i }).first();
  await tab.click();
  await p.waitForTimeout(4500);
  const out = await p.evaluate(() => {
    const t = document.body.innerText;
    const feed = (t.match(/feed \d+[mhd] ago/) || [])[0] || null;
    const cards = [...document.querySelectorAll('a[target="_blank"], div')]
      .filter(el => el.querySelector(':scope > div > h4') || (el.tagName==='A' && el.querySelector('h4')));
    const heads = [...document.querySelectorAll('h4')]
      .map(h => h.innerText.trim()).filter(x => x.length > 25);
    return { feed, empty: /No storylines yet/i.test(t), heads: heads.slice(0, 8) };
  });
  console.log('feed freshness :', out.feed);
  console.log('empty state    :', out.empty);
  console.log('--- rendered storylines ---');
  out.heads.forEach(l => console.log('   *', l.slice(0,76)));
  await p.screenshot({ path: 'scripts/storylines.png' });
  await b.close();
})();



