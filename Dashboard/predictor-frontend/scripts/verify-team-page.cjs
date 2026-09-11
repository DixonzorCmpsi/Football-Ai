const { chromium } = require('@playwright/test');
const state = async (p) => p.evaluate(() => {
  const t = document.body.innerText;
  const back = document.querySelector('button[title^="Back to"]');
  const overlay = [...document.querySelectorAll('div')].some(d => {
    const c = getComputedStyle(d);
    return c.position === 'fixed' && /rgba\(0, 0, 0, 0\.[45]/.test(c.backgroundColor);
  });
  const sidebar = /TRENDING DOWN/i.test(t) || /Most Dropped/i.test(t);
  return {
    onTeamPage: /Team Overview|TEAM BUILDER/i.test(t),
    hasOverlayBackdrop: overlay,
    sidebarVisible: sidebar,
    back: back ? back.getAttribute('title').replace('Back to ','') : null,
  };
});
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1700,height:1000} });
  p.on('pageerror', e => console.log('PAGE ERROR:', e.message));
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  await p.waitForTimeout(9000);

  await p.getByRole('button', { name: /^TEAMS$/i }).first().click();
  await p.waitForTimeout(5000);
  console.log('1. TEAMS list        ->', JSON.stringify(await state(p)));

  // open a team
  const card = p.locator('div:visible, button:visible').filter({ hasText: /^[A-Z]{2,3}$/ }).first();
  const anyTeam = p.locator('button:visible, div[role="button"]:visible').first();
  try { await card.click({ timeout: 8000 }); } catch { await anyTeam.click({ timeout: 8000 }); }
  await p.waitForTimeout(8000);
  console.log('2. opened a team     ->', JSON.stringify(await state(p)));

  // toggle sidebars while on the team page
  const toggle = p.locator('button[aria-label], button[title]').filter({ hasText: '' }).first();
  console.log('3. sidebar present while on team page:', (await state(p)).sidebarVisible);

  const back = p.locator('button[title^="Back to"]').first();
  if (await back.count()) {
    await back.click(); await p.waitForTimeout(5000);
    console.log('4. after Back        ->', JSON.stringify(await state(p)));
  } else console.log('4. no back button');
  await p.screenshot({ path: 'scripts/team-page.png' });
  await b.close();
})();
