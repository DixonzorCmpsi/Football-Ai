const { chromium } = require('@playwright/test');

const waitBoard = async (p) => {
  await p.waitForSelector('[data-testid="tier-row"]', { timeout: 60000 });
  // wait until the unranked pool has actually been populated for this matchup
  await p.waitForFunction(() => {
    const t = document.body.innerText;
    return /Unranked Players/i.test(t) && !/Loading rosters/i.test(t);
  }, { timeout: 60000 });
  await p.waitForTimeout(800);
};

const snap = async (p, label) => {
  const r = await p.evaluate(() => {
    const rows = [...document.querySelectorAll('[data-testid="tier-row"]')];
    const tiers = rows.map(r => ({
      tier: r.getAttribute('data-tier'),
      n: +r.getAttribute('data-count'),
      teams: [...new Set([...r.querySelectorAll('[data-testid="ranked-card"]')].map(c => c.getAttribute('data-player-team')))],
    }));
    const store = JSON.parse(localStorage.getItem('gameRanks.state.v1') || '{}');
    const persisted = Object.fromEntries(Object.entries(store).map(([k,v]) => [k, Object.keys(v.assignments||{}).length]));
    return { tiers, persisted, totalOnBoard: tiers.reduce((a,t)=>a+t.n,0) };
  });
  console.log(label);
  console.log('   on board :', r.totalOnBoard, '|', r.tiers.map(t=>`${t.tier}:${t.n}[${t.teams.join(',')}]`).join(' '));
  console.log('   persisted:', JSON.stringify(r.persisted));
  return r;
};

(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1700,height:1000} });
  p.on('pageerror', e => console.log('PAGE ERROR:', e.message));
  await p.addInitScript(() => { try { localStorage.removeItem('gameRanks.state.v1'); } catch {} });
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  await p.getByRole('button',{name:/RANKS/i}).first().click({timeout:30000});
  await waitBoard(p);

  const sel = p.locator('select').first();
  const games = await sel.locator('option').allTextContents();
  console.log('game1 =', games[0], '| game2 =', games[1], '\n');

  await p.getByRole('button',{name:/Auto-tier/i}).click();
  await p.waitForFunction(() => [...document.querySelectorAll('[data-testid="tier-row"]')].reduce((a,r)=>a+ +r.getAttribute('data-count'),0) > 0, { timeout: 30000 });
  const a = await snap(p, 'A) auto-tiered GAME 1:');

  await sel.selectOption({ index: 1 });
  await waitBoard(p);
  const c = await snap(p, 'B) switched to GAME 2 (game-1 players should persist):');

  await p.getByRole('button',{name:/Auto-tier/i}).click();
  await p.waitForTimeout(2500);
  const d = await snap(p, 'C) auto-tiered GAME 2 (should hold BOTH games):');

  const g1 = games[0].replace(/\s/g,'').split('@');
  const carriedTeams = c.tiers.flatMap(t=>t.teams);
  console.log('\nRESULT');
  console.log('  game-1 rankings survive switch :', c.totalOnBoard === a.totalOnBoard && a.totalOnBoard > 0 ? 'PASS ('+c.totalOnBoard+' still shown)' : 'FAIL ('+a.totalOnBoard+' -> '+c.totalOnBoard+')');
  console.log('  those cards are game-1 teams   :', carriedTeams.length && carriedTeams.every(t=>g1.includes(t)) ? 'PASS ('+[...new Set(carriedTeams)].join(',')+')' : 'FAIL ('+carriedTeams.join(',')+')');
  console.log('  accumulates across games       :', d.totalOnBoard > c.totalOnBoard ? 'PASS ('+c.totalOnBoard+' -> '+d.totalOnBoard+')' : 'FAIL');
  console.log('  both games persisted           :', Object.keys(d.persisted).length === 2 ? 'PASS '+JSON.stringify(d.persisted) : 'FAIL '+JSON.stringify(d.persisted));
  await b.close();
})();
