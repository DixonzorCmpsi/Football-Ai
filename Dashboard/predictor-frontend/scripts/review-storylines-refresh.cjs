// Storylines: ten newest first, and the refresh button asks ESPN now (then cools down).
const { chromium } = require('@playwright/test');
const BASE = process.env.BASE || 'http://localhost:5401';
const PLAYER = process.env.PLAYER || '00-0039075'; // Puka Nacua

(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1400, height: 1000 } });
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  await p.goto(`${BASE}/player/${PLAYER}`, { waitUntil: 'domcontentloaded' });
  await p.getByTestId('storyline-item').first().waitFor({ timeout: 60000 }).catch(() => {});
  const dates = await p.evaluate(async ([base, id]) => {
    const r = await fetch(`${base.replace(/\/$/, '')}/api/player/${id}/storylines?limit=10`).catch(() => null);
    const d = r && r.ok ? await r.json() : null;
    return d ? d.storylines.map((s) => s.published) : null;
  }, [BASE, PLAYER]);
  const shown = await p.getByTestId('storyline-item').count();
  console.log('1. items shown:', shown, '| api dates newest first:', dates && dates.every((d, i) => i === 0 || Date.parse(dates[i - 1]) >= Date.parse(d)));
  console.log('   first/last:', dates && dates[0], '…', dates && dates[dates.length - 1]);

  await p.getByTestId('storylines-refresh').click();
  await p.waitForFunction(() => !/checking/.test(document.querySelector('[data-testid="storylines-refresh"]')?.textContent || ''), null, { timeout: 30000 });
  console.log('2. after refresh:', await p.getByTestId('storylines-refresh').innerText(), '| items:', await p.getByTestId('storyline-item').count());
  await p.getByTestId('storylines-refresh').click();
  await p.waitForFunction(() => !/checking/.test(document.querySelector('[data-testid="storylines-refresh"]')?.textContent || ''), null, { timeout: 30000 });
  console.log('3. second press:', await p.getByTestId('storylines-refresh').innerText());
  console.log('page errors:', errors.length ? errors : 'none');
  await b.close();
})();
