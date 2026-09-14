// Option D dock: centered button, centered composer, and a status pill at the top
// while the assistant is using the screen (Esc stops it). The chat stream comes
// from a tiny local server so it can stay open mid-run, as a real one does.
const http = require('http');
const { chromium } = require('@playwright/test');
const BASE = process.env.BASE || 'http://localhost:5401';
const OUT = process.env.OUT || '.';

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/event-stream', 'access-control-allow-origin': '*' });
  const send = (e) => res.write(`data: ${JSON.stringify(e)}\n\n`);
  send({ type: 'tool', state: 'start', name: 'open_player_card' });
  const timer = setTimeout(() => {
    send({ type: 'tool', state: 'end', name: 'open_player_card' });
    send({ type: 'done', text: 'Opened Adonai Mitchell on NYJ @ TEN.' });
    send({ type: 'end' });
    res.end();
  }, 6000);
  req.on('close', () => clearTimeout(timer));
});

(async () => {
  await new Promise((r) => server.listen(5499, r));
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1500, height: 900 } });
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  await p.route('**/api/agent/quota', (r) => r.fulfill({ json: { quota: { allowed: true, used: 0, limit: 25, remaining: 25, resets_at: '', owner: true, blocked_by: null }, house_configured: true } }));
  let aborted = false;
  await p.route('**/api/agent/abort', (r) => { aborted = true; r.fulfill({ json: { ok: true } }); });
  await p.route('**/api/agent/chat', (r) => r.continue({ url: 'http://localhost:5499/chat' }));

  await p.goto(`${BASE}/`, { waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(4000);
  const centerX = async (sel) => { const box = await p.locator(sel).boundingBox(); return box && Math.round(box.x + box.width / 2); };

  console.log('1. button centered:', await centerX('[data-testid="agent-dock-button"]'), 'of', 750);
  await p.screenshot({ path: `${OUT}/dock-1-button.png` });

  await p.keyboard.press('/');
  await p.waitForTimeout(400);
  console.log('2. "/" opens composer:', await p.getByTestId('agent-dock').isVisible(), '| centered:', await centerX('[data-testid="agent-dock"]'),
    '| input focused:', await p.evaluate(() => document.activeElement?.getAttribute('data-testid')));
  await p.screenshot({ path: `${OUT}/dock-2-open.png` });

  await p.getByTestId('agent-input').fill('take me to the jets game and show ad mitchell');
  await p.getByTestId('agent-send').click();
  await p.getByTestId('agent-working').waitFor({ timeout: 5000 }).catch(() => {});
  console.log('3. while working -> pill:', await p.getByTestId('agent-working').isVisible().catch(() => false),
    '| composer hidden:', !(await p.getByTestId('agent-dock').isVisible().catch(() => false)),
    '| pill text:', await p.getByTestId('agent-working').innerText().catch(() => ''));
  await p.screenshot({ path: `${OUT}/dock-3-working.png` });

  // A key the page dispatches (as the assistant does) must not stop the run.
  await p.evaluate(() => window.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })));
  await p.waitForTimeout(300);
  console.log('4. synthetic Escape ignored:', await p.getByTestId('agent-working').isVisible().catch(() => false), '| abort sent:', aborted);

  await p.keyboard.press('Escape');
  await p.waitForTimeout(800);
  console.log('5. real Escape stops:', aborted, '| pill gone:', !(await p.getByTestId('agent-working').isVisible().catch(() => false)),
    '| composer back:', await p.getByTestId('agent-dock').isVisible().catch(() => false));

  await p.setViewportSize({ width: 400, height: 800 });
  await p.waitForTimeout(400);
  await p.screenshot({ path: `${OUT}/dock-4-phone.png` });
  const box = await p.getByTestId('agent-dock').boundingBox().catch(() => null);
  console.log('6. phone width fits:', box ? box.x >= 0 && box.x + box.width <= 400 : 'no dock');

  console.log('page errors:', errors.length ? errors : 'none');
  await b.close();
  server.close();
})();
