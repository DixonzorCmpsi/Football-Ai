// "Take me to the Jets game and show me AD Mitchell's stats", then "what's his
// situation" and "open that story", driven the way pi does it: each step is one
// ui_command from a faked chat stream; the browser's reply decides the next step.
// Real frontend and backend data; no model.
const { chromium } = require('@playwright/test');
const BASE = process.env.BASE || 'http://localhost:5401';
const sse = (events) => events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join('');

(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1500, height: 1000 } });
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));

  let pending = null;
  let last = null;
  let waiter = null;
  let n = 0;
  await p.route('**/api/agent/quota', (r) => r.fulfill({ json: { quota: { allowed: true, used: 0, limit: 25, remaining: 25, resets_at: '', owner: true, blocked_by: null }, house_configured: true } }));
  await p.route('**/api/agent/chat', (r) => r.fulfill({
    status: 200, headers: { 'content-type': 'text/event-stream' },
    body: sse([{ type: 'ui_command', command: { id: `cmd-${++n}-xxxxxxxx`, ...pending } }, { type: 'done', text: 'ok' }, { type: 'end' }]),
  }));
  await p.route('**/api/agent/ui/result', async (r) => { last = r.request().postDataJSON(); await r.fulfill({ json: { ok: true } }); waiter?.(); });

  async function act(command) {
    pending = command;
    const done = new Promise((res) => { waiter = res; });
    await p.getByTestId('agent-input').fill(`step ${n + 1}`);
    await p.getByTestId('agent-send').click();
    await Promise.race([done, new Promise((_, rej) => setTimeout(() => rej(new Error('no result in 25s')), 25000))]);
    return last;
  }
  const where = () => p.evaluate(() => location.pathname + location.search);
  const activeTab = async () => (await p.locator('[data-testid^="player-modal-tab-"][aria-selected="true"]').getAttribute('data-testid').catch(() => null))?.replace('player-modal-tab-', '');

  await p.goto(`${BASE}/`, { waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(5000);
  await p.getByTestId('agent-dock-button').click();

  // open_player_card("ad mitchel", "stats") -> this URL, as the backend builds it.
  let r = await act({ op: 'navigate', url: '/game/NYJ/TEN?week=1&player=00-0039890', label: 'Adonai Mitchell · stats (NYJ @ TEN)', tool: 'open_player_card' });
  console.log('1. card on stats ->', r.ok, await where(), '| modal:', await p.getByTestId('player-modal').isVisible(), '| tab:', await activeTab());
  console.log('   reply starts:', r.text.split('\n').slice(0, 3).join(' / '));
  console.log('   reply shows the popup + game log weeks:', /\[popup/.test(r.text), /W1\b/.test(r.text), `(${r.text.length} chars)`);

  // "what's his situation" -> the Storylines tab, by clicking it like a user.
  const storyTab = (r.text.split('\n').find((l) => /^e\d+ tab ".*Storylines/i.test(l)) || '').split(' ')[0];
  r = await act({ op: 'click', target: storyTab || 'Storylines' });
  await p.waitForTimeout(1500);
  console.log('2. click Storylines ->', r.ok, await where(), '| tab:', await activeTab());

  // "open that story" -> click the first storyline in the card.
  r = await act({ op: 'snapshot' });
  const story = r.text.split('\n').find((l) => /^e\d+ button ".{25,}/.test(l) && !/tab|icon|Close|Older|Newer/i.test(l));
  console.log('   first story in the listing:', story ? story.slice(0, 110) : '(none)');
  if (story) {
    r = await act({ op: 'click', target: story.split(' ')[0] });
    await p.waitForTimeout(1500);
    const dialogs = await p.locator('[role="dialog"]').count();
    console.log('3. open story ->', r.ok, '| dialogs open:', dialogs, '| reply mentions summary/article:', /summary|read|article|source/i.test(r.text));
  }

  // Escape closes the story, then the card; Back from a card closes the card.
  r = await act({ op: 'key', key: 'Escape' });
  console.log('4. Escape ->', r.ok, '| dialogs left:', await p.locator('[role="dialog"]').count(), '|', await where());
  await p.goBack({ waitUntil: 'commit' }).catch(() => {});
  await p.waitForTimeout(1500);
  console.log('5. browser Back ->', await where(), '| modal:', await p.getByTestId('player-modal').isVisible().catch(() => false));

  // Direct link with a tab, as a shared URL.
  await p.goto(`${BASE}/game/NYJ/TEN?week=1&player=00-0039890&tab=vegas`, { waitUntil: 'domcontentloaded' });
  await p.getByTestId("player-modal").waitFor({ timeout: 20000 }).catch(() => {});
  console.log('6. deep link tab=vegas -> modal:', await p.getByTestId('player-modal').isVisible().catch(() => false), '| tab:', await activeTab());
  await p.goto(`${BASE}/player/00-0039890?view=table`, { waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(6000);
  console.log('7. /player?view=table -> Week rows:', await p.locator('text=/^Week \\d+$/').count());

  console.log('page errors:', errors.length ? errors : 'none');
  await b.close();
})();
