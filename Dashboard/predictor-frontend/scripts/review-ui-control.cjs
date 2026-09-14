// Drives the app through the assistant's hands (read_screen/click/type/key) against
// the real frontend and backend data. The model is scripted: each "question" makes
// the faked chat stream send one ui_command, and the browser's reply (a snapshot)
// decides the next command, the way pi would.
const { chromium } = require('@playwright/test');
const BASE = process.env.BASE || 'http://localhost:5401';

const sse = (events) => events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join('');

(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1600, height: 1000 } });
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));

  let pending = null; // the command the next chat request will carry
  let lastResult = null;
  let waiter = null;
  let n = 0;
  await p.route('**/api/agent/quota', (r) => r.fulfill({ json: { quota: { allowed: true, used: 0, limit: 25, remaining: 25, resets_at: '', owner: true, blocked_by: null }, house_configured: true } }));
  await p.route('**/api/agent/chat', (r) => r.fulfill({
    status: 200, headers: { 'content-type': 'text/event-stream' },
    body: sse([{ type: 'ui_command', command: { id: `cmd-${++n}-xxxxxxxx`, ...pending } }, { type: 'done', text: 'ok' }, { type: 'end' }]),
  }));
  await p.route('**/api/agent/ui/result', async (r) => {
    lastResult = r.request().postDataJSON();
    await r.fulfill({ json: { ok: true } });
    waiter?.();
  });

  async function act(command) {
    pending = command;
    const done = new Promise((res) => { waiter = res; });
    await p.getByTestId('agent-input').fill(`step ${n + 1}`);
    await p.getByTestId('agent-send').click();
    await Promise.race([done, new Promise((_, rej) => setTimeout(() => rej(new Error('no result in 20s')), 20000))]);
    return lastResult;
  }
  const refFor = (snap, re) => (snap.split('\n').find((l) => re.test(l)) || '').split(' ')[0];
  const where = () => p.evaluate(() => location.pathname + location.search);

  await p.goto(`${BASE}/`, { waitUntil: 'domcontentloaded' });
  await p.waitForTimeout(5000);
  await p.getByTestId('agent-dock-button').click();

  let r = await act({ op: 'snapshot' });
  const lines = r.text.split('\n');
  console.log('1. read_screen ok:', r.ok, '| elements listed:', lines.filter((l) => /^e\d+ /.test(l)).length, '| chars:', r.text.length);
  console.log('   assistant dock hidden from snapshot:', !/Ask the spot|agent-input/i.test(r.text));
  const lookupRef = refFor(r.text, /^e\d+ button "Lookup"/);
  console.log('   Lookup button ref:', lookupRef || '(not found)');

  r = await act({ op: 'click', target: lookupRef || 'Lookup' });
  console.log('2. click Lookup ->', r.ok, await where());

  const boxRef = refFor(r.text, /^e\d+ (text|search)box "Search Player/);
  console.log('   search box ref:', boxRef || '(not found)');
  r = await act({ op: 'type', target: boxRef, text: 'Purdy', press_enter: true });
  const typedValue = await p.locator('input[placeholder="Search Player..."]').inputValue().catch(() => '(no box)');
  console.log('3. type "Purdy" + Enter ->', r.ok, '| box now holds:', typedValue);
  if (process.env.SHOW) console.log('---- SNAPSHOT AFTER ENTER ----\n' + r.text.slice(0, 3500));
  const purdy = r.text.split('\n').find((l) => /^e\d+ clickable .*Purdy/.test(l));
  console.log('   result row:', purdy || '(none)');

  if (purdy) {
    r = await act({ op: 'click', target: purdy.split(' ')[0] });
    console.log('4. click the result ->', r.ok, await where());
    if (process.env.SHOW) console.log('---- AFTER CLICK ----\n' + r.text.slice(0, 2500));
  }

  r = await act({ op: 'key', key: 'Escape' });
  console.log('5. press Escape ->', r.ok);
  r = await act({ op: 'scroll', direction: 'down' });
  console.log('6. scroll down ->', r.ok, r.text.split('\n')[0]);
  r = await act({ op: 'click', target: 'e99999' });
  console.log('7. stale ref ->', r.ok, r.text.split('\n')[0]);

  const t0 = Date.now();
  await p.evaluate(() => import('/src/lib/agentDriver.ts').then((m) => m.snapshot()));
  console.log('snapshot time on this page (ms):', Date.now() - t0);
  console.log('page errors:', errors.length ? errors : 'none');
  await b.close();
})();
