// Side rails: each resizes on its own and remembers its width; the right rail
// switches between Trending and the chat without losing the conversation.
const { chromium } = require('@playwright/test');
const BASE = process.env.BASE || 'http://localhost:5401';
const OUT = process.env.OUT || '.';
const sse = (events) => events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join('');

(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1920, height: 1000 } });
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  await p.route('**/api/agent/quota', (r) => r.fulfill({ json: { quota: { allowed: true, used: 0, limit: 25, remaining: 25, resets_at: '', owner: true, blocked_by: null }, house_configured: true } }));
  await p.route('**/api/agent/chat', (r) => r.fulfill({ status: 200, headers: { 'content-type': 'text/event-stream' },
    body: sse([{ type: 'delta', text: 'Puka Nacua projects 20.4.' }, { type: 'done', text: 'Puka Nacua projects 20.4.' }, { type: 'end' }]) }));

  await p.goto(`${BASE}/`, { waitUntil: 'domcontentloaded' });
  await p.locator('[data-agent-region="right panel"]').waitFor({ timeout: 60000 });
  const width = (region) => p.evaluate((r) => Math.round(document.querySelector(`[data-agent-region="${r}"]`).getBoundingClientRect().width), region);
  console.log('1. default widths  left/right:', await width('left panel'), await width('right panel'));

  const drag = async (side, dx) => {
    const box = await p.getByTestId(`rail-resizer-${side}`).boundingBox();
    await p.mouse.move(box.x + box.width / 2, box.y + 300);
    await p.mouse.down();
    await p.mouse.move(box.x + box.width / 2 + dx, box.y + 300, { steps: 8 });
    await p.mouse.up();
  };
  await drag('left', 120);
  console.log('2. left +120 ->   left/right:', await width('left panel'), await width('right panel'));
  await drag('right', -200);
  console.log('3. right +200 ->  left/right:', await width('left panel'), await width('right panel'));
  await drag('right', -900);
  console.log('4. right past max -> right:', await width('right panel'));

  await p.getByTestId('rail-resizer-left').focus();
  await p.keyboard.press('ArrowLeft');
  console.log('5. left ArrowLeft -> left:', await width('left panel'));

  await p.reload({ waitUntil: 'domcontentloaded' });
  await p.locator('[data-agent-region="right panel"]').waitFor({ timeout: 60000 });
  console.log('6. after reload   left/right:', await width('left panel'), await width('right panel'));
  await p.screenshot({ path: `${OUT}/rails-wide.png` });

  // Chat, then Trending, then Chat again: the conversation is still there.
  await p.getByTestId('agent-dock-button').click();
  await p.getByTestId('agent-input').fill('how does puka look?');
  await p.getByTestId('agent-send').click();
  await p.waitForTimeout(800);
  await p.getByTestId('rail-tab-chat').click();
  const chatText = () => p.locator('[data-agent-region="right panel"]').innerText();
  console.log('7. chat tab shows answer:', (await chatText()).includes('projects 20.4'));
  await p.getByTestId('rail-tab-trending').click();
  console.log('8. trending tab shows list:', /Most Added/i.test(await chatText()));
  await p.getByTestId('rail-tab-chat').click();
  console.log('9. back to chat, answer kept:', (await chatText()).includes('projects 20.4'), '| tab badge:', await p.getByTestId('rail-tab-chat').innerText());
  await p.screenshot({ path: `${OUT}/rails-chat.png` });

  await p.getByTestId('rail-resizer-right').dblclick();
  console.log('10. double-click resets right:', await width('right panel'));
  console.log('page errors:', errors.length ? errors : 'none');
  await b.close();
})();
