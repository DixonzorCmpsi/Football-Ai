// Scroll profiler: drives the real app and measures frame pacing + DOM cost.
const { chromium } = require('@playwright/test');

const URL = process.env.APP_URL || 'http://localhost:5273';

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });
  const t0 = Date.now();
  await page.goto(URL, { waitUntil: 'networkidle', timeout: 90000 });
  console.log('load(ms):', Date.now() - t0);

  // Navigate to RANKS
  try {
    await page.getByRole('button', { name: /RANKS/i }).first().click({ timeout: 15000 });
  } catch { console.log('!! could not click RANKS'); }
  await page.waitForTimeout(4000);

  const dom = await page.evaluate(() => {
    const all = document.querySelectorAll('*');
    const cs = [...all].filter(e => getComputedStyle(e).contentVisibility === 'auto').length;
    const bf = [...all].filter(e => { const s = getComputedStyle(e); return s.backdropFilter && s.backdropFilter !== 'none'; }).length;
    const sh = [...all].filter(e => { const s = getComputedStyle(e); return s.boxShadow && s.boxShadow !== 'none'; }).length;
    const tr = [...all].filter(e => { const s = getComputedStyle(e); return s.transitionProperty === 'all'; }).length;
    const imgs = document.querySelectorAll('img').length;
    // find the tallest scrollable container
    let best = null;
    for (const e of all) {
      const s = getComputedStyle(e);
      if ((s.overflowY === 'auto' || s.overflowY === 'scroll') && e.scrollHeight > e.clientHeight + 50) {
        if (!best || e.scrollHeight > best.scrollHeight) best = e;
      }
    }
    return {
      totalNodes: all.length, contentVisibilityAuto: cs, backdropFilter: bf,
      boxShadow: sh, transitionAll: tr, images: imgs,
      scroller: best ? { cls: best.className.toString().slice(0,90), scrollHeight: best.scrollHeight, clientHeight: best.clientHeight } : null,
    };
  });
  console.log('DOM:', JSON.stringify(dom, null, 1));

  // Frame-pacing during a scripted scroll of that container
  const frames = await page.evaluate(async () => {
    const all = document.querySelectorAll('*');
    let el = null;
    for (const e of all) {
      const s = getComputedStyle(e);
      if ((s.overflowY === 'auto' || s.overflowY === 'scroll') && e.scrollHeight > e.clientHeight + 50) {
        if (!el || e.scrollHeight > el.scrollHeight) el = e;
      }
    }
    if (!el) return { error: 'no scroller' };
    const ts = [];
    let stop = false;
    const tick = t => { ts.push(t); if (!stop) requestAnimationFrame(tick); };
    requestAnimationFrame(tick);
    const step = 120, n = 60;
    for (let i = 0; i < n; i++) { el.scrollTop += step; await new Promise(r => setTimeout(r, 16)); }
    for (let i = 0; i < n; i++) { el.scrollTop -= step; await new Promise(r => setTimeout(r, 16)); }
    stop = true;
    await new Promise(r => setTimeout(r, 100));
    const d = []; for (let i = 1; i < ts.length; i++) d.push(ts[i] - ts[i-1]);
    d.sort((a,b) => a-b);
    const pct = p => d.length ? +d[Math.floor(d.length * p)].toFixed(1) : 0;
    return {
      frames: d.length,
      median: pct(0.5), p90: pct(0.9), p99: pct(0.99),
      worst: d.length ? +d[d.length-1].toFixed(1) : 0,
      janky_over_50ms: d.filter(x => x > 50).length,
      janky_over_100ms: d.filter(x => x > 100).length,
    };
  });
  console.log('SCROLL FRAME PACING:', JSON.stringify(frames, null, 1));
  await page.screenshot({ path: 'scripts/ranks.png', fullPage: false });
  await browser.close();
})();
