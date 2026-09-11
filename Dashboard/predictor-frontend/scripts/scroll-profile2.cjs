const { chromium } = require('@playwright/test');
const URL = process.env.APP_URL || 'http://localhost:5273';

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });
  const client = await page.context().newCDPSession(page);
  await client.send('Performance.enable');

  await page.goto(URL, { waitUntil: 'networkidle', timeout: 90000 });
  try { await page.getByRole('button', { name: /RANKS/i }).first().click({ timeout: 15000 }); } catch {}
  await page.waitForTimeout(4000);

  const dom = await page.evaluate(() => {
    const all = [...document.querySelectorAll('*')];
    const realTransition = all.filter(e => {
      const s = getComputedStyle(e);
      const d = (s.transitionDuration || '0s').split(',').some(x => parseFloat(x) > 0);
      return d && (s.transitionProperty || '').includes('all');
    });
    const anyTransition = all.filter(e => (getComputedStyle(e).transitionDuration||'0s').split(',').some(x=>parseFloat(x)>0));
    const bf = all.filter(e => { const s=getComputedStyle(e); return s.backdropFilter && s.backdropFilter!=='none'; });
    const cv = all.filter(e => getComputedStyle(e).contentVisibility === 'auto');
    return {
      totalNodes: all.length,
      elementsWithRealTransition: anyTransition.length,
      of_which_transition_ALL: realTransition.length,
      transitionAll_sample: realTransition.slice(0,4).map(e => e.className.toString().slice(0,70)),
      backdropFilter: bf.length,
      backdropFilter_sample: bf.slice(0,4).map(e => e.className.toString().slice(0,70)),
      contentVisibilityAuto: cv.length,
      images: document.querySelectorAll('img').length,
      imagesNoDims: [...document.querySelectorAll('img')].filter(i => !i.getAttribute('width') && !i.getAttribute('height')).length,
    };
  });
  console.log('DOM:', JSON.stringify(dom, null, 1));

  const before = (await client.send('Performance.getMetrics')).metrics.reduce((a,m)=>(a[m.name]=m.value,a),{});
  const frames = await page.evaluate(async () => {
    const all=[...document.querySelectorAll('*')];
    let el=null;
    for (const e of all){const s=getComputedStyle(e);
      if((s.overflowY==='auto'||s.overflowY==='scroll')&&e.scrollHeight>e.clientHeight+50){if(!el||e.scrollHeight>el.scrollHeight)el=e;}}
    if(!el) return {error:'no scroller'};
    const long=[]; const po=new PerformanceObserver(l=>{for(const e of l.getEntries()) long.push(+e.duration.toFixed(1));});
    try{po.observe({entryTypes:['longtask']});}catch{}
    const ts=[]; let stop=false;
    const tick=t=>{ts.push(t); if(!stop)requestAnimationFrame(tick);};
    requestAnimationFrame(tick);
    for(let i=0;i<80;i++){el.scrollTop+=100; await new Promise(r=>setTimeout(r,16));}
    for(let i=0;i<80;i++){el.scrollTop-=100; await new Promise(r=>setTimeout(r,16));}
    stop=true; await new Promise(r=>setTimeout(r,150)); try{po.disconnect();}catch{}
    const d=[];for(let i=1;i<ts.length;i++)d.push(ts[i]-ts[i-1]);
    const sorted=[...d].sort((a,b)=>a-b);
    const pct=p=>sorted.length?+sorted[Math.floor(sorted.length*p)].toFixed(1):0;
    return {frames:d.length,median:pct(.5),p90:pct(.9),p99:pct(.99),worst:sorted.length?+sorted[sorted.length-1].toFixed(1):0,
      over50:d.filter(x=>x>50).length,over100:d.filter(x=>x>100).length,longTasks:long.length,longTaskMax:long.length?Math.max(...long):0};
  });
  const after = (await client.send('Performance.getMetrics')).metrics.reduce((a,m)=>(a[m.name]=m.value,a),{});
  console.log('SCROLL:', JSON.stringify(frames, null, 1));
  console.log('COST during scroll:', JSON.stringify({
    layoutCount: after.LayoutCount - before.LayoutCount,
    recalcStyleCount: after.RecalcStyleCount - before.RecalcStyleCount,
    layoutDuration_s: +(after.LayoutDuration - before.LayoutDuration).toFixed(3),
    recalcStyleDuration_s: +(after.RecalcStyleDuration - before.RecalcStyleDuration).toFixed(3),
    scriptDuration_s: +(after.ScriptDuration - before.ScriptDuration).toFixed(3),
  }, null, 1));
  await browser.close();
})();
