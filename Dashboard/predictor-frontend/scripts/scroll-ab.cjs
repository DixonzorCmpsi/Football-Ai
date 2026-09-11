const { chromium } = require('@playwright/test');
const URL = process.env.APP_URL || 'http://localhost:5273';

async function run(label, blockImages) {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });
  if (blockImages) await page.route('**/*', r => (r.request().resourceType()==='image' ? r.abort() : r.continue()));
  await page.goto(URL, { waitUntil: 'domcontentloaded', timeout: 90000 });
  try { await page.getByRole('button', { name: /RANKS/i }).first().click({ timeout: 15000 }); } catch {}
  await page.waitForTimeout(9000);
  const r = await page.evaluate(async () => {
    const all=[...document.querySelectorAll('*')];
    let el=null;
    for(const e of all){const s=getComputedStyle(e);
      if((s.overflowY==='auto'||s.overflowY==='scroll')&&e.scrollHeight>e.clientHeight+50){if(!el||e.scrollHeight>el.scrollHeight)el=e;}}
    if(!el) return {error:'no scroller'};
    const long=[]; const po=new PerformanceObserver(l=>{for(const e of l.getEntries())long.push(+e.duration.toFixed(1));});
    try{po.observe({entryTypes:['longtask']});}catch{}
    const ts=[]; let stop=false; const tick=t=>{ts.push(t); if(!stop)requestAnimationFrame(tick);};
    requestAnimationFrame(tick);
    for(let p=0;p<3;p++){
      for(let i=0;i<60;i++){el.scrollTop+=100; await new Promise(r=>setTimeout(r,16));}
      for(let i=0;i<60;i++){el.scrollTop-=100; await new Promise(r=>setTimeout(r,16));}
    }
    stop=true; await new Promise(r=>setTimeout(r,150)); try{po.disconnect();}catch{}
    const d=[];for(let i=1;i<ts.length;i++)d.push(ts[i]-ts[i-1]);
    const s=[...d].sort((a,b)=>a-b); const pct=p=>s.length?+s[Math.floor(s.length*p)].toFixed(1):0;
    return {frames:d.length,p50:pct(.5),p95:pct(.95),p99:pct(.99),worst:s.length?+s[s.length-1].toFixed(1):0,
      over50:d.filter(x=>x>50).length, longTasks:long.length, longTaskTotal:+long.reduce((a,b)=>a+b,0).toFixed(0)};
  });
  console.log(label.padEnd(22), JSON.stringify(r));
  await browser.close();
}
(async () => {
  await run('WITH images:', false);
  await run('IMAGES BLOCKED:', true);
})();


