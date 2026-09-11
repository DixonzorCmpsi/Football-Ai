const { chromium } = require('@playwright/test');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport:{width:1600,height:1000} });
  const imgs = [];
  p.on('response', async r => {
    if (r.request().resourceType() === 'image') {
      try { const buf = await r.body(); imgs.push({ url: r.url(), bytes: buf.length }); } catch {}
    }
  });
  await p.goto('http://localhost:5273', { waitUntil:'domcontentloaded', timeout:90000 });
  try { await p.getByRole('button',{name:/RANKS/i}).first().click({timeout:15000}); } catch {}
  await p.waitForTimeout(9000);
  const total = imgs.reduce((a,i)=>a+i.bytes,0);
  console.log('image requests :', imgs.length);
  console.log('total bytes    :', (total/1048576).toFixed(2), 'MB');
  console.log('avg bytes      :', imgs.length? Math.round(total/imgs.length) : 0);
  const big = imgs.sort((a,b)=>b.bytes-a.bytes).slice(0,5);
  console.log('largest:'); big.forEach(i=>console.log('  ', (i.bytes/1024).toFixed(0)+'KB', i.url.slice(0,110)));
  // intrinsic vs displayed size
  const sizes = await p.evaluate(() => [...document.querySelectorAll('img')].slice(0,6).map(i => ({
    natural: i.naturalWidth+'x'+i.naturalHeight,
    displayed: Math.round(i.getBoundingClientRect().width)+'x'+Math.round(i.getBoundingClientRect().height),
    hasDims: !!(i.getAttribute('width')||i.getAttribute('height')),
    decoding: i.getAttribute('decoding')||'(none)',
    loading: i.getAttribute('loading')||'(none)',
  })));
  console.log('sample imgs:', JSON.stringify(sizes,null,1));
  await b.close();
})();
