import { chromium } from 'playwright';

const WP_ENDPOINT = (process.env.WP_ENDPOINT || process.env.METRIC_ENDPOINT || '').replace(/\/?$/, '/');
const _sk = Buffer.from('RE9XTlJBREFSX0lOR0VTVF9TRUNSRVQ=', 'base64').toString('utf8');
const SECRET = (process.env.INGEST_KEY || process.env[_sk] || process.env.INGEST_SECRET || '').trim();
const RUNNER_KEY = process.env.RUNNER_KEY || 'endpoint-pulse-runner';
const PARTITION = parseInt(process.env.PARTITION_ID || '1', 10);

const EVENT_RE = /\b(down|outage|outages|not working|crash|crashing|offline|login error|blackout|server status|error code)\b/i;
const QUESTION_RE = /^(why|when|how|what|where|who|which|can|is)\b/i;
const NOISE_RE = /\b(song|lyrics|movie|trailer|episode|season|score|highlights|vs|match|recipe|lottery|jackpot|jacket|upside down|calm down|step down|sit down|pull down|let down|lay down|syndrome|podcast|album|cast|actor|actress|nfl|nba|mlb|nhl|ncaa|ufc|wwe)\b/i;

async function fetchText(url, timeoutMs = 7000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ac.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'Accept': 'application/json, application/xml, text/xml, */*'
      }
    });
    if (!res.ok) return '';
    return await res.text();
  } catch {
    return '';
  } finally {
    clearTimeout(t);
  }
}

function decodeXml(str) {
  return str
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .trim();
}

async function collectNewsRss(surges) {
  const url = 'https://news.google.com/rss/search?q=%22is+down%22+OR+%22outage%22+OR+%22github+down%22+when:1h&hl=en-US&gl=US&ceid=US:en';
  const xml = await fetchText(url, 7000);
  if (!xml) return;
  const itemBlocks = xml.match(/<item>[\s\S]*?<\/item>/g) || [];
  for (const block of itemBlocks.slice(0, 15)) {
    const titleM = block.match(/<title>([\s\S]*?)<\/title>/);
    if (!titleM) continue;
    const title = decodeXml(titleM[1]).replace(/\s+[-–—]\s+[^-]+$/, '').trim();
    if (!title || title.length < 8 || NOISE_RE.test(title)) continue;
    if (EVENT_RE.test(title)) {
      surges.push({ query: title, traffic: '+450%', value: 450, type: 'rising', geo: 'US', isBreakout: false });
    }
  }
}

async function collectAutocomplete(surges, questions) {
  const svcSeeds = [
    'is github down', 'github down', 'is steam down', 'shopify down',
    'is discord down', 'is cloudflare down', 'is claude down', 'is chatgpt down',
    'is roblox down', 'is netflix down', 'is youtube down', 'is instagram down',
    'is whatsapp down', 'is aws down', 'is playstation down', 'is xbox down',
    'is spotify down', 'is reddit down', 'is twitch down', 'is bpi down'
  ];
  const slice = svcSeeds.filter((_, idx) => (idx % 4) === ((PARTITION - 1) % 4));
  for (const seed of slice) {
    const raw = await fetchText(`https://suggestqueries.google.com/complete/search?client=firefox&q=${encodeURIComponent(seed)}`, 5000);
    if (!raw) continue;
    try {
      const parsed = JSON.parse(raw);
      const list = Array.isArray(parsed[1]) ? parsed[1] : [];
      for (const q of list.slice(0, 4)) {
        if (typeof q !== 'string' || NOISE_RE.test(q)) continue;
        if (/\b(right now|today|live|status|outage|map)\b/i.test(q) && EVENT_RE.test(q)) {
          surges.push({ query: q, traffic: '+350%', value: 350, type: 'rising', geo: 'worldwide', isBreakout: false });
        }
      }
    } catch {}
  }

  const qSeeds = ['why is ', 'how to ', 'when is ', 'why gold ', 'is there a '];
  const qSeed = qSeeds[(PARTITION - 1) % qSeeds.length];
  const qRaw = await fetchText(`https://suggestqueries.google.com/complete/search?client=firefox&q=${encodeURIComponent(qSeed)}`, 5000);
  if (qRaw) {
    try {
      const parsed = JSON.parse(qRaw);
      const list = Array.isArray(parsed[1]) ? parsed[1] : [];
      for (const q of list.slice(0, 6)) {
        if (typeof q === 'string' && q.length >= 10 && !NOISE_RE.test(q)) {
          questions.push({ query: q, traffic: '+350%', value: 350, type: 'rising', geo: 'worldwide' });
        }
      }
    } catch {}
  }
}

async function scrapeExplorePage(ctx, url, surges, questions, mode) {
  const page = await ctx.newPage();
  try {
    page.on('response', async (res) => {
      try {
        const u = res.url();
        if (u.includes('widgetdata/relatedsearches') && res.status() === 200) {
          let txt = await res.text();
          txt = txt.replace(/^\)\]\}',?\n?/, '');
          const data = JSON.parse(txt);
          const lists = data?.default?.rankedList || [];
          for (const lst of lists) {
            for (const item of (lst.rankedKeyword || []).slice(0, 25)) {
              const q = item.query || '';
              if (!q || NOISE_RE.test(q)) continue;
              const val = Number(item.value || 0);
              const fmt = String(item.formattedValue || '');
              const isBo = fmt.toLowerCase().includes('breakout') || val >= 5000;
              if (!isBo && val < 300) continue;
              const entry = {
                query: q,
                traffic: isBo ? 'Breakout' : `+${val.toLocaleString('en-US')}%`,
                value: isBo ? 50000 : val,
                type: 'rising',
                geo: 'worldwide',
                isBreakout: isBo
              };
              if (mode === 'surges' && EVENT_RE.test(q)) surges.push(entry);
              else if (mode === 'questions' && QUESTION_RE.test(q)) questions.push(entry);
            }
          }
        }
      } catch {}
    });

    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => {});
    const classicBtn = page.locator('text=Back to Classic Explore').first();
    if (await classicBtn.isVisible({ timeout: 2500 }).catch(() => false)) {
      await classicBtn.click().catch(() => {});
    }
    await page.waitForTimeout(5500);

    // Fallback DOM extraction for new Explore SPA tables
    const rows = await page.evaluate(() => {
      const out = [];
      const trs = document.querySelectorAll('table tr, [role="row"]');
      for (const tr of trs) {
        const txt = (tr.innerText || '').trim();
        if (!txt) continue;
        const lines = txt.split(/\n+/).map((s) => s.trim()).filter(Boolean);
        if (lines.length >= 2) {
          out.push({ q: lines[lines.length >= 3 ? 1 : 0], chg: lines[lines.length - 1] });
        }
      }
      return out;
    }).catch(() => []);

    for (const r of rows) {
      const q = (r.q || '').trim();
      const chg = (r.chg || '').trim();
      if (!q || q.length < 4 || NOISE_RE.test(q)) continue;
      const isBo = /breakout/i.test(chg);
      const num = parseInt(chg.replace(/[^0-9]/g, ''), 10) || 0;
      if (!isBo && num < 300) continue;
      const entry = {
        query: q,
        traffic: isBo ? 'Breakout' : `+${num.toLocaleString('en-US')}%`,
        value: isBo ? 50000 : num,
        type: 'rising',
        geo: 'worldwide',
        isBreakout: isBo
      };
      if (mode === 'surges' && EVENT_RE.test(q)) surges.push(entry);
      else if (mode === 'questions' && QUESTION_RE.test(q)) questions.push(entry);
    }
  } finally {
    await page.close().catch(() => {});
  }
}

async function collectTrendsExplorePlaywright(surges, questions) {
  let browser;
  try {
    const launchOpts = { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled'] };
    if (process.env.CHROME_PATH) launchOpts.executablePath = process.env.CHROME_PATH;
    browser = await chromium.launch(launchOpts);
    const ctx = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
      locale: 'en-US'
    });
    await scrapeExplorePage(ctx, 'https://trends.google.com/trends/explore?date=now%201-H&q=down,outage,not%20working,login%20error,downdetector', surges, questions, 'surges');
    await scrapeExplorePage(ctx, 'https://trends.google.com/trends/explore?date=now%201-H&q=why,when,how,what,is', surges, questions, 'questions');
  } catch {} finally {
    if (browser) await browser.close().catch(() => {});
  }
}

async function run() {
  if (!SECRET || !WP_ENDPOINT) {
    console.log('[Supplement] No endpoint/secret configured; skipping.');
    return;
  }
  const surges = [];
  const questions = [];

  await Promise.all([
    collectNewsRss(surges),
    collectAutocomplete(surges, questions),
    collectTrendsExplorePlaywright(surges, questions)
  ]);

  const dedup = (arr) => {
    const seen = new Set();
    return arr.filter((x) => {
      const k = String(x.query || '').toLowerCase().trim();
      if (!k || seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  };

  const uniqueSurges = dedup(surges).slice(0, 50);
  const uniqueQuestions = dedup(questions).slice(0, 30);

  console.log(`[Supplement] [${RUNNER_KEY}] Collected ${uniqueSurges.length} surges, ${uniqueQuestions.length} question vectors.`);
  if (uniqueSurges.length === 0 && uniqueQuestions.length === 0) return;

  const payload = {
    services: [],
    explore_trends: uniqueSurges,
    question_vectors: uniqueQuestions,
    pipeline_provenance: {
      runner_key: RUNNER_KEY,
      partition: PARTITION,
      total_partitions: 4,
      run_id: `supp_${Date.now()}_${Math.floor(Math.random() * 9000 + 1000)}`
    }
  };

  const _sh = Buffer.from('eC1kb3ducmFkYXItc2VjcmV0', 'base64').toString('utf8');
  const res = await fetch(`${WP_ENDPOINT}ingest`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      [_sh]: SECRET,
      'x-ingest-key': SECRET,
      'Authorization': `Bearer ${SECRET}`
    },
    body: JSON.stringify(payload)
  });
  console.log(`[Supplement] [${RUNNER_KEY}] Ingest HTTP ${res.status}`);
}

run().catch((err) => {
  console.error('[Supplement] Non-fatal error:', err?.message || err);
});
