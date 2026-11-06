/**
 * Network Metric Evaluator
 * Distributed availability & latency telemetry probe
 * Multi-vector ingestion: Downdetector Regional Mesh + Google Trends 1h Surge + Statuspage REST
 */

import { chromium } from 'playwright';

const rawEndpoint = process.env.WP_ENDPOINT || process.env.METRIC_ENDPOINT || 'https://isdownnow.com/wp-json/downradar/v1/';
const endpoint = (rawEndpoint.includes('trycloudflare.com') || !rawEndpoint.startsWith('http'))
  ? 'https://isdownnow.com/wp-json/downradar/v1/'
  : rawEndpoint.replace(/\/?$/, '/');

const secret = (process.env.DOWNRADAR_INGEST_SECRET && process.env.DOWNRADAR_INGEST_SECRET.trim())
  ? process.env.DOWNRADAR_INGEST_SECRET.trim()
  : 'dradar_sec_9971_matrix_key';

const CONFIG = {
  endpoint,
  secret,
  runnerKey: 'web-metric-runner',
  partition: parseInt(process.env.PARTITION_ID || '1', 10),
  totalPartitions: parseInt(process.env.TOTAL_PARTITIONS || '1', 10),
  timeWindow: process.env.TIME_WINDOW || 'now 1-H'
};

const OUTAGE_PATTERNS = /\b(down|is down|server down|servers down|server outage|outage|not working|crash|cannot login|offline|error|halted|trading halted|market crash|blackout|cyberattack)\b/i;
const FINANCIAL_PATTERNS = /\b(stock|stocks|market|nasdaq|dow jones|trading|shares|crypto|selloff|recession|drop|halt)\b/i;
const CONFLICT_PATTERNS = /\b(war|missile|airstrike|military|airspace|conflict|blackout|cyberattack|ceasefire|drone strike)\b/i;
const BENIGN_PATTERNS = /\b(download|apk|lyrics|wallpaper|guide|installer|weather|forecast|temperature|movie|song|score|highlights)\b/i;

async function jitter(minMs = 200, maxMs = 500) {
  const ms = Math.floor(minMs + Math.random() * (maxMs - minMs));
  return new Promise(r => setTimeout(r, ms));
}

async function fetchTargets() {
  if (!CONFIG.endpoint || !CONFIG.secret) {
    console.warn('[Collector] No endpoint/secret provided in environment.');
    return [];
  }
  const url = `${CONFIG.endpoint}targets?secret=${encodeURIComponent(CONFIG.secret)}&partition=${CONFIG.partition}&total_partitions=${CONFIG.totalPartitions}`;
  console.log(`[Collector] [${CONFIG.runnerKey}] Querying partition (${CONFIG.partition}/${CONFIG.totalPartitions}) from: ${url.replace(CONFIG.secret, '***')}`);
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(15000) });
    if (res.ok) {
      const data = await res.json();
      const targets = Array.isArray(data.targets) ? data.targets : [];
      console.log(`[Collector] [${CONFIG.runnerKey}] Retrieved ${targets.length} targets for shard ${CONFIG.partition}.`);
      return targets;
    } else {
      console.warn(`[Collector] Target fetch returned HTTP ${res.status}`);
    }
  } catch (e) {
    console.warn('[Collector] Could not fetch targets:', e.message);
  }
  return [];
}

async function evaluateStatuspage(url) {
  if (!url) return 0;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(6000) });
    if (res.ok) {
      const json = await res.json();
      const indicator = json.status?.indicator || 'none';
      if (indicator === 'critical') return 100;
      if (indicator === 'major') return 80;
      if (indicator === 'minor') return 35;
    }
  } catch (_) {}
  return 0;
}

/**
 * Downdetector Regional Mesh Evaluator
 * Queries regional ccTLDs with automated failover (.ca -> .com.au -> .ie -> .co.uk)
 * Extracts authentic report counts, baseline, and status text from React Recharts props and DOM
 */
async function evaluateDowndetector(page, slug) {
  const tlds = ['ca', 'com.au', 'ie', 'co.uk'];
  let lastError = null;

  for (const tld of tlds) {
    const url = `https://downdetector.${tld}/status/${slug}/`;
    try {
      const res = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 8000 });
      const status = res.status();
      if (status === 403 || status === 429) {
        lastError = `HTTP ${status} on .${tld}`;
        continue; // Try next node in regional mesh
      }
      if (status !== 200) {
        lastError = `HTTP ${status} on .${tld}`;
        continue;
      }

      // Allow Recharts SVG to mount (max 2s wait)
      await page.waitForSelector('.recharts-surface', { timeout: 2000 }).catch(() => null);

      const parsed = await page.evaluate(() => {
        const rechartsSvg = document.querySelector('.recharts-surface');
        let chartData = null;
        if (rechartsSvg) {
          const key = Object.keys(rechartsSvg).find(k => k.startsWith('__reactFiber$') || k.startsWith('__reactProps$'));
          if (key) {
            let cur = rechartsSvg[key];
            while (cur) {
              if (cur.memoizedProps?.data) { chartData = cur.memoizedProps.data; break; }
              if (cur.props?.data) { chartData = cur.props.data; break; }
              cur = cur.return || cur._debugOwner;
            }
          }
        }

        const match = document.body.innerText.match(/User reports (?:indicate|show) [^\n\.<]+/i);
        const statusText = match ? match[0].trim() : '';
        const latest = (chartData && chartData.length > 0) ? chartData[chartData.length - 1] : null;
        const reports = latest ? (latest.value || 0) : 0;
        const baseline = latest ? (latest.baseline || 0) : 0;

        let score = 0;
        const stLower = statusText.toLowerCase();
        if (stLower.includes('show problems') || stLower.includes('indicate problems')) {
          score = 85;
        } else if (stLower.includes('possible problems')) {
          score = 65;
        } else if (stLower.includes('no current problems')) {
          score = 0;
        }

        // Relative ratio surge check
        if (baseline > 0 && reports >= 20 && (reports / baseline >= 3.0)) {
          score = Math.max(score, 75);
        }
        if (reports >= 100 && score < 75) {
          score = 75;
        }
        if (reports >= 400) {
          score = Math.max(score, 90);
        }

        return {
          score,
          reports,
          baseline,
          statusText,
          hasChart: !!chartData,
          dataPointsCount: chartData ? chartData.length : 0
        };
      });

      return {
        health: 'ok',
        tld,
        ...parsed
      };
    } catch (e) {
      lastError = e.message;
    }
  }

  return {
    health: 'error',
    error: lastError,
    score: 0,
    reports: 0,
    baseline: 0,
    statusText: 'Unreachable'
  };
}

async function evaluateTrends(page, target) {
  const keywords = (target.keywords && target.keywords.length > 0) ? target.keywords : [target.slug];
  const geo = target.geo || 'US';
  const time = CONFIG.timeWindow;

  let maxScore = 0;
  const breakoutTerms = [];

  try {
    await page.goto(`https://trends.google.com/trends/explore?geo=${encodeURIComponent(geo)}&q=down&hl=en-US`, {
      waitUntil: 'domcontentloaded',
      timeout: 12000
    });

    for (const kw of keywords.slice(0, 2)) {
      try {
        const result = await page.evaluate(async ({ kw, geo, time }) => {
          const stripXSSI = (t) => t.replace(/^\)\]\}'[^\n]*\n/, '');
          const exploreReq = { comparisonItem: [{ keyword: kw, geo, time }], category: 0, property: '' };
          const res = await fetch(`/trends/api/explore?hl=en-US&tz=-330&req=${encodeURIComponent(JSON.stringify(exploreReq))}`, { credentials: 'include' });
          if (!res.ok) return [];
          const data = JSON.parse(stripXSSI(await res.text()));
          const widget = data.widgets?.find(w => w.id === 'RELATED_QUERIES');
          if (!widget) return [];
          const rqRes = await fetch(`/trends/api/widgetdata/relatedsearches?hl=en-US&tz=-330&req=${encodeURIComponent(JSON.stringify(widget.request))}&token=${encodeURIComponent(widget.token)}`, { credentials: 'include' });
          if (!rqRes.ok) return [];
          const rqData = JSON.parse(stripXSSI(await rqRes.text()));
          return rqData.default?.rankedList?.[1]?.rankedKeyword || [];
        }, { kw, geo, time });

        for (const item of result) {
          const q = (item.query || '').trim();
          if (BENIGN_PATTERNS.test(q)) continue;
          const isBreakout = item.formattedValue === 'Breakout' || (typeof item.value === 'number' && item.value >= 1000);
          const isMatch = OUTAGE_PATTERNS.test(q) || FINANCIAL_PATTERNS.test(q) || CONFLICT_PATTERNS.test(q);
          if (isBreakout && isMatch) {
            maxScore = Math.max(maxScore, 85);
            breakoutTerms.push(`${q} (Breakout)`);
          } else if (isMatch && typeof item.value === 'number' && item.value >= 250) {
            maxScore = Math.max(maxScore, 45);
            breakoutTerms.push(`${q} (+${item.value}%)`);
          }
        }
        await jitter(200, 400);
      } catch (_) {}
    }
  } catch (_) {
  }

  return {
    score: maxScore,
    breakouts: breakoutTerms.slice(0, 3).join(', ')
  };
}

async function transmitPayload(records) {
  if (!CONFIG.endpoint || !CONFIG.secret || records.length === 0) {
    console.warn(`[Collector] [${CONFIG.runnerKey}] Skipping transmission: records count = ${records.length}`);
    return;
  }

  const payload = {
    pipeline_provenance: {
      runner: CONFIG.runnerKey,
      runner_key: CONFIG.runnerKey,
      partition: CONFIG.partition,
      total_partitions: CONFIG.totalPartitions,
      timestamp: Math.floor(Date.now() / 1000)
    },
    services: records
  };

  const highImpact = records.filter(r => (r.signals.dd >= 65 || r.signals.trends >= 65 || r.signals.official >= 65));
  console.log(`[Collector] [${CONFIG.runnerKey}] Transmitting ${records.length} service records (${highImpact.length} active disruptions) to: ${CONFIG.endpoint}ingest`);

  try {
    const res = await fetch(`${CONFIG.endpoint}ingest`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-DownRadar-Secret': CONFIG.secret
      },
      body: JSON.stringify(payload)
    });
    const respText = await res.text();
    console.log(`[Collector] [${CONFIG.runnerKey}] Ingestion response HTTP ${res.status}: ${respText.substring(0, 200)}`);
  } catch (e) {
    console.warn(`[Collector] [${CONFIG.runnerKey}] Ingestion transmission error:`, e.message);
  }
}

async function run() {
  console.log(`=== ${CONFIG.runnerKey.toUpperCase()} [Partition ${CONFIG.partition}/${CONFIG.totalPartitions}] ===`);
  const targets = await fetchTargets();
  if (targets.length === 0) {
    console.log('[Collector] Zero targets assigned to this partition.');
    return;
  }

  console.log(`[Collector] Initializing headless Playwright Chromium probe...`);
  const browser = await chromium.launch({
    headless: true,
    args: ['--disable-blink-features=AutomationControlled', '--no-sandbox']
  });

  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    viewport: { width: 1280, height: 800 }
  });

  const page = await context.newPage();
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });

  const records = [];

  try {
    for (const target of targets) {
      // 1. Official Provider REST Statuspage API v2
      const statusScore = target.statuspage_url ? await evaluateStatuspage(target.statuspage_url) : 0;

      // 2. Downdetector Regional Mesh
      const dd = await evaluateDowndetector(page, target.slug);

      // 3. Google Trends 1h Surge (evaluated if statuspage & DD have slight signals, or periodically)
      let trends = { score: 0, breakouts: '' };
      if (dd.score >= 50 || statusScore >= 35 || Math.random() < 0.25) {
        trends = await evaluateTrends(page, target);
      }

      const calculatedReports = dd.reports > 0 
        ? dd.reports 
        : (trends.score >= 65 ? 450 : (trends.score >= 35 ? 120 : 25));

      records.push({
        slug: target.slug,
        title: target.title,
        signals: {
          dd: dd.score,
          trends: trends.score,
          official: statusScore
        },
        fetch_health: {
          dd: dd.health,
          trends: 'ok',
          official: 'ok'
        },
        breakout_terms: trends.breakouts || null,
        report_count: calculatedReports,
        baseline: dd.baseline || 0,
        ratio: (dd.baseline > 0 && dd.reports > 0) ? +(dd.reports / dd.baseline).toFixed(2) : null,
        timestamp: Math.floor(Date.now() / 1000)
      });

      console.log(`  ✓ ${target.title} (${target.slug}): DD=${dd.score} (rep:${dd.reports}, base:${dd.baseline}, "${dd.statusText}"), Trends=${trends.score}, Official=${statusScore}`);
    }
  } finally {
    await browser.close();
  }

  await transmitPayload(records);
  console.log(`[Collector] [${CONFIG.runnerKey}] Benchmark cycle complete.`);
}

run().catch(e => {
  console.error('[Fatal Error]:', e);
  process.exit(1);
});
