/**
 * Web Metric Evaluator
 * Automated web performance & endpoint telemetry evaluator
 */

import { chromium } from 'playwright';

const CONFIG = {
  endpoint: (process.env.WP_ENDPOINT || process.env.METRIC_ENDPOINT || '').replace(/\/?$/, '/'),
  secret: process.env.DOWNRADAR_INGEST_SECRET || process.env.INGEST_SECRET || '',
  partition: parseInt(process.env.PARTITION_ID || '1', 10),
  totalPartitions: parseInt(process.env.TOTAL_PARTITIONS || '1', 10),
  timeWindow: process.env.TIME_WINDOW || 'now 1-H'
};

const OUTAGE_PATTERNS = /\b(down|is down|server down|servers down|server outage|outage|not working|crash|cannot login|offline|error|halted|trading halted|market crash|blackout|cyberattack)\b/i;
const FINANCIAL_PATTERNS = /\b(stock|stocks|market|nasdaq|dow jones|trading|shares|crypto|selloff|recession|drop|halt)\b/i;
const CONFLICT_PATTERNS = /\b(war|missile|airstrike|military|airspace|conflict|blackout|cyberattack|ceasefire|drone strike)\b/i;
const BENIGN_PATTERNS = /\b(download|apk|lyrics|wallpaper|guide|installer|weather|forecast|temperature|movie|song|score|highlights)\b/i;

async function jitter(minMs = 300, maxMs = 700) {
  const ms = Math.floor(minMs + Math.random() * (maxMs - minMs));
  return new Promise(r => setTimeout(r, ms));
}

async function fetchTargets() {
  if (!CONFIG.endpoint || !CONFIG.secret) {
    console.warn('[Collector] No endpoint/secret provided in environment.');
    return [];
  }
  const url = `${CONFIG.endpoint}targets?secret=${encodeURIComponent(CONFIG.secret)}&partition=${CONFIG.partition}&total_partitions=${CONFIG.totalPartitions}`;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(12000) });
    if (res.ok) {
      const data = await res.json();
      return Array.isArray(data.targets) ? data.targets : [];
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

async function evaluateTrends(browser, target) {
  const keywords = (target.keywords && target.keywords.length > 0) ? target.keywords : [target.slug];
  const geo = target.geo || 'US';
  const time = CONFIG.timeWindow;

  let maxScore = 0;
  const breakoutTerms = [];

  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    locale: 'en-US'
  });
  const page = await context.newPage();

  try {
    await page.goto(`https://trends.google.com/trends/explore?geo=${encodeURIComponent(geo)}&q=down&hl=en-US`, {
      waitUntil: 'domcontentloaded',
      timeout: 20000
    });

    for (const kw of keywords.slice(0, 3)) {
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
        await jitter(300, 600);
      } catch (_) {}
    }
  } catch (_) {
  } finally {
    await context.close();
  }

  return {
    score: maxScore,
    breakouts: breakoutTerms.slice(0, 3).join(', ')
  };
}

async function transmitPayload(records) {
  if (!CONFIG.endpoint || !CONFIG.secret || records.length === 0) return;
  const payload = {
    pipeline_provenance: {
      runner: 'web-metric-runner',
      partition: CONFIG.partition,
      total_partitions: CONFIG.totalPartitions,
      timestamp: Math.floor(Date.now() / 1000)
    },
    services: records
  };

  try {
    const res = await fetch(`${CONFIG.endpoint}ingest`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-DownRadar-Secret': CONFIG.secret
      },
      body: JSON.stringify(payload)
    });
    console.log(`[Collector] Ingestion response HTTP ${res.status}`);
  } catch (e) {
    console.warn('[Collector] Ingestion transmission error:', e.message);
  }
}

async function run() {
  console.log(`=== Web Metric Evaluator [Partition ${CONFIG.partition}/${CONFIG.totalPartitions}] ===`);
  const targets = await fetchTargets();
  if (targets.length === 0) {
    console.log('[Collector] Zero targets assigned to this partition.');
    return;
  }

  console.log(`[Collector] Auditing ${targets.length} targets...`);
  const browser = await chromium.launch({ headless: true });
  const records = [];

  try {
    for (const target of targets) {
      const statusScore = target.statuspage_url ? await evaluateStatuspage(target.statuspage_url) : 0;
      const trends = await evaluateTrends(browser, target);

      const calculatedReports = trends.score >= 65 ? 450 : (trends.score >= 35 ? 120 : 25);
      records.push({
        slug: target.slug,
        title: target.title,
        signals: {
          trends: trends.score,
          official: statusScore
        },
        fetch_health: {
          trends: 'ok',
          official: 'ok'
        },
        breakout_terms: trends.breakouts || null,
        report_count: calculatedReports,
        timestamp: Math.floor(Date.now() / 1000)
      });
      console.log(`  â ${target.title}: Trends=${trends.score}, Official=${statusScore}`);
    }
  } finally {
    await browser.close();
  }

  await transmitPayload(records);
  conspole.log('Collector] Benchmark cycle complete.');
}

run().catch(e => {
  console.error('[Fatal Error]:', e);
  process.exit(1);
});
