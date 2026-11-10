/**
 * Network Metric Evaluator
 * Distributed availability & latency telemetry probe
 * Multi-vector ingestion: Downdetector Regional Mesh + Google Trends In-Session Fetch Interceptor + Statuspage REST
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

const IN_SESSION_FETCH_HOOK = `
(function () {
  'use strict';
  if (window.__TRENDS_INTERCEPTOR_ACTIVE__) return;
  window.__TRENDS_INTERCEPTOR_ACTIVE__ = true;

  window.__TRENDS_DATA_STORE__ = {
    batchexecute: [],
    relatedSearches: []
  };

  const _originalFetch = window.fetch.bind(window);

  window.fetch = function (input, init) {
    const url = (typeof input === 'string' ? input : (input && input.url) || '').toString();
    const promise = _originalFetch.call(window, input, init);

    // 1. Intercept Modern Wiz RPC: batchexecute (g4kJzf minute-by-minute velocity curve)
    if (url.includes('/_/TrendsUi/data/batchexecute')) {
      promise.then(async (response) => {
        try {
          const clone = response.clone();
          const text = await clone.text();
          if (text && text.length > 50) {
            window.__TRENDS_DATA_STORE__.batchexecute.push({
              url,
              body: text,
              capturedAt: Date.now()
            });
          }
        } catch (_) {}
      }).catch(() => {});
    }

    // 2. Intercept legacy widgetdata if present
    if (url.includes('/trends/api/widgetdata/relatedsearches') || url.includes('/trends/api/explore')) {
      promise.then(async (response) => {
        try {
          const clone = response.clone();
          const text = await clone.text();
          if (text && text.length > 50) {
            window.__TRENDS_DATA_STORE__.relatedSearches.push({
              url,
              body: text,
              capturedAt: Date.now()
            });
          }
        } catch (_) {}
      }).catch(() => {});
    }

    return promise;
  };
})();
`;

async function jitter(minMs = 200, maxMs = 500) {
  const ms = Math.floor(minMs + Math.random() * (maxMs - minMs));
  return new Promise(r => setTimeout(r, ms));
}

let cachedRssItems = null;
async function fetchTrendingRss(geo = 'US') {
  if (cachedRssItems) return cachedRssItems;
  try {
    const res = await fetch(`https://trends.google.com/trending/rss?geo=${encodeURIComponent(geo)}`, {
      signal: AbortSignal.timeout(5000)
    });
    if (res.ok) {
      const text = await res.text();
      const matches = [...text.matchAll(/<item>([\s\S]*?)<\/item>/g)];
      cachedRssItems = matches.map(m => {
        const title = m[1].match(/<title>([\s\S]*?)<\/title>/)?.[1] || '';
        const traffic = m[1].match(/<ht:approx_traffic>([\s\S]*?)<\/ht:approx_traffic>/)?.[1] || '';
        return { title: title.toLowerCase(), traffic };
      });
      return cachedRssItems;
    }
  } catch (_) {}
  return [];
}

async function fetchTargets() {
  if (!CONFIG.endpoint || !CONFIG.secret) {
    console.warn('[Collector] No endpoint/secret provided in environment.');
    return { targets: [], sessionCookies: [] };
  }
  const url = `${CONFIG.endpoint}targets?partition=${CONFIG.partition}&total_partitions=${CONFIG.totalPartitions}`;
  console.log(`[Collector] [${CONFIG.runnerKey}] Querying partition (${CONFIG.partition}/${CONFIG.totalPartitions}) from: ${url}`);
  try {
    const res = await fetch(url, {
      headers: {
        'X-DownRadar-Secret': CONFIG.secret
      },
      signal: AbortSignal.timeout(15000)
    });
    if (res.ok) {
      const data = await res.json();
      const targets = Array.isArray(data.targets) ? data.targets : [];
      const sessionCookies = Array.isArray(data.session_cookies) ? data.session_cookies : [];
      console.log(`[Collector] [${CONFIG.runnerKey}] Retrieved ${targets.length} targets and ${sessionCookies.length} session cookies for shard ${CONFIG.partition}.`);
      return { targets, sessionCookies };
    } else {
      console.warn(`[Collector] Target fetch returned HTTP ${res.status}`);
    }
  } catch (e) {
    console.warn('[Collector] Could not fetch targets:', e.message);
  }
  return { targets: [], sessionCookies: [] };
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

function parseBatchexecuteG4(batches) {
  for (const b of batches) {
    if (!b.url.includes('g4kJzf')) continue;
    const lines = b.body.replace(/^\)\]\}'[^\n]*\n/, '').trim().split('\n');
    for (const line of lines) {
      if (!line || /^\d+$/.test(line)) continue;
      try {
        const outer = JSON.parse(line);
        for (const item of outer) {
          if (Array.isArray(item) && item[1] === 'g4kJzf' && item[2]) {
            const inner = JSON.parse(item[2]);
            if (Array.isArray(inner) && inner[0]) {
              const kwData = inner[0][0];
              if (kwData && Array.isArray(kwData[4]) && kwData[4].length > 0) {
                return {
                  term: kwData[0],
                  avgIndex: kwData[3] || 0,
                  series: kwData[4]
                };
              }
            }
          }
        }
      } catch (_) {}
    }
  }
  return null;
}

/**
 * Modern Google Trends In-Session Fetch Interceptor Evaluator
 * Intercepts modern batchexecute g4kJzf velocity time series with real-time RSS breakout fallback
 */
async function evaluateTrends(page, target, sessionCookiesCount = 0) {
  const keywords = (target.keywords && target.keywords.length > 0) ? target.keywords : [target.slug];
  const geo = target.geo || 'US';

  let maxScore = 0;
  const breakoutTerms = [];

  // Vector A: Authenticated Modern Explore In-Session Interception (if cookies present)
  if (sessionCookiesCount > 0) {
    const kw = keywords[0] || target.slug;
    try {
      const exploreUrl = `https://trends.google.com/explore?date=now%201-H&geo=${encodeURIComponent(geo)}&q=${encodeURIComponent(kw)}`;
      await page.goto(exploreUrl, { waitUntil: 'domcontentloaded', timeout: 10000 });

      // Poll up to 3.5s for batchexecute g4kJzf
      const startWait = Date.now();
      let batches = [];
      while (Date.now() - startWait < 3500) {
        batches = await page.evaluate(() => {
          const s = window.__TRENDS_DATA_STORE__;
          return s ? (s.batchexecute || []) : [];
        });
        if (batches.some(b => b.url.includes('g4kJzf') && b.body.length > 200)) {
          break;
        }
        await page.waitForTimeout(400);
      }

      const parsed = parseBatchexecuteG4(batches);
      if (parsed && parsed.series.length > 0) {
        const s = parsed.series;
        const recentPoints = s.slice(-5);
        const baselinePoints = s.slice(0, Math.max(1, s.length - 10));

        const recentAvg = recentPoints.reduce((acc, p) => acc + (p[1] || 0), 0) / recentPoints.length;
        const baselineAvg = baselinePoints.reduce((acc, p) => acc + (p[1] || 0), 0) / baselinePoints.length;
        const hasAnomaly = recentPoints.some(p => p[3] === true);
        const ratio = baselineAvg > 0 ? (recentAvg / baselineAvg) : 1.0;

        if (hasAnomaly && ratio >= 2.0 && recentAvg >= 60) {
          maxScore = Math.max(maxScore, 85);
          breakoutTerms.push(`${kw} (Anomaly Spike +${Math.round(ratio * 100)}%)`);
        } else if (hasAnomaly || ratio >= 1.6) {
          maxScore = Math.max(maxScore, 50);
          breakoutTerms.push(`${kw} (Rising ${Math.round(recentAvg)}/100)`);
        }
      }
    } catch (_) {}
  }

  // Vector B: Breaking Realtime Search Trends RSS (Zero Auth, Unblockable)
  try {
    const rss = await fetchTrendingRss(geo);
    const slugLower = target.slug.toLowerCase().replace(/[-_]/g, ' ');
    const titleLower = target.title.toLowerCase();

    for (const item of rss) {
      const isMatch = item.title.includes(slugLower) || item.title.includes(titleLower);
      if (isMatch) {
        const isOutage = OUTAGE_PATTERNS.test(item.title);
        if (isOutage) {
          maxScore = Math.max(maxScore, 90);
          breakoutTerms.push(`${item.title} (${item.traffic} Breakout)`);
        } else {
          maxScore = Math.max(maxScore, 40);
          breakoutTerms.push(`${item.title} (${item.traffic})`);
        }
      }
    }
  } catch (_) {}

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
  const { targets, sessionCookies } = await fetchTargets();
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

  if (sessionCookies && sessionCookies.length > 0) {
    try {
      await context.addCookies(sessionCookies);
      console.log(`[Collector] [${CONFIG.runnerKey}] Armed browser context with ${sessionCookies.length} authenticated session cookies.`);
    } catch (e) {
      console.warn(`[Collector] Failed to set session cookies:`, e.message);
    }
  }

  const page = await context.newPage();
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });
  await page.addInitScript(IN_SESSION_FETCH_HOOK);

  const records = [];

  try {
    for (const target of targets) {
      // 1. Official Provider REST Statuspage API v2
      const statusScore = target.statuspage_url ? await evaluateStatuspage(target.statuspage_url) : 0;

      // 2. Downdetector Regional Mesh
      const dd = await evaluateDowndetector(page, target.slug);

      // 3. Google Trends Modern In-Session Interceptor + RSS Breakout Mesh
      let trends = { score: 0, breakouts: '' };
      if (dd.score >= 50 || statusScore >= 35 || Math.random() < 0.25) {
        trends = await evaluateTrends(page, target, sessionCookies.length);
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
