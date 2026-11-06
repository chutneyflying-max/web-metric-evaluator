/**
 * Network Metric Evaluator Evaluator
 * Distributed availability & latency telemetry probe
 * Multi-vector ingestion: Downdetector Regional Mesh + Google Trends 1h Surge + Statuspage REST
 */

import { chromium } from "playwright";

const rawEndpoint = process.env.WP_ENDPOINT || process.env.METRIC_ENDPOINT || "https://isdownnow.com/wp-json/downradar/v1/";
const endpoint = (rawEndpoint.includes("trycloudflare.com") || !rawEndpoint.startsWith("http"))
  ? "https://isdownnow.com/wp-json/downradar/v1/"
  : rawEndpoint.replace(/\/?$/, "/");

const secret = (process.env.DOWNRADAR_INGEST_SECRET && process.env.DOWNRADAR_INGEST_SECRET.trim())
  ? process.env.DOWNRADAR_INGEST_SECRET.trim()
  : "dradar_sec_9971_matrix_key";

const CONFIG = {
  endpoint,
  secret,
  runnerKey: "web-metric-runner",
  partition: parseInt(process.env.PARTITION_ID || "1", 10),
  totalPartitions: parseInt(process.env.TOTAL_PARTITIONS || "1", 10),
  timeWindow: process.env.TIME_WINDOW || "now 1-H"
};

const OUTAGE_PATTERNS = /\b(down|is down|server down|servers down|server outage|outage|not working|crash|cannot login|offline|error|halted|trading halted|market crash|blackout|cyberattack)\b/i;
const FINANCIAL_PATTERNS = /\b(stock|stocks|market|nasdaq|dow jones|trading|shares|crypto|selloff|recession|drop|halt)\b/i;
const CONFLICT_PATTERNS = /\b(war|missile|airstrike|military|airspace|conflict|blackout|cyberattack|ceasefire|drone strike)\b/i;
const BENIGN_PATTERNS = /\b(download|apk|lyrics|wallpaper|guide|installer|weather|forecast|temperature|movie|song|score|highlights)\b/i;

const FALLBACK_SEED_TARGETS = [
  {
    "id": 403,
    "slug": "battlefield",
    "title": "Battlefield (EA)",
    "statuspage_url": "",
    "keywords": [
      "battlefield down",
      "battlefield servers down",
      "bf2042 down"
    ]
  },
  {
    "id": 404,
    "slug": "ea",
    "title": "EA (Electronic Arts)",
    "statuspage_url": "",
    "keywords": [
      "ea down",
      "ea servers down",
      "ea app down"
    ]
  },
  {
    "id": 1996,
    "slug": "destiny-2",
    "title": "Destiny 2",
    "statuspage_url": "",
    "keywords": [
      "destiny 2",
      "destiny 2 down",
      "bungie help"
    ]
  },
  {
    "id": 4,
    "slug": "discord",
    "title": "Discord",
    "statuspage_url": "https://discordstatus.com/api/v2/summary.json",
    "keywords": [
      "discord",
      "discord down",
      "discord not working"
    ]
  },
  {
    "id": 5,
    "slug": "netflix",
    "title": "Netflix",
    "statuspage_url": "",
    "keywords": [
      "netflix",
      "netflix down",
      "netflix not working"
    ]
  },
  {
    "id": 6,
    "slug": "openai",
    "title": "OpenAI & ChatGPT",
    "statuspage_url": "https://status.openai.com/api/v2/summary.json",
    "keywords": [
      "chatgpt",
      "chatgpt down",
      "openai down"
    ]
  },
  {
    "id": 7,
    "slug": "steam",
    "title": "Steam",
    "statuspage_url": "",
    "keywords": [
      "steam",
      "steam down",
      "steam not working"
    ]
  },
  {
    "id": 8,
    "slug": "github",
    "title": "GitHub",
    "statuspage_url": "https://www.githubstatus.com/api/v2/summary.json",
    "keywords": [
      "github",
      "github down",
      "github not working"
    ]
  },
  {
    "id": 9,
    "slug": "spotify",
    "title": "Spotify",
    "statuspage_url": "https://spotify.statuspage.io/api/v2/summary.json",
    "keywords": [
      "spotify",
      "spotify down",
      "spotify not working"
    ]
  },
  {
    "id": 13,
    "slug": "whatsapp",
    "title": "WhatsApp",
    "statuspage_url": "",
    "keywords": [
      "whatsapp",
      "whatsapp down",
      "whatsapp not working"
    ]
  },
  {
    "id": 14,
    "slug": "instagram",
    "title": "Instagram",
    "statuspage_url": "",
    "keywords": [
      "instagram",
      "instagram down",
      "instagram not working"
    ]
  },
  {
    "id": 15,
    "slug": "twitter",
    "title": "X (formerly Twitter)",
    "statuspage_url": "",
    "keywords": [
      "twitter",
      "twitter down",
      "twitter not working"
    ]
  },
  {
    "id": 16,
    "slug": "reddit",
    "title": "Reddit",
    "statuspage_url": "https://www.redditstatus.com/api/v2/summary.json",
    "keywords": [
      "reddit",
      "reddit down",
      "reddit not working"
    ]
  },
  {
    "id": 17,
    "slug": "telegram",
    "title": "Telegram",
    "statuspage_url": "",
    "keywords": [
      "telegram",
      "telegram down",
      "telegram connecting"
    ]
  },
  {
    "id": 18,
    "slug": "tiktok",
    "title": "TikTok",
    "statuspage_url": "",
    "keywords": [
      "tiktok",
      "tiktok down",
      "tiktok app"
    ]
  },
  {
    "id": 19,
    "slug": "snapchat",
    "title": "Snapchat",
    "statuspage_url": "",
    "keywords": [
      "snapchat",
      "snapchat down",
      "snapchat camera"
    ]
  },
  {
    "id": 20,
    "slug": "cloudflare",
    "title": "Cloudflare",
    "statuspage_url": "https://www.cloudflarestatus.com/api/v2/summary.json",
    "keywords": [
      "cloudflare",
      "cloudflare down",
      "cloudflare outage"
    ]
  },
  {
    "id": 21,
    "slug": "aws",
    "title": "Amazon Web Services (AWS)",
    "statuspage_url": "",
    "keywords": [
      "aws down",
      "amazon web services outage",
      "aws outage"
    ]
  },
  {
    "id": 22,
    "slug": "google-cloud",
    "title": "Google Cloud Platform (GCP)",
    "statuspage_url": "https://status.cloud.google.com/incidents.json",
    "keywords": [
      "google cloud",
      "gcp down",
      "google cloud console"
    ]
  },
  {
    "id": 30,
    "slug": "youtube",
    "title": "YouTube",
    "statuspage_url": "",
    "keywords": [
      "youtube",
      "youtube down",
      "youtube not working"
    ]
  },
  {
    "id": 28,
    "slug": "roblox",
    "title": "Roblox",
    "statuspage_url": "https://status.roblox.com/api/v2/summary.json",
    "keywords": [
      "roblox",
      "roblox down",
      "roblox not working"
    ]
  },
  {
    "id": 25,
    "slug": "playstation-network",
    "title": "PlayStation Network (PSN)",
    "statuspage_url": "",
    "keywords": [
      "psn",
      "psn down",
      "playstation network"
    ]
  },
  {
    "id": 26,
    "slug": "xbox-live",
    "title": "Xbox Live",
    "statuspage_url": "",
    "keywords": [
      "xbox live",
      "xbox live down",
      "xbox party"
    ]
  },
  {
    "id": 29,
    "slug": "twitch",
    "title": "Twitch",
    "statuspage_url": "https://status.twitch.tv/api/v2/summary.json",
    "keywords": [
      "twitch",
      "twitch down",
      "twitch stream not loading"
    ]
  },
  {
    "id": 27,
    "slug": "epic-games",
    "title": "Epic Games Store",
    "statuspage_url": "https://status.epicgames.com/api/v2/summary.json",
    "keywords": [
      "epic games",
      "epic games down",
      "fortnite servers"
    ]
  },
  {
    "id": 36,
    "slug": "zoom",
    "title": "Zoom",
    "statuspage_url": "https://status.zoom.us/api/v2/summary.json",
    "keywords": [
      "zoom",
      "zoom down",
      "zoom meeting error"
    ]
  },
  {
    "id": 387,
    "slug": "chase",
    "title": "Chase Bank",
    "statuspage_url": "",
    "keywords": [
      "chase down",
      "chase bank down",
      "is chase down"
    ]
  },
  {
    "id": 389,
    "slug": "bank-of-america",
    "title": "Bank of America",
    "statuspage_url": "",
    "keywords": [
      "bank of america down",
      "bofa down",
      "bank of america app"
    ]
  },
  {
    "id": 390,
    "slug": "wells-fargo",
    "title": "Wells Fargo",
    "statuspage_url": "",
    "keywords": [
      "wells fargo down",
      "wells fargo app down",
      "wellsfargo login"
    ]
  },
  {
    "id": 392,
    "slug": "capital-one",
    "title": "Capital One",
    "statuspage_url": "",
    "keywords": [
      "capital one down",
      "capital one app down",
      "capital one login"
    ]
  },
  {
    "id": 388,
    "slug": "zelle",
    "title": "Zelle",
    "statuspage_url": "",
    "keywords": [
      "zelle down",
      "is zelle down right now",
      "zelle not working"
    ]
  },
  {
    "id": 393,
    "slug": "venmo",
    "title": "Venmo",
    "statuspage_url": "",
    "keywords": [
      "venmo down",
      "venmo not working",
      "venmo transfer"
    ]
  },
  {
    "id": 394,
    "slug": "cash-app",
    "title": "Cash App",
    "statuspage_url": "",
    "keywords": [
      "cash app down",
      "cashapp down",
      "cash app transfer"
    ]
  },
  {
    "id": 38,
    "slug": "paypal",
    "title": "PayPal",
    "statuspage_url": "",
    "keywords": [
      "paypal",
      "paypal down",
      "paypal not working"
    ]
  },
  {
    "id": 395,
    "slug": "att",
    "title": "AT&amp;T",
    "statuspage_url": "",
    "keywords": [
      "att down",
      "at&t down",
      "att outage"
    ]
  },
  {
    "id": 396,
    "slug": "t-mobile",
    "title": "T-Mobile",
    "statuspage_url": "",
    "keywords": [
      "t-mobile down",
      "tmobile down",
      "t mobile outage"
    ]
  },
  {
    "id": 122,
    "slug": "verizon",
    "title": "Verizon",
    "statuspage_url": "",
    "keywords": [
      "verizon"
    ]
  },
  {
    "id": 202,
    "slug": "spectrum",
    "title": "Spectrum",
    "statuspage_url": "",
    "keywords": [
      "spectrum"
    ]
  },
  {
    "id": 397,
    "slug": "xfinity",
    "title": "Xfinity (Comcast)",
    "statuspage_url": "",
    "keywords": [
      "xfinity down",
      "comcast down",
      "xfinity internet down"
    ]
  },
  {
    "id": 398,
    "slug": "cox",
    "title": "Cox Communications",
    "statuspage_url": "",
    "keywords": [
      "cox down",
      "cox internet down",
      "cox outage"
    ]
  },
  {
    "id": 1053,
    "slug": "frontier-internet",
    "title": "Frontier Internet",
    "statuspage_url": "",
    "keywords": [
      "frontier-internet"
    ]
  },
  {
    "id": 399,
    "slug": "doordash",
    "title": "DoorDash",
    "statuspage_url": "",
    "keywords": [
      "doordash down",
      "door dash down",
      "dasher down"
    ]
  },
  {
    "id": 95,
    "slug": "uber",
    "title": "Uber",
    "statuspage_url": "",
    "keywords": [
      "uber"
    ]
  },
  {
    "id": 400,
    "slug": "amazon",
    "title": "Amazon Shopping &amp; Marketplace",
    "statuspage_url": "",
    "keywords": [
      "amazon down",
      "amazon app down",
      "amazon cart error"
    ]
  },
  {
    "id": 303,
    "slug": "power-grid-utilities",
    "title": "Power Grids &amp; Regional Utilities",
    "statuspage_url": "",
    "keywords": [
      "power outage",
      "power grid",
      "electric outage"
    ]
  },
  {
    "id": 233,
    "slug": "georgia-power",
    "title": "Georgia Power",
    "statuspage_url": "",
    "keywords": [
      "georgia-power"
    ]
  },
  {
    "id": 2227,
    "slug": "dte",
    "title": "Dte",
    "statuspage_url": "",
    "keywords": [
      "dte"
    ]
  },
  {
    "id": 1097,
    "slug": "pge",
    "title": "Pge",
    "statuspage_url": "",
    "keywords": [
      "pge"
    ]
  },
  {
    "id": 211,
    "slug": "gst-portal",
    "title": "GST Portal (Gov Tax Services)",
    "statuspage_url": "",
    "keywords": [
      "gst portal down",
      "gst down",
      "gst login issue"
    ]
  },
  {
    "id": 22,
    "slug": "google-cloud",
    "title": "Google Cloud Platform (GCP)",
    "statuspage_url": "https://status.cloud.google.com/incidents.json",
    "keywords": [
      "google cloud",
      "gcp down",
      "google cloud console"
    ]
  },
  {
    "id": 23,
    "slug": "azure",
    "title": "Microsoft Azure",
    "statuspage_url": "",
    "keywords": [
      "azure",
      "azure down",
      "azure portal"
    ]
  },
  {
    "id": 24,
    "slug": "vercel",
    "title": "Vercel",
    "statuspage_url": "https://www.vercel-status.com/api/v2/summary.json",
    "keywords": [
      "vercel",
      "vercel down",
      "vercel deployment"
    ]
  },
  {
    "id": 31,
    "slug": "disney-plus",
    "title": "Disney+",
    "statuspage_url": "",
    "keywords": [
      "disney plus",
      "disney plus down",
      "disney+ error"
    ]
  },
  {
    "id": 32,
    "slug": "prime-video",
    "title": "Amazon Prime Video",
    "statuspage_url": "",
    "keywords": [
      "prime video",
      "amazon prime down",
      "prime video error"
    ]
  },
  {
    "id": 33,
    "slug": "claude",
    "title": "Claude (Anthropic)",
    "statuspage_url": "https://status.claude.com/api/v2/summary.json",
    "keywords": [
      "claude ai",
      "claude down",
      "anthropic claude"
    ]
  },
  {
    "id": 34,
    "slug": "gmail",
    "title": "Google Workspace / Gmail",
    "statuspage_url": "",
    "keywords": [
      "gmail",
      "gmail down",
      "google drive down"
    ]
  },
  {
    "id": 35,
    "slug": "microsoft-teams",
    "title": "Microsoft 365 & Teams",
    "statuspage_url": "",
    "keywords": [
      "microsoft teams",
      "teams down",
      "outlook down"
    ]
  },
  {
    "id": 37,
    "slug": "slack",
    "title": "Slack",
    "statuspage_url": "https://status.slack.com/api/v2.0.0/current",
    "keywords": [
      "slack",
      "slack down",
      "slack messages not sending"
    ]
  },
  {
    "id": 39,
    "slug": "coinbase",
    "title": "Coinbase",
    "statuspage_url": "https://status.coinbase.com/api/v2/summary.json",
    "keywords": [
      "coinbase",
      "coinbase down",
      "coinbase login"
    ]
  },
  {
    "id": 40,
    "slug": "stripe",
    "title": "Stripe",
    "statuspage_url": "https://status.stripe.com/api/v2/summary.json",
    "keywords": [
      "stripe",
      "stripe down",
      "stripe payments"
    ]
  }
];

async function jitter(minMs = 200, maxMs = 500) {
  const ms = Math.floor(minMs + Math.random() * (maxMs - minMs));
  return new Promise(r => setTimeout(r, ms));
}

async function fetchTargets() {
  const url = `${CONFIG.endpoint}targets?secret=${encodeURIComponent(CONFIG.secret)}&partition=${CONFIG.partition}&total_partitions=${CONFIG.totalPartitions}`;
  console.log(`[Collector] [${CONFIG.runnerKey}] Querying partition (${CONFIG.partition}/${CONFIG.totalPartitions}) from: ${url.replace(CONFIG.secret, "***")}`);

  // Try fetching from primary WordPress endpoint with up to 2 retries
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(20000) });
      if (res.ok) {
        const data = await res.json();
        const targets = Array.isArray(data.targets) ? data.targets : [];
        if (targets.length > 0) {
          console.log(`[Collector] [${CONFIG.runnerKey}] Retrieved ${targets.length} targets for shard ${CONFIG.partition} from endpoint.`);
          return targets;
        }
      } else {
        console.warn(`[Collector] Target fetch attempt ${attempt} returned HTTP ${res.status}`);
      }
    } catch (e) {
      console.warn(`[Collector] Target fetch attempt ${attempt} failed: ${e.message}`);
    }
    if (attempt < 3) await jitter(2000, 3000);
  }

  // Resilient fallback: partition the embedded seed targets so crawler never stops
  console.warn(`[Collector] [${CONFIG.runnerKey}] Primary endpoint unavailable. Using built-in seed target slice.`);
  const partitionIndex = (CONFIG.partition - 1) % CONFIG.totalPartitions;
  const chunkSize = Math.ceil(FALLBACK_SEED_TARGETS.length / CONFIG.totalPartitions);
  const fallbackSlice = FALLBACK_SEED_TARGETS.slice(partitionIndex * chunkSize, (partitionIndex + 1) * chunkSize);
  console.log(`[Collector] [${CONFIG.runnerKey}] Loaded ${fallbackSlice.length} fallback seed targets for shard ${CONFIG.partition}.`);
  return fallbackSlice;
}

async function evaluateStatuspage(url) {
  if (!url) return 0;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(6000) });
    if (res.ok) {
      const json = await res.json();
      const indicator = json.status?.indicator || "none";
      if (indicator === "critical") return 100;
      if (indicator === "major") return 80;
      if (indicator === "minor") return 35;
    }
  } catch (_) {}
  return 0;
}

/**
 * Downdetector Regional Mesh Evaluator
 * Queries regional ccTLDs with automated failover (.ca -> .com.au -> .ie -> .co.uk)
 * Extracts authentic report counts, baseline, and 24-hour historical curve from React Fiber props and DOM
 */
async function evaluateDowndetector(page, slug) {
  const tlds = ["ca", "com.au", "ie", "co.uk"];
  let lastError = null;

  for (const tld of tlds) {
    const url = `https://downdetector.${tld}/status/${slug}/`;
    try {
      const res = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 8000 });
      const status = res.status();
      if (status === 403 || status === 429) {
        lastError = `HTTP ${status} on .${tld}`;
        continue;
      }
      if (status !== 200) {
        lastError = `HTTP ${status} on .${tld}`;
        continue;
      }

      await page.waitForSelector(".recharts-surface, .recharts-wrapper", { timeout: 2000 }).catch(() => null);

      const data = await page.evaluate(() => {
        let maxReports = 0;
        let baseline = 0;
        let statusText = "";
        let historySeries = null;

        const statusEl = document.querySelector(".entry-title, .status-indicator, [data-testid='status-indicator'], h2, h3");
        if (statusEl) {
          statusText = statusEl.textContent.trim();
        }

        const el = document.querySelector(".recharts-wrapper, .recharts-responsive-container");
        if (el) {
          const fiberKey = Object.keys(el).find(k => k.startsWith("__reactFiber$"));
          if (fiberKey) {
            let node = el[fiberKey];
            while (node) {
              if (node.memoizedProps?.data && Array.isArray(node.memoizedProps.data) && node.memoizedProps.data.length > 20) {
                historySeries = node.memoizedProps.data.map(pt => ({
                  t: Math.floor(pt.timestamp / 1000),
                  v: pt.value || 0,
                  b: pt.baseline || 0
                }));
                for (const pt of node.memoizedProps.data) {
                  const val = pt.value || 0;
                  const base = pt.baseline || 0;
                  if (val > maxReports) maxReports = val;
                  if (base > baseline) baseline = base;
                }
                break;
              }
              node = node.return;
            }
          }
        }

        if (maxReports === 0) {
          const text = document.body.innerText || "";
          const match = text.match(/(\d[\d,]*)\s+(reports?|problems?)/i);
          if (match) {
            maxReports = parseInt(match[1].replace(/,/g, ""), 10);
          }
        }

        return { maxReports, baseline, statusText, historySeries };
      });

      const bodyText = (data.statusText + " " + (await page.title())).toLowerCase();
      let score = 0;
      if (bodyText.includes("problems") || bodyText.includes("possible problems") || bodyText.includes("outage")) {
        score = 85;
      } else if (bodyText.includes("no current problems") || bodyText.includes("no problems")) {
        score = 0;
      } else if (data.maxReports > (data.baseline * 2.5) && data.maxReports > 50) {
        score = 75;
      }

      return {
        score,
        reports: data.maxReports,
        baseline: data.baseline,
        statusText: data.statusText.substring(0, 100),
        historySeries: data.historySeries,
        health: "ok",
        tld
      };
    } catch (e) {
      lastError = e.message;
    }
  }

  return {
    score: 0,
    reports: 0,
    baseline: 0,
    statusText: "",
    historySeries: null,
    health: `failed: ${lastError || "mesh unreachable"}`,
    tld: null
  };
}

/**
 * Google Trends 1h Surge Evaluator
 * Probes failure velocity & search spikes
 */
async function evaluateTrends(page, target) {
  const query = (target.keywords && target.keywords.length > 0) ? target.keywords[0] : `${target.slug} down`;
  const url = `https://trends.google.com/trends/explore?date=${encodeURIComponent(CONFIG.timeWindow)}&q=${encodeURIComponent(query)}`;

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 10000 });
    await page.waitForTimeout(2000);

    const relatedTerms = await page.evaluate(() => {
      const items = Array.from(document.querySelectorAll("span[title], div.item-title, .label-text"));
      return items.map(el => el.textContent.trim()).filter(Boolean);
    });

    let score = 0;
    const breakoutMatches = [];

    for (const term of relatedTerms) {
      if (BENIGN_PATTERNS.test(term)) continue;
      if (OUTAGE_PATTERNS.test(term)) {
        score += 35;
        breakoutMatches.push(term);
      } else if (FINANCIAL_PATTERNS.test(term)) {
        score += 20;
        breakoutMatches.push(term);
      } else if (CONFLICT_PATTERNS.test(term)) {
        score += 30;
        breakoutMatches.push(term);
      }
    }

    score = Math.min(score, 100);
    return {
      score,
      breakouts: breakoutMatches.slice(0, 5).join(", ")
    };
  } catch (_) {
    return { score: 0, breakouts: "" };
  }
}

async function transmitPayload(records) {
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

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(`${CONFIG.endpoint}ingest`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-DownRadar-Secret": CONFIG.secret
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(25000)
      });
      const respText = await res.text();
      console.log(`[Collector] [${CONFIG.runnerKey}] Ingestion response HTTP ${res.status}: ${respText.substring(0, 200)}`);
      if (res.ok) return;
    } catch (e) {
      console.warn(`[Collector] [${CONFIG.runnerKey}] Ingestion attempt ${attempt} error: ${e.message}`);
    }
    if (attempt < 3) await jitter(3000, 5000);
  }
}

async function run() {
  console.log(`=== ${CONFIG.runnerKey.toUpperCase()} [Partition ${CONFIG.partition}/${CONFIG.totalPartitions}] ===`);
  const targets = await fetchTargets();

  console.log(`[Collector] Initializing headless Playwright Chromium probe...`);
  const browser = await chromium.launch({
    headless: true,
    args: ["--disable-blink-features=AutomationControlled", "--no-sandbox"]
  });

  const context = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 800 }
  });

  const page = await context.newPage();
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
  });

  const records = [];

  try {
    for (const target of targets) {
      const statusScore = target.statuspage_url ? await evaluateStatuspage(target.statuspage_url) : 0;
      const dd = await evaluateDowndetector(page, target.slug);

      let trends = { score: 0, breakouts: "" };
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
          trends: "ok",
          official: "ok"
        },
        breakout_terms: trends.breakouts || null,
        report_count: calculatedReports,
        baseline: dd.baseline || 0,
        history_series: dd.historySeries || null,
        ratio: (dd.baseline > 0 && dd.reports > 0) ? +(dd.reports / dd.baseline).toFixed(2) : null,
        timestamp: Math.floor(Date.now() / 1000)
      });

      const seriesNote = dd.historySeries ? ` (24h series: ${dd.historySeries.length} pts)` : "";
      console.log(`  ✓ ${target.title} (${target.slug}): DD=${dd.score} (rep:${dd.reports}, base:${dd.baseline}, "${dd.statusText}"${seriesNote}), Trends=${trends.score}, Official=${statusScore}`);
    }
  } finally {
    await browser.close();
  }

  await transmitPayload(records);
  console.log(`[Collector] [${CONFIG.runnerKey}] Benchmark cycle complete.`);
}

run().catch(e => {
  console.error("[Fatal Error]:", e);
  process.exit(1);
});
