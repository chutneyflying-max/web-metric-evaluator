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
 * Normalizes regional volume to US/Global baseline scale (CA x10, AU x12, IE x30, UK x4)
 * Extracts 24-hour historical curve, peak report volume, and status text from React Fiber
 */
async function evaluateDowndetector(page, slug) {
  const tlds = ["com", "ca", "co.uk", "com.au", "ie", "in"];
  let lastError = null;

  for (const tld of tlds) {
    const url = `https://downdetector.${tld}/status/${slug}/`;
    try {
      const res = await page.goto(url, { waitUntil: "commit", timeout: 10000 });
      const status = res.status();
      if (status === 429) {
        lastError = `HTTP 429 rate limit on .${tld}`;
        continue;
      }
      // Wait up to 6s for Cloudflare Turnstile challenge resolution if 403 or 200
      if (status === 403 || status === 200) {
        const isBypassed = await page.waitForFunction(() => {
          const scripts = Array.from(document.querySelectorAll("script")).map(s => s.textContent || "");
          return scripts.some(s => s.includes("CompanyType"));
        }, { timeout: 6000 }).catch(() => null);

        if (!isBypassed) {
          lastError = `HTTP ${status} challenge unresolved on .${tld}`;
          continue;
        }
      } else if (status !== 200) {
        lastError = `HTTP ${status} on .${tld}`;
        continue;
      }

      // Wait briefly for Next.js RSC script chunk
      await page.waitForFunction(() => {
        const scripts = Array.from(document.querySelectorAll("script")).map(s => s.textContent || "");
        return scripts.some(s => s.includes("CompanyType"));
      }, { timeout: 4000 }).catch(() => null);

      const multiplier = (tld === "com" ? 1 : (tld === "ca" ? 10 : (tld === "com.au" ? 12 : (tld === "ie" ? 30 : 4))));

      const data = await page.evaluate((mult) => {
        const scripts = Array.from(document.querySelectorAll("script")).map(s => s.textContent || "");
        const rscScript = scripts.find(s => s.includes("CompanyType"));

        if (rscScript) {
          // 1. Next.js RSC Streaming Parse (Fast, 100% reliable)
          const statusMatch = rscScript.match(/\\\"status\\\":\\\"([a-z]+)\\\"/);
          const status = statusMatch ? statusMatch[1] : "unknown";

          const dpRegex = /\{\\\"__typename\\\":\\\"ChartDataPointType\\\",\\\"timestampUtc\\\":\\\"([^\\"]+)\\\",\\\"reportsValue\\\":(\d+),\\\"baselineValue\\\":(\d+)\}/g;
          const historySeries = [];
          let m;
          let maxReports = 0;
          let baseline = 0;
          while ((m = dpRegex.exec(rscScript)) !== null) {
            const v = Math.round(parseInt(m[2], 10) * mult);
            const b = Math.round(parseInt(m[3], 10) * mult);
            historySeries.push({
              t: Math.floor(new Date(m[1]).getTime() / 1000),
              v,
              b
            });
            if (v > maxReports) maxReports = v;
            if (b > baseline) baseline = b;
          }

          // Symptoms
          const indRegex = /\{\\\"__typename\\\":\\\"LastIndicatorType\\\",\\\"localizedName\\\":\\\"([^\\"]+)\\\",\\\"count\\\":(\d+),\\\"percentage\\\":([\d.]+),\\\"id\\\":(\d+)\}/g;
          const symptoms = [];
          while ((m = indRegex.exec(rscScript)) !== null) {
            symptoms.push({
              id: parseInt(m[4], 10),
              name: m[1],
              count: parseInt(m[2], 10),
              percentage: parseFloat(m[3])
            });
          }

          // Cities
          const cityRegex = /\{\\\"__typename\\\":\\\"CityType\\\",\\\"id\\\":\\\"(\d+)\\\",\\\"slug\\\":\\\"([^\\"]+)\\\",\\\"name\\\":\\\"([^\\"]+)\\\"\}/g;
          const cities = [];
          while ((m = cityRegex.exec(rscScript)) !== null) {
            cities.push({
              id: m[1],
              slug: m[2],
              name: m[3]
            });
          }

          return { maxReports, baseline, statusText: status, historySeries, symptoms, cities };
        }

        // 2. Secondary Fallback: React Fiber Walk
        let maxReports = 0;
        let baseline = 0;
        let statusText = "";
        let historySeries = null;

        const el = document.querySelector(".recharts-wrapper, .recharts-responsive-container");
        if (el) {
          const fiberKey = Object.keys(el).find(k => k.startsWith("__reactFiber$"));
          if (fiberKey) {
            let node = el[fiberKey];
            while (node) {
              if (node.memoizedProps?.data && Array.isArray(node.memoizedProps.data) && node.memoizedProps.data.length > 20) {
                historySeries = node.memoizedProps.data.map(pt => ({
                  t: Math.floor(pt.timestamp / 1000),
                  v: Math.round((pt.value || 0) * mult),
                  b: Math.round((pt.baseline || 0) * mult)
                }));
                for (const pt of historySeries) {
                  if (pt.v > maxReports) maxReports = pt.v;
                  if (pt.b > baseline) baseline = pt.b;
                }
                break;
              }
              node = node.return;
            }
          }
        }

        return { maxReports, baseline, statusText, historySeries, symptoms: [], cities: [] };
      }, multiplier);

      if (!data) {
        lastError = `No telemetry chunk on .${tld}`;
        continue;
      }

      // Inspect recent 4-hour window (last 16 data points) for active/recent disruption
      let recentPeak = 0;
      let latestReport = 0;
      let latestBaseline = 0;
      if (data.historySeries && data.historySeries.length > 0) {
        const recentSlice = data.historySeries.slice(-16);
        recentPeak = Math.max(...recentSlice.map(p => p.v));
        latestReport = data.historySeries[data.historySeries.length - 1].v || 0;
        latestBaseline = data.historySeries[data.historySeries.length - 1].b || 0;
      }

      let peakRatio = 1.0;
      if (data.historySeries && data.historySeries.length > 0) {
        const baseForRatio = Math.max(1, latestBaseline || data.baseline);
        peakRatio = recentPeak / baseForRatio;
      }

      const stLower = (data.statusText || "").toLowerCase();
      let score = 0;
      if (stLower === "danger") {
        score = 85;
      } else if (stLower === "warning") {
        score = 75; // Downdetector official warning badge
      } else if (recentPeak >= 500) {
        score = 85;
      } else if (recentPeak >= 150) {
        score = 75;
      } else if (recentPeak >= 30 && peakRatio >= 3.0) {
        score = 70; // 3x breakout over baseline (e.g. Call of Duty 75 vs 10 baseline)
      } else if (recentPeak >= 50) {
        score = 50;
      } else if (recentPeak >= 20 && peakRatio >= 2.0) {
        score = 35;
      } else {
        score = 0;
      }

      const reportedVol = recentPeak > 0 ? recentPeak : latestReport;

      return {
        score,
        reports: reportedVol,
        baseline: latestBaseline || data.baseline,
        statusText: data.statusText || "ok",
        historySeries: data.historySeries,
        symptoms: data.symptoms || [],
        cities: data.cities || [],
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
    symptoms: [],
    cities: [],
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
    userAgent: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 800 }
  });

  const page = await context.newPage();
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
  });

  const AD_DOMAINS_TO_BLOCK = [
    "doubleclick.net", "criteo.com", "ziffstatic.com", "id5-sync.com",
    "adnxs.com", "rubiconproject.com", "pubmatic.com", "amazon-adsystem.com",
    "googletagservices.com", "google-analytics.com", "disqus.com", "scorecardresearch.com"
  ];
  await page.route("**/*", (route) => {
    const type = route.request().resourceType();
    const url = route.request().url();
    if (["image", "media", "font"].includes(type) || AD_DOMAINS_TO_BLOCK.some(d => url.includes(d))) {
      return route.abort();
    }
    return route.continue();
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

      // CRITICAL FIX: Never default healthy services to 25! If no signal, report count is 0.
      const calculatedReports = dd.reports > 0 
        ? dd.reports 
        : (trends.score >= 65 ? 450 : (trends.score >= 35 ? 120 : 0));

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
        symptoms: dd.symptoms || null,
        cities: dd.cities || null,
        ratio: (dd.baseline > 0 && dd.reports > 0) ? +(dd.reports / dd.baseline).toFixed(2) : null,
        timestamp: Math.floor(Date.now() / 1000)
      });

      const seriesNote = dd.historySeries ? ` (24h series: ${dd.historySeries.length} pts, peak:${dd.reports}, tld:.${dd.tld})` : ` [${dd.health}]`;
      console.log(`  ✓ ${target.title} (${target.slug}): DD=${dd.score} (rep:${calculatedReports}, base:${dd.baseline}, "${dd.statusText}"${seriesNote}), Trends=${trends.score}, Official=${statusScore}`);
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
