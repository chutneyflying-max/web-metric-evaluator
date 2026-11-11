/**
 * Network Metric Evaluator
 * Distributed availability & latency telemetry probe
 * Multi-vector ingestion: In-Session Google Trends Explore (Worldwide) + Statuspage REST + Fast HTTP Telemetry Probe
 */

import { chromium } from "playwright";

const rawEndpoint = process.env.WP_ENDPOINT || process.env.METRIC_ENDPOINT || "https://isdownnow.com/wp-json/downradar/v1/";
const endpoint = (rawEndpoint.includes("trycloudflare.com") || !rawEndpoint.startsWith("http"))
  ? "https://isdownnow.com/wp-json/downradar/v1/"
  : rawEndpoint.replace(/\/?$/, "/");

const secret = (process.env.DOWNRADAR_INGEST_SECRET && process.env.DOWNRADAR_INGEST_SECRET.trim())
  || (process.env.INGEST_SECRET && process.env.INGEST_SECRET.trim());

if (!secret) {
  console.error("[Fatal] DOWNRADAR_INGEST_SECRET environment variable is missing or empty. Exiting.");
  process.exit(1);
}

const CONFIG = {
  endpoint,
  secret,
  runnerKey: "web-metric-runner",
  partition: parseInt(process.env.PARTITION_ID || "1", 10),
  totalPartitions: parseInt(process.env.TOTAL_PARTITIONS || "1", 10),
  timeWindow: process.env.TIME_WINDOW || "now 1-H",
  userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
};

const OUTAGE_PATTERNS = /\b(down|is down|server down|servers down|server outage|outage|not working|crash|cannot login|login error|offline|error|failed|maintenance)\b/i;
const STRICT_DISCOVERY_OUTAGE_PATTERNS = /\b(down|is down|went down|servers? down|service down|network down|portal down|system down|api down|app down|website down|platform down|outage|outages|not working|crash|crashes|crashing|cannot login|login error|login failed|offline|maintenance)\b/i;
const BENIGN_PATTERNS = /\b(war|missile|airstrike|military|airspace|ceasefire|stock|stocks|nasdaq|dow jones|trading halted|market crash|crypto crash|shares|selloff|recession|pizza|pizzas|pizzeria|food|foods|restaurant|restaurants|cafe|coffee|burger|burgers|taco|tacos|sushi|pasta|noodle|noodles|sandwich|sandwiches|bakery|deli|menu|recipe|recipes|delivery|takeaway|takeout|near me|nearby|closest|grocery|groceries|supermarket|ice cream|dessert|donut|bagel|download|apk|lyrics|wallpaper|guide|installer|weather|forecast|temperature|snow|rain|storm|hurricane|tornado|lottery|lotto|jackpot|result|results|draw|kerala|strike|strikes|tube strike|train strike|rail strike|walkout|protest|union|theft|robbery|police|arrest|arrested|suspect|crime|shooting|movie|trailer|box office|actor|actress|album|song|songs|dance|track|singer|artist|score|highlights|vs|match|gameplay|patch notes|update|sale|deal|price|buy|jacket|jackets|coat|coats|hoodie|pillow|pillows|feather|feathers|step down|steps down|stepping down|stepped down|stand down|stands down|standing down|stood down|go down|goes down|going down|went down|come down|comes down|coming down|sit down|sits down|sitting down|sat down|lay down|put down|puts down|calm down|calms down|upside down|hands down|run down|dress down|pinned down|pipe down|write down|breakdown of|breaking down|break down|breaks down|broken down|breakdown|lies down|lying down|lie down|touch down|touchdown|takedown|take down|countdown|closing down|shut down permanently|shutting down permanently|permanent closure|permanently closed|syndrome|syndroom|lansdown|finkley|black hawk down|watership down|47 meters down|spice go down|spice go down deh|seamus o shea|josh services|josh trade|best pizza|best pizza near|the lamb lies down on broadway|lamb lies broadway|experiencing partial)\b/i;

const HIGH_PRIORITY_SLUGS = new Set([
  'ionos', 'battlefield', 'call-of-duty', 'fortnite', 'roblox', 'discord', 'steam',
  'openai', 'chatgpt', 'netflix', 'youtube', 'spotify', 'playstation-network', 'xbox-live',
  'ea', 'rockstar-games', 'epic-games', 'riot-games', 'amazon', 'cloudflare',
  'spectrum', 'att', 'verizon', 't-mobile', 'comcast-xfinity',
  'world-of-warcraft', 'battle-net', 'counter-strike-2', 'destiny-2', 'genshin-impact',
  'the-sims-4', 'dead-by-daylight', 'pubg', 'ea-sports-fc', 'warframe', 'dota-2',
  'final-fantasy-xiv', 'escape-from-tarkov', 'clash-of-clans', 'clash-royale'
]);

// Broad failure & breaking news query vectors monitored worldwide (8 vectors combined)
const BROAD_FAILURE_VECTORS = [
  "down", "outage", "not working", "login error",
  "downdetector", "maintenance", "failed", "crash"
];

const FALLBACK_SEED_TARGETS = [
  {
    "id": 4,
    "slug": "discord",
    "title": "Discord",
    "statuspage_url": "https://discordstatus.com/api/v2/summary.json",
    "keywords": [
      "discord",
      "discord down",
      "discord app",
      "discord login"
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
      "netflix error",
      "netflix streaming"
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
      "openai down",
      "chatgpt error in conversation"
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
      "steam store",
      "steam friends"
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
      "github actions",
      "github push"
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
      "spotify not playing",
      "spotify lyrics"
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
      "whatsapp web",
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
      "instagram feed",
      "instagram login"
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
      "x down",
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
      "reddit not loading",
      "reddit app"
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
      "telegram connecting",
      "telegram web"
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
      "tiktok app",
      "tiktok not working"
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
      "snapchat camera",
      "snap streaks"
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
      "cloudflare 502",
      "cloudflare error"
    ]
  },
  {
    "id": 21,
    "slug": "aws",
    "title": "Amazon Web Services (AWS)",
    "statuspage_url": "",
    "keywords": [
      "aws",
      "aws down",
      "amazon aws",
      "aws ec2 down"
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
      "google cloud console",
      "compute engine"
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
      "azure portal",
      "azure devops"
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
      "vercel deployment",
      "vercel dns"
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
      "playstation network",
      "playstation login"
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
      "xbox party",
      "xbox servers"
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
      "fortnite servers",
      "epic launcher"
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
      "roblox servers",
      "roblox login"
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
      "twitch stream not loading",
      "twitch chat"
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
      "youtube video not playing",
      "youtube studio"
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
      "disney+ error",
      "disney plus app"
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
      "prime video error",
      "amazon streaming"
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
      "anthropic claude",
      "claude error"
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
      "google drive down",
      "google meet down"
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
      "outlook down",
      "office 365 down"
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
      "zoom meeting error",
      "zoom audio"
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
      "slack messages not sending",
      "slack huddle"
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
      "paypal payment failed",
      "paypal login"
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
      "coinbase login",
      "coinbase trade error"
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
      "stripe payments",
      "stripe webhook"
    ]
  },
  {
    "id": 66,
    "slug": "airtel",
    "title": "Airtel",
    "statuspage_url": "",
    "keywords": [
      "airtel",
      "airtel down",
      "airtel network",
      "airtel 5g"
    ]
  },
  {
    "id": 67,
    "slug": "jio",
    "title": "Jio",
    "statuspage_url": "",
    "keywords": [
      "jio",
      "jio down",
      "jio network",
      "jio fiber"
    ]
  },
  {
    "id": 68,
    "slug": "royalmail",
    "title": "Royal Mail",
    "statuspage_url": "",
    "keywords": [
      "royal mail",
      "royal mail tracking",
      "royal mail down"
    ]
  },
  {
    "id": 69,
    "slug": "state-bank-of-india-sbi",
    "title": "State Bank of India (SBI)",
    "statuspage_url": "",
    "keywords": [
      "sbi",
      "sbi down",
      "yono sbi",
      "sbi netbanking"
    ]
  },
  {
    "id": 70,
    "slug": "virgin-media",
    "title": "Virgin Media",
    "statuspage_url": "",
    "keywords": [
      "virgin media",
      "virgin media down",
      "virgin broadband"
    ]
  },
  {
    "id": 71,
    "slug": "barclays",
    "title": "Barclays",
    "statuspage_url": "",
    "keywords": [
      "barclays",
      "barclays app down",
      "barclays online banking"
    ]
  },
  {
    "id": 72,
    "slug": "claro",
    "title": "Claro",
    "statuspage_url": "",
    "keywords": [
      "claro",
      "claro fora do ar",
      "claro internet",
      "claro sinal"
    ]
  },
  {
    "id": 73,
    "slug": "nubank",
    "title": "NuBank",
    "statuspage_url": "",
    "keywords": [
      "nubank",
      "nubank fora do ar",
      "pix nubank",
      "app nubank"
    ]
  },
  {
    "id": 74,
    "slug": "vivo",
    "title": "Vivo",
    "statuspage_url": "",
    "keywords": [
      "vivo",
      "vivo fora do ar",
      "vivo fibra",
      "vivo sinal"
    ]
  },
  {
    "id": 88,
    "slug": "revolut",
    "title": "Revolut",
    "statuspage_url": "",
    "keywords": [
      "revolut",
      "revolut down",
      "revolut not working",
      "revolut outage"
    ]
  },
  {
    "id": 89,
    "slug": "shopify",
    "title": "Shopify",
    "statuspage_url": "",
    "keywords": [
      "shopify",
      "shopify down",
      "shopify not working",
      "shopify outage"
    ]
  },
  {
    "id": 91,
    "slug": "bet365",
    "title": "Bet365",
    "statuspage_url": "",
    "keywords": [
      "bet365",
      "bet365 down",
      "bet365 not working",
      "bet365 outage"
    ]
  },
  {
    "id": 92,
    "slug": "binance",
    "title": "Binance",
    "statuspage_url": "",
    "keywords": [
      "binance",
      "binance down",
      "binance not working",
      "binance outage"
    ]
  },
  {
    "id": 93,
    "slug": "bsnl",
    "title": "BSNL",
    "statuspage_url": "",
    "keywords": [
      "bsnl",
      "bsnl down",
      "bsnl not working",
      "bsnl outage"
    ]
  },
  {
    "id": 94,
    "slug": "airbnb",
    "title": "Airbnb",
    "statuspage_url": "",
    "keywords": [
      "airbnb",
      "airbnb down",
      "airbnb not working",
      "airbnb outage"
    ]
  },
  {
    "id": 95,
    "slug": "uber",
    "title": "Uber",
    "statuspage_url": "",
    "keywords": [
      "uber",
      "uber down",
      "uber not working",
      "uber outage"
    ]
  },
  {
    "id": 100,
    "slug": "faceit",
    "title": "Faceit",
    "statuspage_url": "",
    "keywords": [
      "faceit",
      "faceit down",
      "faceit not working",
      "faceit outage"
    ]
  },
  {
    "id": 101,
    "slug": "letterboxd",
    "title": "Letterboxd",
    "statuspage_url": "",
    "keywords": [
      "letterboxd",
      "letterboxd down",
      "letterboxd not working",
      "letterboxd outage"
    ]
  },
  {
    "id": 103,
    "slug": "disboard",
    "title": "Disboard",
    "statuspage_url": "",
    "keywords": [
      "disboard",
      "disboard down",
      "disboard not working",
      "disboard outage"
    ]
  },
  {
    "id": 104,
    "slug": "espn",
    "title": "ESPN",
    "statuspage_url": "",
    "keywords": [
      "espn",
      "espn down",
      "espn not working",
      "espn outage"
    ]
  },
  {
    "id": 110,
    "slug": "fortnite",
    "title": "Fortnite",
    "statuspage_url": "",
    "keywords": [
      "fortnite",
      "is fortnite down",
      "fortnite servers",
      "fortnite down"
    ]
  },
  {
    "id": 111,
    "slug": "valorant",
    "title": "Valorant",
    "statuspage_url": "",
    "keywords": [
      "valorant",
      "valorant down",
      "valorant not working",
      "valorant outage"
    ]
  },
  {
    "id": 114,
    "slug": "arc-raiders",
    "title": "Arc Raiders",
    "statuspage_url": "",
    "keywords": [
      "arc-raiders",
      "arc-raiders down",
      "arc-raiders not working",
      "arc-raiders outage"
    ]
  },
  {
    "id": 116,
    "slug": "betterhelp",
    "title": "BetterHelp",
    "statuspage_url": "",
    "keywords": [
      "betterhelp",
      "betterhelp down",
      "betterhelp not working",
      "betterhelp outage"
    ]
  },
  {
    "id": 117,
    "slug": "godaddy",
    "title": "GoDaddy",
    "statuspage_url": "",
    "keywords": [
      "godaddy",
      "godaddy down",
      "godaddy not working",
      "godaddy outage"
    ]
  },
  {
    "id": 122,
    "slug": "verizon",
    "title": "Verizon",
    "statuspage_url": "",
    "keywords": [
      "verizon",
      "verizon down",
      "verizon not working",
      "verizon outage"
    ]
  },
  {
    "id": 125,
    "slug": "libby",
    "title": "Libby",
    "statuspage_url": "",
    "keywords": [
      "libby",
      "libby down",
      "libby not working",
      "libby outage"
    ]
  },
  {
    "id": 133,
    "slug": "ao3",
    "title": "AO3 (Archive of Our Own)",
    "statuspage_url": "",
    "keywords": [
      "ao3",
      "ao3 down",
      "ao3 not working",
      "ao3 outage"
    ]
  },
  {
    "id": 148,
    "slug": "mangago",
    "title": "Mangago",
    "statuspage_url": "",
    "keywords": [
      "mangago",
      "mangago down",
      "mangago not working",
      "mangago down today"
    ]
  },
  {
    "id": 165,
    "slug": "grok",
    "title": "Grok (xAI)",
    "statuspage_url": "",
    "keywords": [
      "grok",
      "grok down",
      "grok not working",
      "grok outage"
    ]
  },
  {
    "id": 166,
    "slug": "workday",
    "title": "Workday",
    "statuspage_url": "",
    "keywords": [
      "workday",
      "workday down",
      "workday not working",
      "workday outage"
    ]
  },
  {
    "id": 174,
    "slug": "wayback-machine",
    "title": "Wayback Machine",
    "statuspage_url": "",
    "keywords": [
      "wayback-machine",
      "wayback-machine down",
      "wayback-machine not working",
      "wayback-machine outage"
    ]
  },
  {
    "id": 184,
    "slug": "fanfiction-net",
    "title": "Fanfiction Net",
    "statuspage_url": "",
    "keywords": [
      "fanfiction-net",
      "fanfiction-net down",
      "fanfiction-net not working",
      "fanfiction-net outage"
    ]
  },
  {
    "id": 187,
    "slug": "scribd",
    "title": "Scribd",
    "statuspage_url": "",
    "keywords": [
      "scribd",
      "scribd down",
      "scribd not working",
      "scribd outage"
    ]
  },
  {
    "id": 202,
    "slug": "spectrum",
    "title": "Spectrum",
    "statuspage_url": "",
    "keywords": [
      "spectrum",
      "spectrum down",
      "spectrum not working",
      "spectrum outage"
    ]
  },
  {
    "id": 211,
    "slug": "gst-portal",
    "title": "GST Portal (Gov Tax Services)",
    "statuspage_url": "",
    "keywords": [
      "gst portal",
      "gst portal down",
      "gst portal login",
      "gst down",
      "gst site not working"
    ]
  },
  {
    "id": 233,
    "slug": "georgia-power",
    "title": "Georgia Power",
    "statuspage_url": "",
    "keywords": [
      "georgia-power",
      "georgia-power down",
      "georgia-power not working",
      "georgia-power outage"
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
      "electric outage",
      "blackout",
      "enmax",
      "dte",
      "georgia power",
      "hydro one",
      "pge"
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
      "is chase down",
      "chase app not working",
      "chase login",
      "is zelle down right now"
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
      "zelle not working",
      "zelle app down",
      "chase zelle down"
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
      "bank of america app",
      "bofa login",
      "bofa app down"
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
      "wellsfargo login",
      "wells fargo not working"
    ]
  },
  {
    "id": 391,
    "slug": "us-bank",
    "title": "U.S. Bank",
    "statuspage_url": "",
    "keywords": [
      "us bank down",
      "usbank down",
      "us bank app",
      "is us bank down"
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
      "capital one login",
      "capital one not working"
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
      "venmo transfer",
      "is venmo down"
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
      "cash app transfer",
      "cash app not working"
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
      "att outage",
      "att cell service",
      "att fiber down",
      "is att down"
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
      "t mobile outage",
      "tmobile no service",
      "t mobile network",
      "tmobile outage"
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
      "xfinity internet down",
      "comcast outage",
      "xfinity outage"
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
      "cox outage",
      "cox cable down",
      "cox wifi down"
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
      "dasher down",
      "doordash app error",
      "doordash not working"
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
      "amazon cart error",
      "amazon checkout down",
      "amazon not working"
    ]
  },
  {
    "id": 401,
    "slug": "uber-eats",
    "title": "Uber Eats",
    "statuspage_url": "",
    "keywords": [
      "uber eats down",
      "ubereats down",
      "uber eats ordering",
      "uber eats not working"
    ]
  },
  {
    "id": 402,
    "slug": "instacart",
    "title": "Instacart",
    "statuspage_url": "",
    "keywords": [
      "instacart down",
      "instacart shopper down",
      "instacart delivery",
      "is instacart down"
    ]
  },
  {
    "id": 403,
    "slug": "battlefield",
    "title": "Battlefield (EA)",
    "statuspage_url": "",
    "keywords": [
      "battlefield down",
      "battlefield servers down",
      "bf2042 down",
      "battlefield unable to connect"
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
      "ea app down",
      "ea sports down",
      "origin down"
    ]
  },
  {
    "id": 405,
    "slug": "war-thunder",
    "title": "War Thunder",
    "statuspage_url": "",
    "keywords": [
      "war thunder down",
      "war thunder servers down",
      "gaijin error",
      "warthunder login"
    ]
  },
  {
    "id": 406,
    "slug": "call-of-duty",
    "title": "Call of Duty (Activision)",
    "statuspage_url": "",
    "keywords": [
      "call of duty down",
      "warzone down",
      "cod servers down",
      "activision status",
      "call of duty error"
    ]
  },
  {
    "id": 407,
    "slug": "apex-legends",
    "title": "Apex Legends",
    "statuspage_url": "",
    "keywords": [
      "apex legends down",
      "apex down",
      "apex servers down",
      "apex unable to connect"
    ]
  },
  {
    "id": 408,
    "slug": "minecraft",
    "title": "Minecraft",
    "statuspage_url": "",
    "keywords": [
      "minecraft down",
      "minecraft realms down",
      "minecraft auth servers down",
      "mojang status"
    ]
  },
  {
    "id": 409,
    "slug": "league-of-legends",
    "title": "League of Legends (Riot Games)",
    "statuspage_url": "",
    "keywords": [
      "league of legends down",
      "lol servers down",
      "riot games down",
      "lol client down"
    ]
  },
  {
    "id": 410,
    "slug": "overwatch",
    "title": "Overwatch 2 (Blizzard)",
    "statuspage_url": "",
    "keywords": [
      "overwatch down",
      "overwatch 2 servers down",
      "ow2 disconnect",
      "overwatch server error"
    ]
  },
  {
    "id": 411,
    "slug": "rainbow-six-siege",
    "title": "Rainbow Six Siege (Ubisoft)",
    "statuspage_url": "",
    "keywords": [
      "rainbow six siege down",
      "r6 down",
      "r6 siege servers down",
      "ubisoft connect down"
    ]
  },
  {
    "id": 412,
    "slug": "character-ai",
    "title": "Character.AI",
    "statuspage_url": "",
    "keywords": [
      "character ai down",
      "character.ai down",
      "cai down",
      "character ai chat not working",
      "c.ai down"
    ]
  },
  {
    "id": 413,
    "slug": "life360",
    "title": "Life360",
    "statuspage_url": "",
    "keywords": [
      "life360 down",
      "life360 not updating",
      "life360 location error",
      "is life360 down"
    ]
  },
  {
    "id": 414,
    "slug": "google",
    "title": "Google Search &amp; Core Services",
    "statuspage_url": "",
    "keywords": [
      "google down",
      "google search down",
      "google not loading",
      "is google down"
    ]
  },
  {
    "id": 415,
    "slug": "paramount-plus",
    "title": "Paramount+",
    "statuspage_url": "",
    "keywords": [
      "paramount plus down",
      "paramount+ down",
      "paramount streaming error",
      "paramount app not working"
    ]
  },
  {
    "id": 416,
    "slug": "hulu",
    "title": "Hulu",
    "statuspage_url": "",
    "keywords": [
      "hulu down",
      "hulu not working",
      "hulu error code",
      "hulu buffering",
      "is hulu down"
    ]
  },
  {
    "id": 417,
    "slug": "max",
    "title": "Max (HBO Max)",
    "statuspage_url": "",
    "keywords": [
      "max down",
      "hbo max down",
      "max app error",
      "hbo down",
      "is max down"
    ]
  },
  {
    "id": 418,
    "slug": "peacock",
    "title": "Peacock",
    "statuspage_url": "",
    "keywords": [
      "peacock down",
      "peacock tv down",
      "peacock streaming",
      "peacock error"
    ]
  },
  {
    "id": 548,
    "slug": "starbucks",
    "title": "Starbucks",
    "statuspage_url": "",
    "keywords": [
      "starbucks down",
      "starbucks app down",
      "starbucks mobile order down",
      "starbucks app not working",
      "starbucks rewards down"
    ]
  },
  {
    "id": 1003,
    "slug": "janitor-ai",
    "title": "Janitor AI",
    "statuspage_url": "",
    "keywords": [
      "janitor-ai",
      "janitor-ai down",
      "janitor-ai not working",
      "janitor-ai outage"
    ]
  },
  {
    "id": 1004,
    "slug": "f1-tv",
    "title": "F1 TV",
    "statuspage_url": "",
    "keywords": [
      "f1-tv",
      "f1-tv down",
      "f1-tv not working",
      "f1-tv outage"
    ]
  },
  {
    "id": 1007,
    "slug": "sdge",
    "title": "Sdge",
    "statuspage_url": "",
    "keywords": [
      "sdge",
      "sdge down",
      "sdge not working",
      "sdge outage"
    ]
  },
  {
    "id": 1012,
    "slug": "optimum-internet",
    "title": "Optimum Internet",
    "statuspage_url": "",
    "keywords": [
      "optimum-internet",
      "optimum-internet down",
      "optimum-internet not working",
      "optimum-internet outage"
    ]
  },
  {
    "id": 1013,
    "slug": "toca-boca",
    "title": "Toca Boca",
    "statuspage_url": "",
    "keywords": [
      "toca-boca",
      "toca-boca down",
      "toca-boca not working",
      "toca-boca outage"
    ]
  },
  {
    "id": 1026,
    "slug": "centurylink",
    "title": "Centurylink",
    "statuspage_url": "",
    "keywords": [
      "centurylink",
      "centurylink down",
      "centurylink not working",
      "centurylink outage"
    ]
  },
  {
    "id": 1027,
    "slug": "redgifs",
    "title": "Redgifs",
    "statuspage_url": "",
    "keywords": [
      "redgifs",
      "redgifs down",
      "redgifs not working",
      "redgifs outage"
    ]
  },
  {
    "id": 1028,
    "slug": "comed",
    "title": "Comed",
    "statuspage_url": "",
    "keywords": [
      "comed",
      "comed down",
      "comed not working",
      "comed outage"
    ]
  },
  {
    "id": 1035,
    "slug": "energex-finder",
    "title": "Energex Finder",
    "statuspage_url": "",
    "keywords": [
      "energex-finder",
      "energex-finder down",
      "energex-finder not working",
      "energex-finder outage",
      "energex",
      "energex outage",
      "energex power outage",
      "energex outage map"
    ]
  },
  {
    "id": 1049,
    "slug": "xcel",
    "title": "Xcel",
    "statuspage_url": "",
    "keywords": [
      "xcel",
      "xcel down",
      "xcel not working",
      "xcel outage"
    ]
  },
  {
    "id": 1053,
    "slug": "frontier-internet",
    "title": "Frontier Internet",
    "statuspage_url": "",
    "keywords": [
      "frontier-internet",
      "frontier-internet down",
      "frontier-internet not working",
      "frontier-internet outage"
    ]
  },
  {
    "id": 1074,
    "slug": "beyond-bank",
    "title": "Beyond Bank",
    "statuspage_url": "",
    "keywords": [
      "beyond-bank",
      "beyond-bank down",
      "beyond-bank not working",
      "beyond-bank outage"
    ]
  },
  {
    "id": 1076,
    "slug": "myq",
    "title": "Myq",
    "statuspage_url": "",
    "keywords": [
      "myq",
      "myq down",
      "myq not working",
      "myq outage"
    ]
  },
  {
    "id": 1097,
    "slug": "pge",
    "title": "Pge",
    "statuspage_url": "",
    "keywords": [
      "pge",
      "pge down",
      "pge not working",
      "pge outage"
    ]
  },
  {
    "id": 1102,
    "slug": "rogers",
    "title": "Rogers",
    "statuspage_url": "",
    "keywords": [
      "rogers",
      "rogers down",
      "rogers not working",
      "rogers outage"
    ]
  },
  {
    "id": 1113,
    "slug": "national-lottery",
    "title": "National Lottery",
    "statuspage_url": "",
    "keywords": [
      "national-lottery",
      "national-lottery down",
      "national-lottery not working",
      "national-lottery outage"
    ]
  },
  {
    "id": 1141,
    "slug": "nationwide",
    "title": "Nationwide",
    "statuspage_url": "",
    "keywords": [
      "nationwide",
      "nationwide down",
      "nationwide not working",
      "nationwide outage"
    ]
  },
  {
    "id": 1153,
    "slug": "imessage",
    "title": "Imessage",
    "statuspage_url": "",
    "keywords": [
      "imessage",
      "imessage down",
      "imessage not working",
      "imessage outage"
    ]
  },
  {
    "id": 1154,
    "slug": "mcb",
    "title": "Mcb",
    "statuspage_url": "",
    "keywords": [
      "mcb",
      "mcb down",
      "mcb app not working",
      "mcb juice not working",
      "mcb app"
    ]
  },
  {
    "id": 1160,
    "slug": "ziply-fiber",
    "title": "Ziply Fiber",
    "statuspage_url": "",
    "keywords": [
      "ziply-fiber",
      "ziply-fiber down",
      "ziply-fiber not working",
      "ziply-fiber outage"
    ]
  },
  {
    "id": 1196,
    "slug": "ionos",
    "title": "Ionos",
    "statuspage_url": "",
    "keywords": [
      "ionos",
      "ionos down",
      "ionos not working",
      "ionos outage"
    ]
  },
  {
    "id": 1198,
    "slug": "telstra",
    "title": "Telstra",
    "statuspage_url": "",
    "keywords": [
      "telstra",
      "telstra down",
      "telstra not working",
      "telstra outage"
    ]
  },
  {
    "id": 1199,
    "slug": "ziggo",
    "title": "Ziggo",
    "statuspage_url": "",
    "keywords": [
      "ziggo",
      "ziggo down",
      "ziggo not working",
      "ziggo outage"
    ]
  },
  {
    "id": 1202,
    "slug": "fios",
    "title": "Fios",
    "statuspage_url": "",
    "keywords": [
      "fios",
      "fios down",
      "fios not working",
      "fios outage"
    ]
  },
  {
    "id": 1207,
    "slug": "outlook",
    "title": "Outlook",
    "statuspage_url": "",
    "keywords": [
      "outlook",
      "outlook down",
      "outlook not working",
      "outlook outage"
    ]
  },
  {
    "id": 1216,
    "slug": "pnm",
    "title": "Pnm",
    "statuspage_url": "",
    "keywords": [
      "pnm",
      "pnm down",
      "pnm not working",
      "pnm outage"
    ]
  },
  {
    "id": 1223,
    "slug": "optus",
    "title": "Optus",
    "statuspage_url": "",
    "keywords": [
      "optus",
      "optus down",
      "optus not working",
      "optus outage"
    ]
  },
  {
    "id": 1236,
    "slug": "metronet",
    "title": "Metronet",
    "statuspage_url": "",
    "keywords": [
      "metronet",
      "metronet down",
      "metronet not working",
      "metronet outage"
    ]
  },
  {
    "id": 1256,
    "slug": "bc-hydro",
    "title": "Bc Hydro",
    "statuspage_url": "",
    "keywords": [
      "bc-hydro",
      "bc-hydro down",
      "bc-hydro not working",
      "bc-hydro outage"
    ]
  },
  {
    "id": 1278,
    "slug": "siri",
    "title": "Siri",
    "statuspage_url": "",
    "keywords": [
      "siri",
      "siri down",
      "siri not working",
      "siri outage"
    ]
  },
  {
    "id": 1289,
    "slug": "meted",
    "title": "Meted",
    "statuspage_url": "",
    "keywords": [
      "meted",
      "meted down",
      "meted not working",
      "meted outage"
    ]
  },
  {
    "id": 1297,
    "slug": "microsoft-authenticator",
    "title": "Microsoft Authenticator",
    "statuspage_url": "",
    "keywords": [
      "microsoft-authenticator",
      "microsoft-authenticator down",
      "microsoft-authenticator not working",
      "microsoft-authenticator outage"
    ]
  },
  {
    "id": 1318,
    "slug": "superloop",
    "title": "Superloop",
    "statuspage_url": "",
    "keywords": [
      "superloop",
      "superloop down",
      "superloop not working",
      "superloop outage"
    ]
  },
  {
    "id": 1329,
    "slug": "pepco",
    "title": "Pepco",
    "statuspage_url": "",
    "keywords": [
      "pepco",
      "pepco down",
      "pepco not working",
      "pepco outage"
    ]
  },
  {
    "id": 1332,
    "slug": "gcash",
    "title": "Gcash",
    "statuspage_url": "",
    "keywords": [
      "gcash",
      "gcash down",
      "gcash not working",
      "gcash outage"
    ]
  },
  {
    "id": 1336,
    "slug": "hotworx",
    "title": "Hotworx",
    "statuspage_url": "",
    "keywords": [
      "hotworx",
      "hotworx down",
      "hotworx not working",
      "hotworx outage"
    ]
  },
  {
    "id": 1367,
    "slug": "amc",
    "title": "Amc",
    "statuspage_url": "",
    "keywords": [
      "amc",
      "amc down",
      "amc not working",
      "amc outage"
    ]
  },
  {
    "id": 1371,
    "slug": "irctc",
    "title": "Irctc",
    "statuspage_url": "",
    "keywords": [
      "irctc",
      "irctc down",
      "irctc not working",
      "irctc outage"
    ]
  },
  {
    "id": 1380,
    "slug": "quantum-fiber",
    "title": "Quantum Fiber",
    "statuspage_url": "",
    "keywords": [
      "quantum-fiber",
      "quantum-fiber down",
      "quantum-fiber not working",
      "quantum-fiber outage"
    ]
  },
  {
    "id": 1383,
    "slug": "srp",
    "title": "Srp",
    "statuspage_url": "",
    "keywords": [
      "srp",
      "srp down",
      "srp not working",
      "srp outage"
    ]
  },
  {
    "id": 1384,
    "slug": "midco",
    "title": "Midco",
    "statuspage_url": "",
    "keywords": [
      "midco",
      "midco down",
      "midco not working",
      "midco outage"
    ]
  },
  {
    "id": 1386,
    "slug": "bell-internet",
    "title": "Bell Internet",
    "statuspage_url": "",
    "keywords": [
      "bell-internet",
      "bell-internet down",
      "bell-internet not working",
      "bell-internet outage"
    ]
  },
  {
    "id": 1387,
    "slug": "hydro-quebec",
    "title": "Hydro Quebec",
    "statuspage_url": "",
    "keywords": [
      "hydro-quebec",
      "hydro-quebec down",
      "hydro-quebec not working",
      "hydro-quebec outage"
    ]
  },
  {
    "id": 1393,
    "slug": "breezeline",
    "title": "Breezeline",
    "statuspage_url": "",
    "keywords": [
      "breezeline",
      "breezeline down",
      "breezeline not working",
      "breezeline outage"
    ]
  },
  {
    "id": 1396,
    "slug": "cupra",
    "title": "Cupra",
    "statuspage_url": "",
    "keywords": [
      "cupra",
      "cupra down",
      "cupra not working",
      "cupra outage"
    ]
  },
  {
    "id": 1400,
    "slug": "surescripts",
    "title": "Surescripts",
    "statuspage_url": "",
    "keywords": [
      "surescripts",
      "surescripts down",
      "surescripts not working",
      "surescripts outage"
    ]
  },
  {
    "id": 1402,
    "slug": "ringcentral",
    "title": "Ringcentral",
    "statuspage_url": "",
    "keywords": [
      "ringcentral",
      "ringcentral down",
      "ringcentral not working",
      "ringcentral outage"
    ]
  },
  {
    "id": 1406,
    "slug": "cricfy-tv",
    "title": "Cricfy Tv",
    "statuspage_url": "",
    "keywords": [
      "cricfy-tv",
      "cricfy-tv down",
      "cricfy-tv not working",
      "cricfy-tv outage"
    ]
  },
  {
    "id": 1407,
    "slug": "sparklight",
    "title": "Sparklight",
    "statuspage_url": "",
    "keywords": [
      "sparklight",
      "sparklight down",
      "sparklight not working",
      "sparklight outage"
    ]
  },
  {
    "id": 1409,
    "slug": "cogeco",
    "title": "Cogeco",
    "statuspage_url": "",
    "keywords": [
      "cogeco",
      "cogeco down",
      "cogeco not working",
      "cogeco outage"
    ]
  },
  {
    "id": 1410,
    "slug": "cpws",
    "title": "Cpws",
    "statuspage_url": "",
    "keywords": [
      "cpws",
      "cpws down",
      "cpws not working",
      "cpws outage"
    ]
  },
  {
    "id": 1412,
    "slug": "mcdonalds",
    "title": "Mcdonalds",
    "statuspage_url": "",
    "keywords": [
      "mcdonalds",
      "mcdonalds down",
      "mcdonalds not working",
      "mcdonalds outage"
    ]
  },
  {
    "id": 1413,
    "slug": "ameren",
    "title": "Ameren",
    "statuspage_url": "",
    "keywords": [
      "ameren",
      "ameren down",
      "ameren not working",
      "ameren outage"
    ]
  },
  {
    "id": 1415,
    "slug": "eduroam",
    "title": "Eduroam",
    "statuspage_url": "",
    "keywords": [
      "eduroam",
      "eduroam down",
      "eduroam not working",
      "eduroam outage"
    ]
  },
  {
    "id": 1416,
    "slug": "global",
    "title": "Global",
    "statuspage_url": "",
    "keywords": [
      "global",
      "global down",
      "global not working",
      "global outage"
    ]
  },
  {
    "id": 1419,
    "slug": "visual-voicemail",
    "title": "Visual Voicemail",
    "statuspage_url": "",
    "keywords": [
      "visual-voicemail",
      "visual-voicemail down",
      "visual-voicemail not working",
      "visual-voicemail outage"
    ]
  },
  {
    "id": 1422,
    "slug": "smartthings",
    "title": "Smartthings",
    "statuspage_url": "",
    "keywords": [
      "smartthings",
      "smartthings down",
      "smartthings not working",
      "smartthings outage"
    ]
  },
  {
    "id": 1423,
    "slug": "imanage",
    "title": "Imanage",
    "statuspage_url": "",
    "keywords": [
      "imanage",
      "imanage down",
      "imanage not working",
      "imanage outage"
    ]
  },
  {
    "id": 1696,
    "slug": "saskpower",
    "title": "SaskPower",
    "statuspage_url": "",
    "keywords": [
      "saskpower",
      "saskpower down",
      "saskpower not working",
      "saskpower outage"
    ]
  },
  {
    "id": 1697,
    "slug": "hydro-one",
    "title": "Hydro One",
    "statuspage_url": "",
    "keywords": [
      "hydro-one",
      "hydro-one down",
      "hydro-one not working",
      "hydro-one outage"
    ]
  },
  {
    "id": 1698,
    "slug": "point-broadband",
    "title": "Point Broadband",
    "statuspage_url": "",
    "keywords": [
      "point-broadband",
      "point-broadband down",
      "point-broadband not working",
      "point-broadband outage"
    ]
  },
  {
    "id": 1699,
    "slug": "ppl-electric",
    "title": "PPL Electric",
    "statuspage_url": "",
    "keywords": [
      "ppl-electric",
      "ppl-electric down",
      "ppl-electric not working",
      "ppl-electric outage"
    ]
  },
  {
    "id": 1739,
    "slug": "ergon-energy",
    "title": "Ergon Energy",
    "statuspage_url": "",
    "keywords": [
      "ergon-energy",
      "ergon-energy down",
      "ergon-energy not working",
      "ergon-energy outage"
    ]
  },
  {
    "id": 1878,
    "slug": "ooma",
    "title": "Ooma",
    "statuspage_url": "",
    "keywords": [
      "ooma",
      "ooma down",
      "ooma not working",
      "ooma outage"
    ]
  },
  {
    "id": 1879,
    "slug": "brightspeed",
    "title": "Brightspeed",
    "statuspage_url": "",
    "keywords": [
      "brightspeed",
      "brightspeed down",
      "brightspeed not working",
      "brightspeed outage"
    ]
  },
  {
    "id": 1880,
    "slug": "fidelity",
    "title": "Fidelity",
    "statuspage_url": "",
    "keywords": [
      "fidelity",
      "fidelity down",
      "fidelity not working",
      "fidelity outage"
    ]
  },
  {
    "id": 1881,
    "slug": "google-fiber",
    "title": "Google Fiber",
    "statuspage_url": "",
    "keywords": [
      "google fiber",
      "google fiber down",
      "fiber outage",
      "google fiber internet"
    ]
  },
  {
    "id": 1886,
    "slug": "nhl-27",
    "title": "Nhl 27",
    "statuspage_url": "",
    "keywords": [
      "nhl-27",
      "nhl-27 down",
      "nhl-27 not working",
      "nhl-27 outage"
    ]
  },
  {
    "id": 1892,
    "slug": "nbn",
    "title": "Nbn",
    "statuspage_url": "",
    "keywords": [
      "nbn",
      "nbn down",
      "nbn not working",
      "nbn outage"
    ]
  },
  {
    "id": 1893,
    "slug": "astound-wifi",
    "title": "Astound Wifi",
    "statuspage_url": "",
    "keywords": [
      "astound-wifi",
      "astound-wifi down",
      "astound-wifi not working",
      "astound-wifi outage"
    ]
  },
  {
    "id": 1894,
    "slug": "sticker-go",
    "title": "Sticker Go",
    "statuspage_url": "",
    "keywords": [
      "sticker-go",
      "sticker-go down",
      "sticker-go not working",
      "sticker-go outage"
    ]
  },
  {
    "id": 1899,
    "slug": "sce",
    "title": "Sce",
    "statuspage_url": "",
    "keywords": [
      "sce",
      "sce down",
      "sce not working",
      "sce outage"
    ]
  },
  {
    "id": 1900,
    "slug": "walmart",
    "title": "Walmart",
    "statuspage_url": "",
    "keywords": [
      "walmart",
      "walmart down",
      "walmart not working",
      "walmart outage"
    ]
  },
  {
    "id": 1901,
    "slug": "grubhub",
    "title": "Grubhub",
    "statuspage_url": "",
    "keywords": [
      "grubhub",
      "grubhub down",
      "grubhub not working",
      "grubhub outage"
    ]
  },
  {
    "id": 1902,
    "slug": "xbox",
    "title": "Xbox",
    "statuspage_url": "",
    "keywords": [
      "xbox",
      "xbox down",
      "xbox not working",
      "xbox outage"
    ]
  },
  {
    "id": 1906,
    "slug": "premium-bonds-prize-checker",
    "title": "Premium Bonds Prize Checker",
    "statuspage_url": "",
    "keywords": [
      "premium-bonds-prize-checker",
      "premium-bonds-prize-checker down",
      "premium-bonds-prize-checker not working",
      "premium-bonds-prize-checker outage"
    ]
  },
  {
    "id": 1907,
    "slug": "rcs-message",
    "title": "Rcs Message",
    "statuspage_url": "",
    "keywords": [
      "rcs-message",
      "rcs-message down",
      "rcs-message not working",
      "rcs-message outage"
    ]
  },
  {
    "id": 1908,
    "slug": "premium-bond-prize-checker",
    "title": "Premium Bond Prize Checker",
    "statuspage_url": "",
    "keywords": [
      "premium-bond-prize-checker",
      "premium-bond-prize-checker down",
      "premium-bond-prize-checker not working",
      "premium-bond-prize-checker outage"
    ]
  },
  {
    "id": 1914,
    "slug": "omni-fiber",
    "title": "Omni Fiber",
    "statuspage_url": "",
    "keywords": [
      "omni-fiber",
      "omni-fiber down",
      "omni-fiber not working",
      "omni-fiber outage"
    ]
  },
  {
    "id": 1916,
    "slug": "hyperoptic",
    "title": "Hyperoptic",
    "statuspage_url": "",
    "keywords": [
      "hyperoptic",
      "hyperoptic down",
      "hyperoptic not working",
      "hyperoptic outage"
    ]
  },
  {
    "id": 1926,
    "slug": "becu",
    "title": "Becu",
    "statuspage_url": "",
    "keywords": [
      "becu",
      "becu down",
      "becu not working",
      "becu outage"
    ]
  },
  {
    "id": 1927,
    "slug": "comporium",
    "title": "Comporium",
    "statuspage_url": "",
    "keywords": [
      "comporium",
      "comporium down",
      "comporium not working",
      "comporium outage"
    ]
  },
  {
    "id": 1928,
    "slug": "centerpoint",
    "title": "Centerpoint",
    "statuspage_url": "",
    "keywords": [
      "centerpoint",
      "centerpoint down",
      "centerpoint not working",
      "centerpoint outage"
    ]
  },
  {
    "id": 1929,
    "slug": "fpl",
    "title": "Fpl",
    "statuspage_url": "",
    "keywords": [
      "fpl",
      "fpl down",
      "fpl not working",
      "fpl outage"
    ]
  },
  {
    "id": 1931,
    "slug": "tpg",
    "title": "Tpg",
    "statuspage_url": "",
    "keywords": [
      "tpg",
      "tpg down",
      "tpg not working",
      "tpg outage"
    ]
  },
  {
    "id": 1932,
    "slug": "iinet",
    "title": "Iinet",
    "statuspage_url": "",
    "keywords": [
      "iinet",
      "iinet down",
      "iinet not working",
      "iinet outage"
    ]
  },
  {
    "id": 1937,
    "slug": "gci",
    "title": "Gci",
    "statuspage_url": "",
    "keywords": [
      "gci",
      "gci down",
      "gci not working",
      "gci outage"
    ]
  },
  {
    "id": 1938,
    "slug": "optimum",
    "title": "Optimum",
    "statuspage_url": "",
    "keywords": [
      "optimum",
      "optimum down",
      "optimum not working",
      "optimum outage"
    ]
  },
  {
    "id": 1939,
    "slug": "deepseek",
    "title": "Deepseek",
    "statuspage_url": "https://status.deepseek.com/",
    "keywords": [
      "deepseek",
      "deepseek down",
      "is deepseek down",
      "status deepseek",
      "deepseek api down",
      "deepseek app not working"
    ]
  },
  {
    "id": 1940,
    "slug": "brawl-stars",
    "title": "Brawl Stars",
    "statuspage_url": "",
    "keywords": [
      "brawl stars",
      "brawl stars maintenance",
      "brawl stars down",
      "brawl stars servers",
      "brawl stars not working",
      "brawl stars maintenance break"
    ]
  },
  {
    "id": 1942,
    "slug": "energex",
    "title": "Energex",
    "statuspage_url": "",
    "keywords": [
      "energex",
      "energex down",
      "energex not working",
      "energex outage"
    ]
  },
  {
    "id": 1943,
    "slug": "telus",
    "title": "Telus",
    "statuspage_url": "",
    "keywords": [
      "telus",
      "telus down",
      "telus not working",
      "telus outage"
    ]
  },
  {
    "id": 1944,
    "slug": "osrs",
    "title": "Osrs",
    "statuspage_url": "",
    "keywords": [
      "osrs",
      "osrs down",
      "osrs not working",
      "osrs outage"
    ]
  },
  {
    "id": 1945,
    "slug": "dayz",
    "title": "Dayz",
    "statuspage_url": "",
    "keywords": [
      "dayz",
      "dayz down",
      "dayz not working",
      "dayz outage"
    ]
  },
  {
    "id": 1970,
    "slug": "runescape",
    "title": "RuneScape",
    "statuspage_url": "",
    "keywords": [
      "runescape",
      "runescape down",
      "osrs down",
      "old school runescape down",
      "jagex down",
      "runescape servers",
      "runescape login"
    ]
  },
  {
    "id": 1986,
    "slug": "widex-moment",
    "title": "Widex Moment",
    "statuspage_url": "",
    "keywords": [
      "widex-moment",
      "widex-moment down",
      "widex-moment not working",
      "widex-moment outage"
    ]
  },
  {
    "id": 1987,
    "slug": "kik",
    "title": "Kik",
    "statuspage_url": "",
    "keywords": [
      "kik",
      "kik down",
      "kik not working",
      "kik app down",
      "kik login",
      "kik messages not sending"
    ]
  },
  {
    "id": 1988,
    "slug": "mediacom",
    "title": "Mediacom",
    "statuspage_url": "",
    "keywords": [
      "mediacom",
      "mediacom down",
      "mediacom outage",
      "mediacom internet down",
      "mediacom cable"
    ]
  },
  {
    "id": 1989,
    "slug": "world-of-tanks",
    "title": "World of Tanks",
    "statuspage_url": "",
    "keywords": [
      "world of tanks",
      "world of tanks down",
      "wot down",
      "wot server maintenance",
      "world of tanks not working"
    ]
  },
  {
    "id": 1990,
    "slug": "yahoo-mail",
    "title": "Yahoo Mail",
    "statuspage_url": "",
    "keywords": [
      "yahoo mail",
      "yahoo mail down",
      "is yahoo mail down",
      "yahoo email not working",
      "yahoo login issues"
    ]
  },
  {
    "id": 1991,
    "slug": "battle-net",
    "title": "Battle.net (Blizzard)",
    "statuspage_url": "",
    "keywords": [
      "battle.net",
      "battle net down",
      "blizzard down",
      "battlenet login",
      "battlenet servers"
    ]
  },
  {
    "id": 1992,
    "slug": "nintendo-network",
    "title": "Nintendo Network (eShop)",
    "statuspage_url": "",
    "keywords": [
      "nintendo",
      "nintendo down",
      "nintendo switch online down",
      "nintendo eshop down",
      "nintendo network maintenance"
    ]
  },
  {
    "id": 1993,
    "slug": "rockstar-games",
    "title": "Rockstar Games Services",
    "statuspage_url": "",
    "keywords": [
      "rockstar games",
      "rockstar down",
      "gta online down",
      "rockstar games launcher down",
      "rockstar servers"
    ]
  },
  {
    "id": 1994,
    "slug": "riot-games",
    "title": "Riot Games",
    "statuspage_url": "",
    "keywords": [
      "riot games",
      "riot down",
      "riot client down",
      "riot servers not working",
      "riot login"
    ]
  },
  {
    "id": 1995,
    "slug": "ubisoft-connect",
    "title": "Ubisoft Connect",
    "statuspage_url": "",
    "keywords": [
      "ubisoft",
      "ubisoft down",
      "ubisoft connect down",
      "uplay down",
      "ubisoft servers"
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
      "bungie help",
      "destiny servers down",
      "destiny 2 maintenance"
    ]
  },
  {
    "id": 1997,
    "slug": "genshin-impact",
    "title": "Genshin Impact",
    "statuspage_url": "",
    "keywords": [
      "genshin impact",
      "genshin down",
      "genshin impact maintenance",
      "genshin server not responding",
      "hoyoverse down"
    ]
  },
  {
    "id": 1998,
    "slug": "pokemon-go",
    "title": "Pokemon GO",
    "statuspage_url": "",
    "keywords": [
      "pokemon go",
      "pokemon go down",
      "pokemon go login error",
      "niantic servers down",
      "pokemon go unable to authenticate"
    ]
  },
  {
    "id": 1999,
    "slug": "rocket-league",
    "title": "Rocket League",
    "statuspage_url": "",
    "keywords": [
      "rocket league",
      "rocket league down",
      "rocket league servers down",
      "rocket league matchmaking"
    ]
  },
  {
    "id": 2000,
    "slug": "counter-strike-2",
    "title": "Counter-Strike 2",
    "statuspage_url": "",
    "keywords": [
      "cs2",
      "cs2 down",
      "counter strike 2 down",
      "csgo down",
      "cs2 game coordinator down",
      "cs2 matchmaking"
    ]
  },
  {
    "id": 2001,
    "slug": "dota-2",
    "title": "Dota 2",
    "statuspage_url": "",
    "keywords": [
      "dota 2",
      "dota 2 down",
      "dota down",
      "dota 2 game coordinator",
      "dota 2 coordinator down"
    ]
  },
  {
    "id": 2002,
    "slug": "helldivers-2",
    "title": "Helldivers 2",
    "statuspage_url": "",
    "keywords": [
      "helldivers 2",
      "helldivers 2 down",
      "helldivers servers full",
      "helldivers 2 matchmaking",
      "helldivers connection error"
    ]
  },
  {
    "id": 2003,
    "slug": "final-fantasy-xiv",
    "title": "Final Fantasy XIV",
    "statuspage_url": "",
    "keywords": [
      "ffxiv",
      "ffxiv down",
      "final fantasy xiv down",
      "lodestone maintenance",
      "ffxiv servers",
      "ffxiv 2002 error"
    ]
  },
  {
    "id": 2004,
    "slug": "world-of-warcraft",
    "title": "World of Warcraft",
    "statuspage_url": "",
    "keywords": [
      "world of warcraft",
      "wow down",
      "world of warcraft down",
      "wow realm status",
      "wow maintenance",
      "blizzard wow servers"
    ]
  },
  {
    "id": 2005,
    "slug": "pubg",
    "title": "PUBG: Battlegrounds",
    "statuspage_url": "",
    "keywords": [
      "pubg",
      "pubg down",
      "pubg servers down",
      "pubg maintenance",
      "pubg not connecting"
    ]
  },
  {
    "id": 2006,
    "slug": "dead-by-daylight",
    "title": "Dead by Daylight",
    "statuspage_url": "",
    "keywords": [
      "dead by daylight",
      "dbd down",
      "dead by daylight down",
      "dbd servers",
      "dbd initialization error"
    ]
  },
  {
    "id": 2007,
    "slug": "escape-from-tarkov",
    "title": "Escape from Tarkov",
    "statuspage_url": "",
    "keywords": [
      "tarkov",
      "tarkov down",
      "escape from tarkov down",
      "eft down",
      "tarkov backend error",
      "eft matching"
    ]
  },
  {
    "id": 2008,
    "slug": "path-of-exile",
    "title": "Path of Exile",
    "statuspage_url": "",
    "keywords": [
      "path of exile",
      "poe down",
      "path of exile down",
      "poe servers",
      "poe league launch"
    ]
  },
  {
    "id": 2009,
    "slug": "warframe",
    "title": "Warframe",
    "statuspage_url": "",
    "keywords": [
      "warframe",
      "warframe down",
      "warframe servers",
      "warframe network not responding"
    ]
  },
  {
    "id": 2010,
    "slug": "clash-of-clans",
    "title": "Clash of Clans",
    "statuspage_url": "",
    "keywords": [
      "clash of clans",
      "clash of clans down",
      "coc down",
      "clash of clans maintenance break",
      "clash of clans servers"
    ]
  },
  {
    "id": 2011,
    "slug": "clash-royale",
    "title": "Clash Royale",
    "statuspage_url": "",
    "keywords": [
      "clash royale",
      "clash royale down",
      "clash royale maintenance",
      "clash royale not working",
      "clash royale servers"
    ]
  },
  {
    "id": 2012,
    "slug": "ea-sports-fc",
    "title": "EA Sports FC (FIFA)",
    "statuspage_url": "",
    "keywords": [
      "ea sports fc",
      "ea fc down",
      "fifa down",
      "ea servers down",
      "ultimate team down",
      "fut down"
    ]
  },
  {
    "id": 2013,
    "slug": "the-sims-4",
    "title": "The Sims 4",
    "statuspage_url": "",
    "keywords": [
      "the sims 4",
      "sims 4 down",
      "sims 4 gallery down",
      "the sims down",
      "sims 4 offline"
    ]
  },
  {
    "id": 2014,
    "slug": "signal",
    "title": "Signal",
    "statuspage_url": "",
    "keywords": [
      "signal",
      "signal down",
      "signal app down",
      "signal messages not delivering",
      "signal calling issues"
    ]
  },
  {
    "id": 2015,
    "slug": "bluesky",
    "title": "Bluesky Social",
    "statuspage_url": "",
    "keywords": [
      "bluesky",
      "bluesky down",
      "bsky down",
      "bluesky not loading",
      "bluesky app down"
    ]
  },
  {
    "id": 2016,
    "slug": "threads",
    "title": "Threads",
    "statuspage_url": "",
    "keywords": [
      "threads",
      "threads down",
      "instagram threads down",
      "threads not loading",
      "threads app down"
    ]
  },
  {
    "id": 2017,
    "slug": "kick",
    "title": "Kick",
    "statuspage_url": "",
    "keywords": [
      "kick",
      "kick down",
      "kick streaming down",
      "kick stream buffering",
      "kick chat down"
    ]
  },
  {
    "id": 2018,
    "slug": "chatgpt",
    "title": "ChatGPT",
    "statuspage_url": "https://status.openai.com/api/v2/summary.json",
    "downdetector_slug": "openai",
    "keywords": [
      "chatgpt",
      "chatgpt down",
      "is chatgpt down",
      "chat gpt down",
      "openai down",
      "chatgpt internal server error"
    ]
  },
  {
    "id": 2019,
    "slug": "perplexity-ai",
    "title": "Perplexity AI",
    "statuspage_url": "",
    "keywords": [
      "perplexity",
      "perplexity ai down",
      "perplexity down",
      "perplexity search not working"
    ]
  },
  {
    "id": 2020,
    "slug": "midjourney",
    "title": "Midjourney",
    "statuspage_url": "",
    "keywords": [
      "midjourney",
      "midjourney down",
      "midjourney bot not responding",
      "midjourney queue stuck",
      "midjourney server issues"
    ]
  },
  {
    "id": 2021,
    "slug": "gemini",
    "title": "Google Gemini",
    "statuspage_url": "",
    "keywords": [
      "gemini",
      "google gemini down",
      "gemini down",
      "gemini app not working",
      "gemini advanced"
    ]
  },
  {
    "id": 2022,
    "slug": "starlink",
    "title": "Starlink",
    "statuspage_url": "",
    "keywords": [
      "starlink",
      "starlink down",
      "starlink outage",
      "spacex starlink down",
      "starlink satellite internet"
    ]
  },
  {
    "id": 2023,
    "slug": "suddenlink",
    "title": "Suddenlink (Optimum)",
    "statuspage_url": "",
    "keywords": [
      "suddenlink",
      "suddenlink down",
      "suddenlink internet down",
      "suddenlink outage"
    ]
  },
  {
    "id": 2024,
    "slug": "windstream",
    "title": "Windstream (Kinetic)",
    "statuspage_url": "",
    "keywords": [
      "windstream",
      "windstream down",
      "kinetic windstream down",
      "windstream outage",
      "windstream internet"
    ]
  },
  {
    "id": 2025,
    "slug": "earthlink",
    "title": "EarthLink",
    "statuspage_url": "",
    "keywords": [
      "earthlink",
      "earthlink down",
      "earthlink internet down",
      "earthlink webmail down"
    ]
  },
  {
    "id": 2026,
    "slug": "proton-mail",
    "title": "Proton Mail",
    "statuspage_url": "",
    "keywords": [
      "proton mail",
      "protonmail down",
      "proton down",
      "proton vpn down",
      "proton mail not working"
    ]
  },
  {
    "id": 2483,
    "slug": "monday-com",
    "title": "Monday.com",
    "monitor_type": "hybrid",
    "statuspage_url": "https://status.monday.com/api/v2/summary.json",
    "geo": "US",
    "keywords": [
      "monday.com",
      "monday.com down",
      "monday.com outage",
      "is monday.com down right now",
      "monday.com status",
      "monday.com login"
    ],
    "subreddit": "mondaydotcom"
  },
  {
    "id": 2737,
    "slug": "fedex",
    "title": "FedEx",
    "monitor_type": "trends_breakout",
    "statuspage_url": "",
    "geo": "US",
    "keywords": [
      "fedex",
      "fedex down",
      "fedex tracking down",
      "is fedex down",
      "fedex tracking not working",
      "fedex ship manager down",
      "fedex delivery delays"
    ],
    "subreddit": "fedex"
  },
  {
    "id": 2738,
    "slug": "ups",
    "title": "UPS",
    "monitor_type": "trends_breakout",
    "statuspage_url": "",
    "geo": "US",
    "keywords": [
      "ups",
      "ups down",
      "ups tracking down",
      "is ups down",
      "ups tracking not working",
      "ups my choice down",
      "ups delivery delays"
    ],
    "subreddit": "ups"
  },
  {
    "slug": "verifone",
    "name": "Verifone",
    "domain": "verifone.com",
    "category": "fintech",
    "keywords": [
      "verifone outage",
      "verifone down",
      "verifone payment down",
      "verifone pos down",
      "is verifone down"
    ]
  },
  {
    "id": 2748,
    "slug": "bt-email",
    "title": "BT Email",
    "monitor_type": "trends_breakout",
    "statuspage_url": "",
    "geo": "GB",
    "keywords": [
      "bt email down",
      "is bt email down",
      "bt mail",
      "bt email login",
      "bt email"
    ],
    "domain": "bt.com"
  },
  {
    "id": 2756,
    "slug": "bricklink",
    "title": "BrickLink",
    "monitor_type": "trends_breakout",
    "statuspage_url": "",
    "geo": "US",
    "keywords": [
      "bricklink outage",
      "is bricklink down",
      "bricklink down",
      "bricklink not working"
    ],
    "domain": "bricklink.com"
  },
  {
    "id": 2908,
    "slug": "utilita",
    "title": "Utilita Energy",
    "category": "utilities",
    "monitor_type": "trends_breakout",
    "statuspage_url": "",
    "geo": "GB",
    "keywords": [
      "is utilita down",
      "is utilita app down",
      "utilita down",
      "utilita energy down",
      "utilita app outage",
      "my utilita down"
    ],
    "domain": "utilita.co.uk"
  },
  {
    "id": 2909,
    "slug": "canada-post",
    "title": "Canada Post",
    "category": "ecommerce-logistics",
    "monitor_type": "trends_breakout",
    "statuspage_url": "",
    "geo": "CA",
    "keywords": [
      "canada post technical disruption outage",
      "canada post down",
      "is canada post down",
      "canada post outage",
      "canada post tracking not working"
    ],
    "domain": "canadapost-postescanada.ca"
  },
  {
    "id": 2991,
    "slug": "tds",
    "title": "TDS Telecom",
    "monitor_type": "trends_breakout",
    "statuspage_url": "",
    "geo": "US",
    "keywords": [
      "tds down",
      "tds outage",
      "tds telecom outage",
      "is tds down",
      "tds internet down",
      "tds fiber outage"
    ],
    "subreddit": "tdstelecom"
  }
];

async function jitter(minMs = 200, maxMs = 500) {
  const ms = Math.floor(minMs + Math.random() * (maxMs - minMs));
  return new Promise(r => setTimeout(r, ms));
}

async function fetchTargets() {
  if (!CONFIG.endpoint || !CONFIG.secret) {
    return FALLBACK_SEED_TARGETS;
  }
  const url = `${CONFIG.endpoint}targets?partition=${CONFIG.partition}&total_partitions=${CONFIG.totalPartitions}`;
  try {
    const res = await fetch(url, {
      headers: {
        "X-DownRadar-Secret": CONFIG.secret
      },
      signal: AbortSignal.timeout(15000)
    });
    if (res.ok) {
      const data = await res.json();
      if (Array.isArray(data.targets)) {
        if (data.targets.length > 0) {
          console.log(`[Collector] [${CONFIG.runnerKey}] Retrieved ${data.targets.length} targets for shard ${CONFIG.partition}.`);
          return data.targets;
        } else {
          console.log(`[Collector] [${CONFIG.runnerKey}] Partition ${CONFIG.partition} returned 0 targets authoritatively.`);
          return [];
        }
      }
    }
  } catch (e) {
    console.warn(`[Collector] Could not fetch targets from API (${e.message}), using fallback targets.`);
  }

  const sliceSize = Math.ceil(FALLBACK_SEED_TARGETS.length / CONFIG.totalPartitions);
  const start = (CONFIG.partition - 1) * sliceSize;
  return FALLBACK_SEED_TARGETS.slice(start, start + sliceSize);
}

/**
 * High-speed Global Regional Trends RSS (<300ms)
 * Across 5 primary geos: US, GB, CA, AU, IN
 */
async function fetchGlobalTrendsRss() {
  const geos = ['US', 'GB', 'CA', 'AU', 'IN'];
  const matches = [];
  let okCount = 0;
  let rateLimited = false;

  await Promise.all(geos.map(async (geo) => {
    try {
      const res = await fetch(`https://trends.google.com/trending/rss?geo=${geo}`, { signal: AbortSignal.timeout(4000) });
      if (res.status === 429) {
        rateLimited = true;
      }
      if (res.ok) {
        okCount++;
        const xml = await res.text();
        const itemRegex = /<item>([\s\S]*?)<\/item>/g;
        let m;
        while ((m = itemRegex.exec(xml)) !== null) {
          const item = m[1];
          const titleM = /<title>(.*?)<\/title>/i.exec(item);
          const trafficM = /<ht:approx_traffic>(.*?)<\/ht:approx_traffic>/i.exec(item);
          if (titleM) {
            matches.push({
              title: titleM[1].toLowerCase(),
              traffic: trafficM ? trafficM[1] : 'Breakout 🚀',
              geo
            });
          }
        }
      }
    } catch (_) {}
  }));

  const health = okCount > 0 ? "ok" : (rateLimited ? "rate_limited" : "network_error");
  return { matches, health };
}

/**
 * In-Session Google Trends Explore Engine (Worldwide & Target-Specific)
 * Batches target keywords into a single comparison probe for maximum speed and rich Top/Rising telemetry
 */
async function probeGoogleTrendsExplore(browser, target, timeWindow = "now 1-H") {
  const keywords = (target.keywords && target.keywords.length > 0) ? target.keywords : [target.slug];
  // Default to Worldwide (empty geo: "") or target-specific geo
  const geo = target.geo || "";
  const time = timeWindow || CONFIG.timeWindow;

  let peakScore = 0;
  const breakoutQueries = [];
  const topQueries = [];
  const confirmedErrors = [];

  const context = await browser.newContext({
    userAgent: CONFIG.userAgent,
    locale: "en-US"
  });
  const page = await context.newPage();

  try {
    const exploreBaseUrl = geo
      ? `https://trends.google.com/trends/explore?geo=${encodeURIComponent(geo)}&hl=en-US&legacy`
      : `https://trends.google.com/trends/explore?hl=en-US&legacy`;

    await page.goto(exploreBaseUrl, {
      waitUntil: "domcontentloaded",
      timeout: 20000
    });

    try {
      const consentBtn = page.locator('button:has-text("Accept all"), button:has-text("Reject all")').first();
      if (await consentBtn.isVisible({ timeout: 2000 })) {
        await consentBtn.click();
        await page.waitForTimeout(400);
      }
    } catch (_) {}

    // Batch candidate keywords together (up to 5 keywords in 1 single compare call)
    const chunk = keywords.slice(0, 5);
    try {
      const result = await page.evaluate(async ({ chunk, geo, time }) => {
        const stripHeaders = (t) => t.replace(/^\)\]\}'[^\n]*\n/, '');

        const exploreReq = {
          comparisonItem: chunk.map(k => ({ keyword: k, geo: geo || '', time: time || 'now 1-H' })),
          category: 0,
          property: ''
        };

        const exploreRes = await fetch(
          `/trends/api/explore?hl=en-US&tz=-330&req=${encodeURIComponent(JSON.stringify(exploreReq))}`,
          { credentials: 'include' }
        );
        if (!exploreRes.ok) return { itemsByKeyword: {} };

        const exploreData = JSON.parse(stripHeaders(await exploreRes.text()));
        const itemsByKeyword = {};

        for (let idx = 0; idx < chunk.length; idx++) {
          const kw = chunk[idx];
          const widget = exploreData.widgets?.find((w) => w.id === `RELATED_QUERIES_${idx}` || (chunk.length === 1 && w.id === 'RELATED_QUERIES'));
          if (!widget) continue;

          try {
            const widgetRes = await fetch(
              `/trends/api/widgetdata/relatedsearches?hl=en-US&tz=-330&req=${encodeURIComponent(
                JSON.stringify(widget.request)
              )}&token=${encodeURIComponent(widget.token)}`,
              { credentials: 'include' }
            );
            if (widgetRes.ok) {
              const widgetData = JSON.parse(stripHeaders(await widgetRes.text()));
              itemsByKeyword[kw] = {
                top: widgetData.default?.rankedList?.[0]?.rankedKeyword || [],
                rising: widgetData.default?.rankedList?.[1]?.rankedKeyword || []
              };
            }
          } catch (_) {}
        }
        return { itemsByKeyword };
      }, { chunk, geo, time });

      for (const [kw, lists] of Object.entries(result.itemsByKeyword || {})) {
        // 1. Process Rising queries (breakouts)
        for (const item of (lists.rising || [])) {
          const query = (item.query || '').trim();
          if (!query || BENIGN_PATTERNS.test(query)) continue;

          const isSpike = item.formattedValue === 'Breakout' || (typeof item.value === 'number' && item.value >= 1000);

          // Extract confirmed error codes for class-error-library.php
          const errMatch = query.match(/\b(error\s?\d{3,5}|HTTP\s?\d{3}|code\s?\d{3,5}|ERR_[A-Z_]+|websocket_\w+|\d{4}[A-Z]?)\b/i);
          if (errMatch) {
            confirmedErrors.push(errMatch[0]);
          }

          if (isSpike && OUTAGE_PATTERNS.test(query)) {
            peakScore = Math.max(peakScore, 85);
            breakoutQueries.push({ query, traffic: item.formattedValue || 'Breakout 🚀', isBreakout: true, type: 'rising', keyword: kw });
          } else if (OUTAGE_PATTERNS.test(query) && typeof item.value === 'number' && item.value >= 200) {
            peakScore = Math.max(peakScore, 65);
            breakoutQueries.push({ query, traffic: `+${item.value}%`, isBreakout: false, type: 'rising', keyword: kw });
          }
        }

        // 2. Process Top queries
        for (const item of (lists.top || [])) {
          const query = (item.query || '').trim();
          if (!query || BENIGN_PATTERNS.test(query)) continue;

          const errMatch = query.match(/\b(error\s?\d{3,5}|HTTP\s?\d{3}|code\s?\d{3,5}|ERR_[A-Z_]+|websocket_\w+|\d{4}[A-Z]?)\b/i);
          if (errMatch) {
            confirmedErrors.push(errMatch[0]);
          }

          topQueries.push({
            query,
            value: item.value || 0,
            formattedValue: item.formattedValue || String(item.value || 0),
            type: 'top',
            keyword: kw
          });
        }
      }
    } catch (_) {}
  } catch (err) {
    // Session notice
  } finally {
    await context.close().catch(() => {});
  }

  // Deduplicate breakoutQueries
  const uniqueBreakouts = [];
  const seenQueries = new Set();
  for (const b of breakoutQueries) {
    const norm = b.query.toLowerCase().trim();
    if (!seenQueries.has(norm)) {
      seenQueries.add(norm);
      uniqueBreakouts.push(b);
    }
  }

  // Deduplicate topQueries
  const uniqueTopQueries = [];
  const seenTop = new Set();
  for (const t of topQueries) {
    const norm = t.query.toLowerCase().trim();
    if (!seenTop.has(norm)) {
      seenTop.add(norm);
      uniqueTopQueries.push(t);
    }
  }

  // Multi-Keyword Consensus Rule:
  // A single search query is ambient or isolated volume; it can NEVER trigger an outage score (>=25).
  // Elevated outage score (>=65) strictly requires >= 2 distinct concurrent breakout/rising queries.
  if (uniqueBreakouts.length === 1) {
    peakScore = Math.min(peakScore, 20); // Cap at 20 (Nominal Baseline)
  } else if (uniqueBreakouts.length >= 2) {
    peakScore = Math.max(peakScore, 75);
  } else {
    peakScore = 0;
  }

  return {
    score: peakScore,
    breakoutQueries: uniqueBreakouts,
    topQueries: uniqueTopQueries,
    confirmedErrors: [...new Set(confirmedErrors)]
  };
}

/**
 * Scan Broad Failure & News Query Vectors Worldwide
 * Combines 8 failure vectors in high-speed batches (Modern 8-term Explore UI + In-Session API fallback)
 */
async function scanBroadWorldwideVectors(browser) {
  const discoveredSurges = [];
  const context = await browser.newContext({
    userAgent: CONFIG.userAgent,
    locale: "en-US",
    viewport: { width: 1440, height: 900 }
  });
  const page = await context.newPage();

  try {
    // 1. Attempt Modern Google Trends 8-Term Batched Explore
    const encodedQ = BROAD_FAILURE_VECTORS
      .map(t => encodeURIComponent(t).replace(/%20/g, "%2520"))
      .join("%2C");
    const modernUrl = `https://trends.google.com/explore?q=${encodedQ}&date=now%201-h`;

    await page.goto(modernUrl, { waitUntil: "domcontentloaded", timeout: 15000 }).catch(() => {});

    try {
      const consentBtn = page.locator('button:has-text("Accept all"), button:has-text("Reject all"), button:has-text("Got it"), button:has-text("OK, got it")').first();
      if (await consentBtn.isVisible({ timeout: 1500 })) {
        await consentBtn.click();
      }
    } catch (_) {}

    await page.waitForTimeout(2000);

    let modernSuccess = false;
    const hasPills = await page.evaluate(() => {
      return document.querySelectorAll('button.dc3dBf, button[data-index]').length >= 4;
    });

    if (hasPills) {
      await page.evaluate(() => {
        const el = document.querySelector('.Jh24Ne') || document.documentElement;
        el.scrollTop = el.scrollHeight;
      });
      await page.waitForTimeout(1000);

      for (const term of BROAD_FAILURE_VECTORS) {
        try {
          const btn = page.locator(`button[aria-label="${term}"]`).first();
          if (await btn.count() > 0) {
            await btn.click({ force: true });
            await page.waitForTimeout(600);

            const scraped = await page.evaluate(() => {
              const tables = document.querySelectorAll('table');
              const extract = (tbl) => {
                if (!tbl) return [];
                return Array.from(tbl.querySelectorAll('tbody tr, tr'))
                  .map(tr => Array.from(tr.querySelectorAll('td, th')).map(c => c.innerText.trim()).filter(Boolean))
                  .filter(r => r.length >= 2 && !r[0].includes('Query'));
              };
              return {
                top: extract(tables[0]),
                rising: extract(tables[1])
              };
            });

            for (const row of (scraped.rising || [])) {
              const q = (row[1] || '').trim();
              const traffic = row[2] || 'Breakout 🚀';
              if (q && !BENIGN_PATTERNS.test(q) && STRICT_DISCOVERY_OUTAGE_PATTERNS.test(q)) {
                discoveredSurges.push({ title: q, traffic, geo: 'GLOBAL', type: 'rising', seed: term });
              }
            }

            for (const row of (scraped.top || [])) {
              const q = (row[1] || '').trim();
              const traffic = row[2] || '+50%';
              if (q && !BENIGN_PATTERNS.test(q) && STRICT_DISCOVERY_OUTAGE_PATTERNS.test(q)) {
                discoveredSurges.push({ title: q, traffic, geo: 'GLOBAL', type: 'top', seed: term });
              }
            }

            if (scraped.rising?.length > 0 || scraped.top?.length > 0) {
              modernSuccess = true;
            }
          }
        } catch (_) {}
      }
    }

    // 2. In-Session Batched API Fallback (runs if modern UI didn't produce queries)
    if (!modernSuccess || discoveredSurges.length === 0) {
      await page.goto("https://trends.google.com/trends/explore?hl=en-US&legacy", {
        waitUntil: "domcontentloaded",
        timeout: 15000
      }).catch(() => {});

      const chunks = [
        BROAD_FAILURE_VECTORS.slice(0, 5),
        BROAD_FAILURE_VECTORS.slice(5, 8)
      ];

      for (const chunk of chunks) {
        if (chunk.length === 0) continue;
        try {
          const result = await page.evaluate(async ({ chunk }) => {
            const stripHeaders = (t) => t.replace(/^\)\]\}'[^\n]*\n/, '');
            const exploreReq = {
              comparisonItem: chunk.map(v => ({ keyword: v, geo: '', time: 'now 1-H' })),
              category: 0,
              property: ''
            };
            const res = await fetch(`/trends/api/explore?hl=en-US&tz=-330&req=${encodeURIComponent(JSON.stringify(exploreReq))}`, { credentials: 'include' });
            if (!res.ok) return { itemsByKeyword: {} };
            const data = JSON.parse(stripHeaders(await res.text()));
            const itemsByKeyword = {};

            for (let i = 0; i < chunk.length; i++) {
              const kw = chunk[i];
              const widget = data.widgets?.find(w => w.id === `RELATED_QUERIES_${i}` || (chunk.length === 1 && w.id === 'RELATED_QUERIES'));
              if (!widget) continue;
              try {
                const wRes = await fetch(`/trends/api/widgetdata/relatedsearches?hl=en-US&tz=-330&req=${encodeURIComponent(JSON.stringify(widget.request))}&token=${encodeURIComponent(widget.token)}`, { credentials: 'include' });
                if (wRes.ok) {
                  const wData = JSON.parse(stripHeaders(await wRes.text()));
                  itemsByKeyword[kw] = {
                    top: wData.default?.rankedList?.[0]?.rankedKeyword || [],
                    rising: wData.default?.rankedList?.[1]?.rankedKeyword || []
                  };
                }
              } catch (_) {}
            }
            return { itemsByKeyword };
          }, { chunk });

          for (const [seedKw, lists] of Object.entries(result.itemsByKeyword || {})) {
            for (const it of (lists.rising || [])) {
              const q = (it.query || '').trim();
              if (!q || BENIGN_PATTERNS.test(q)) continue;
              if (STRICT_DISCOVERY_OUTAGE_PATTERNS.test(q)) {
                discoveredSurges.push({
                  title: q,
                  traffic: it.formattedValue || 'Breakout 🚀',
                  geo: 'GLOBAL',
                  type: 'rising',
                  seed: seedKw
                });
              }
            }
            for (const it of (lists.top || [])) {
              const q = (it.query || '').trim();
              if (!q || BENIGN_PATTERNS.test(q)) continue;
              if (STRICT_DISCOVERY_OUTAGE_PATTERNS.test(q)) {
                discoveredSurges.push({
                  title: q,
                  traffic: it.formattedValue || '+50%',
                  geo: 'GLOBAL',
                  type: 'top',
                  seed: seedKw
                });
              }
            }
          }
          await page.waitForTimeout(300);
        } catch (_) {}
      }
    }
  } catch (_) {
  } finally {
    await context.close().catch(() => {});
  }

  const uniqueSurges = [];
  const seenSurges = new Set();
  for (const s of discoveredSurges) {
    const k = s.title.toLowerCase().trim();
    if (!seenSurges.has(k)) {
      seenSurges.add(k);
      uniqueSurges.push(s);
    }
  }
  return uniqueSurges;
}

async function evaluateStatuspage(url) {
  if (!url) return { score: 0, health: "not_configured" };
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(4000) });
    if (res.ok) {
      const json = await res.json();
      const indicator = json.status?.indicator || "none";
      let score = 0;
      if (indicator === "critical") score = 100;
      else if (indicator === "major") score = 80;
      else if (indicator === "minor") score = 35;
      return { score, health: "ok", indicator };
    }
    return { score: 0, health: `http_${res.status}` };
  } catch (e) {
    const isTimeout = e.name === "TimeoutError" || (e.message && e.message.includes("timeout"));
    return { score: 0, health: isTimeout ? "timeout" : "network_error" };
  }
}

/**
 * Fast HTTP Downdetector Probe (<150ms)
 */
async function evaluateDowndetector(slug) {
  const tld = "com";
  const url = `https://downdetector.${tld}/status/${slug}/`;
  let lastError = null;

  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9"
      },
      signal: AbortSignal.timeout(3500)
    });

    const status = res.status;
    if (status === 403 || status === 429 || status === 503) {
      return {
        score: 0, reports: 0, baseline: 0, statusText: "", historySeries: null,
        symptoms: [], cities: [], health: `cloudflare_shielded: HTTP ${status}`, tld
      };
    }

    const html = await res.text();
    if (!html.includes("CompanyType")) {
      return {
        score: 0, reports: 0, baseline: 0, statusText: "", historySeries: null,
        symptoms: [], cities: [], health: "cloudflare_shielded: challenge_unresolved", tld
      };
    }

    const statusMatch = html.match(/\"status\":\"([a-z]+)\"/);
    const statusText = statusMatch ? statusMatch[1] : "unknown";

    const dpRegex = /\{\\"__typename\\":\\"ChartDataPointType\\",\\"timestampUtc\\":\\"([^\\"]+)\\",\\"reportsValue\\":(\d+),\\"baselineValue\\":(\d+)\}/g;
    const historySeries = [];
    let m;
    let maxReports = 0;
    let baseline = 0;

    while ((m = dpRegex.exec(html)) !== null) {
      const v = parseInt(m[2], 10);
      const b = parseInt(m[3], 10);
      historySeries.push({
        t: Math.floor(new Date(m[1]).getTime() / 1000),
        v,
        b
      });
      if (v > maxReports) maxReports = v;
      if (b > baseline) baseline = b;
    }

    const indRegex = /\{\\"__typename\\":\\"LastIndicatorType\\",\\"localizedName\\":\\"([^\\"]+)\\",\\"count\\":(\d+),\\"percentage\\":([\d.]+),\\"id\\":(\d+)\}/g;
    const symptoms = [];
    while ((m = indRegex.exec(html)) !== null) {
      symptoms.push({
        id: parseInt(m[4], 10),
        name: m[1],
        count: parseInt(m[2], 10),
        percentage: parseFloat(m[3])
      });
    }

    const cityRegex = /\{\\"__typename\\":\\"CityType\\",\\"id\\":\\"(\d+)\\\",\\"slug\\":\\"([^\\"]+)\\",\\"name\\":\\"([^\\"]+)\\"\}/g;
    const cities = [];
    while ((m = cityRegex.exec(html)) !== null) {
      cities.push({
        id: m[1],
        slug: m[2],
        name: m[3]
      });
    }

    if (historySeries.length > 0) {
      const recentSlice = historySeries.slice(-4);
      const recentPeak = Math.max(...recentSlice.map(p => p.v));
      const latestPoint = historySeries[historySeries.length - 1];
      const latestBaseline = latestPoint.b || 0;
      const baseForRatio = Math.max(1, latestBaseline || baseline);
      const peakRatio = recentPeak / baseForRatio;

      let score = 0;
      const stLower = (statusText || "").toLowerCase();
      if (stLower === "danger") {
        score = 85;
      } else if (stLower === "warning") {
        score = 70;
      } else if (stLower === "success") {
        if (recentPeak >= 300 && peakRatio >= 5.0) {
          score = 75;
        } else {
          score = 0;
        }
      } else {
        if (recentPeak >= 100 && peakRatio >= 4.0) score = 80;
        else if (recentPeak >= 50 && peakRatio >= 3.0) score = 65;
        else if (recentPeak >= 30 && peakRatio >= 2.5) score = 40;
        else score = 0;
      }

      return {
        score,
        reports: score > 0 ? recentPeak : (latestPoint.v || 0),
        baseline: latestBaseline || baseline,
        statusText: statusText,
        historySeries: historySeries,
        symptoms: symptoms,
        cities: cities,
        health: "ok",
        tld
      };
    }
  } catch (e) {
    lastError = e.message;
  }

  const isTimeout = lastError && (lastError.includes("timeout") || lastError.includes("Timeout"));
  return {
    score: 0, reports: 0, baseline: 0, statusText: "", historySeries: null,
    symptoms: [], cities: [], health: `failed: ${isTimeout ? "timeout" : (lastError || "network_error")}`, tld
  };
}

/**
 * Match against real-time global RSS surges with Multi-Keyword Consensus
 */
function evaluateTrendMatch(target, activeSurges, trendsHealth = "ok") {
  if (!activeSurges || activeSurges.length === 0) {
    return { score: 0, breakouts: "", health: trendsHealth || "empty" };
  }

  const slugClean = target.slug.replace(/-/g, " ").trim().toLowerCase();
  const rawTitle = (target.title || "").toLowerCase().replace(/\s*\(.*?\)/g, "").trim();
  
  const candidatePhrases = new Set();
  if (slugClean.length >= 3) candidatePhrases.add(slugClean);
  if (rawTitle.length >= 3) candidatePhrases.add(rawTitle);
  
  for (const kw of (target.keywords || [])) {
    const k = kw.toLowerCase().trim();
    if (k.length >= 4) {
      candidatePhrases.add(k);
    }
  }

  const matchedSurges = [];

  for (const s of activeSurges) {
    const rssTitle = s.title.toLowerCase().trim();
    if (BENIGN_PATTERNS.test(rssTitle)) continue;

    let matchedPhrase = null;
    for (const phrase of candidatePhrases) {
      const escaped = phrase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/\s+/g, "\\s+");
      const regex = new RegExp(`\\b${escaped}\\b`, "i");
      if (regex.test(rssTitle)) {
        matchedPhrase = phrase;
        break;
      }
    }

    if (!matchedPhrase) continue;

    const hasOutageIntent = OUTAGE_PATTERNS.test(rssTitle);
    if (hasOutageIntent) {
      matchedSurges.push(s);
    }
  }

  let bestScore = 0;
  let bestMatch = null;

  // Multi-Keyword Consensus Rule:
  // Elevated outage score (>=65) requires >= 2 distinct concurrent surges, OR a single confirmed BREAKOUT / massive spike (>= 50K searches)
  if (matchedSurges.length >= 2) {
    bestScore = 80;
    bestMatch = matchedSurges[0];
  } else if (matchedSurges.length === 1) {
    const s = matchedSurges[0];
    const trafficStr = String(s.traffic || "");
    const isBreakout = /breakout|🚀/i.test(trafficStr);
    const trafficNum = parseInt(trafficStr.replace(/[^0-9]/g, ""), 10) || 0;
    // Confirmed Breakout (>5000% velocity) or massive nationwide trending story (>= 50K searches) triggers outage score
    if (isBreakout || trafficNum >= 50000) {
      bestScore = 80;
      bestMatch = s;
    } else if (trafficNum >= 5000) {
      bestScore = 65;
      bestMatch = s;
    } else {
      bestScore = 20; // 1 single modest search surge capped at 20 (Nominal Baseline)
      bestMatch = s;
    }
  }

  return {
    score: bestScore,
    breakouts: (bestMatch && bestScore >= 25) ? `Search Surge: ${bestMatch.title} (${bestMatch.traffic} queries)` : "",
    health: "ok",
    matchCount: matchedSurges.length
  };
}

async function evaluateNewsSignal(target) {
  const cleanTitle = (target.title || "").replace(/\s*\(.*?\)/g, "").trim().toLowerCase();
  const cleanSlug = target.slug.replace(/-/g, " ").trim().toLowerCase();
  if (!cleanTitle || cleanTitle.length < 3) return { score: 0, headline: "", ageMin: 0, health: "not_configured" };

  // Explicit query targeting actual downtime/outage, excluding generic "not working"
  const query = `"${cleanTitle}" (outage OR "servers down" OR "service down" OR "widespread outage") when:3h`;
  const url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=en-US&gl=US&ceid=US:en`;

  try {
    const res = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" },
      signal: AbortSignal.timeout(3500)
    });
    if (res.status === 429) return { score: 0, headline: "", ageMin: 0, health: "rate_limited" };
    if (!res.ok) return { score: 0, headline: "", ageMin: 0, health: `http_${res.status}` };

    const xml = await res.text();
    const itemRegex = /<item>[\s\S]*?<title>(.*?)<\/title>[\s\S]*?<pubDate>(.*?)<\/pubDate>[\s\S]*?<\/item>/g;
    
    // Strict pattern: Must denote widespread system/platform outage, NOT tutorial, guide, or single-user trouble
    const STRICT_OUTAGE_PATTERNS = /\b(is down|went down|servers down|server down|service down|network down|major outage|widespread outage|blackout|downtime|unresponsive)\b/i;
    const TUTORIAL_OR_GUIDE_PATTERNS = /\b(how to|fix|fixes|why is|guide|tips|vpn not working|best vpn|troubleshoot|tutorial|reasons|workaround)\b/i;

    let m;
    while ((m = itemRegex.exec(xml)) !== null) {
      const headline = m[1].replace(/ - [^-]+$/, "").trim();
      const publishedMs = Date.parse(m[2]);
      if (!Number.isFinite(publishedMs)) continue;

      const ageMin = Math.round((Date.now() - publishedMs) / 60000);
      if (ageMin < 0 || ageMin > 180) continue;

      const headlineLower = headline.toLowerCase();
      const targetMatches = (cleanTitle.length >= 3 && headlineLower.includes(cleanTitle)) ||
                            (cleanSlug.length >= 3 && headlineLower.includes(cleanSlug));
      if (!targetMatches) continue;

      if (BENIGN_PATTERNS.test(headline)) continue;
      if (TUTORIAL_OR_GUIDE_PATTERNS.test(headline)) continue;

      const isOutage = STRICT_OUTAGE_PATTERNS.test(headline);

      if (isOutage) {
        const isCritical = /\b(major outage|widespread|massive|servers down|server down|offline)\b/i.test(headline);
        return { score: isCritical ? 85 : 75, headline, ageMin, health: "media_confirmed" };
      }
    }
    return { score: 0, headline: "", ageMin: 0, health: "ok" };
  } catch (e) {
    const isTimeout = e.name === "TimeoutError" || (e.message && e.message.includes("timeout"));
    return { score: 0, headline: "", ageMin: 0, health: isTimeout ? "timeout" : "network_error" };
  }
}

function synthesizeSymptomsAndCities(targetSlug, score) {
  if (score < 35) return { symptoms: [], cities: [] };
  
  const isGaming = /\b(warcraft|battle-net|steam|playstation|xbox|roblox|fortnite|minecraft|destiny|valorant|riot|league|apex|ea|ubisoft|nintendo|epic|cod|overwatch|dota|cs2|counter-strike)\b/i.test(targetSlug);
  const isIsp = /\b(broadband|fiber|telecom|wireless|comcast|xfinity|verizon|att|spectrum|t-mobile|bt-|virgin|sky-|jio|airtel)\b/i.test(targetSlug);
  
  let symptoms = [];
  if (isGaming) {
    symptoms = [
      { id: 1, name: "Server Connection", count: Math.round(score * 4.2), percentage: 56.0 },
      { id: 2, name: "Login & Authentication", count: Math.round(score * 2.2), percentage: 29.0 },
      { id: 3, name: "Gameplay / Latency", count: Math.round(score * 1.1), percentage: 15.0 }
    ];
  } else if (isIsp) {
    symptoms = [
      { id: 1, name: "Total Internet Blackout", count: Math.round(score * 4.8), percentage: 64.0 },
      { id: 2, name: "Wi-Fi & Router Sync", count: Math.round(score * 1.8), percentage: 24.0 },
      { id: 3, name: "Fixed Line / Phone", count: Math.round(score * 0.9), percentage: 12.0 }
    ];
  } else {
    symptoms = [
      { id: 1, name: "Website & App Access", count: Math.round(score * 3.8), percentage: 51.0 },
      { id: 2, name: "Server Connection", count: Math.round(score * 2.5), percentage: 33.0 },
      { id: 3, name: "Login & Sessions", count: Math.round(score * 1.2), percentage: 16.0 }
    ];
  }

  const cities = [
    { id: "1", slug: "new-york", name: "New York" },
    { id: "2", slug: "los-angeles", name: "Los Angeles" },
    { id: "3", slug: "chicago", name: "Chicago" },
    { id: "4", slug: "dallas", name: "Dallas" },
    { id: "5", slug: "london", name: "London" }
  ];

  return { symptoms, cities };
}

function generateSyntheticHistory(targetSlug, peakReports, currentScore) {
  const now = Math.floor(Date.now() / 1000);
  const start = Math.floor((now - 86400) / 900) * 900;
  const series = [];
  const baseVal = Math.max(1, Math.round(peakReports * 0.015));

  for (let i = 0; i < 96; i++) {
    const t = start + (i * 900);
    const hoursAgo = (now - t) / 3600;
    let b = baseVal;
    let v = Math.max(0, Math.round(b + Math.sin(i / 5) * (b * 0.5)));

    if (hoursAgo >= 0 && hoursAgo <= 2.5) {
      if (hoursAgo <= 0.75) {
        v = Math.round(peakReports * (0.8 + Math.random() * 0.2));
      } else if (hoursAgo <= 1.5) {
        v = Math.round(peakReports * (0.5 + Math.random() * 0.3));
      } else {
        v = Math.round(peakReports * (0.2 + Math.random() * 0.2));
      }
    }
    series.push({ t, v, b });
  }
  return series;
}

async function transmitPayload(records, activeSurges = []) {
  const payload = {
    pipeline_provenance: {
      runner: CONFIG.runnerKey,
      runner_key: CONFIG.runnerKey,
      partition: CONFIG.partition,
      total_partitions: CONFIG.totalPartitions,
      timestamp: Math.floor(Date.now() / 1000)
    },
    services: records,
    explore_trends: (activeSurges || []).map(s => ({
      query: s.title,
      traffic: s.traffic,
      geo: s.geo || "GLOBAL",
      type: s.type || "rising",
      seed_keyword: s.seed || null,
      source_channel: "playwright_explore_swarm",
      source: CONFIG.runnerKey
    }))
  };

  const highImpact = records.filter(r => (r.signals.dd >= 35 || r.signals.trends >= 35 || r.signals.official >= 35 || r.signals.probe >= 35));
  console.log(`[Collector] [${CONFIG.runnerKey}] Transmitting ${records.length} service records (${highImpact.length} active disruptions) to: ${CONFIG.endpoint}ingest`);

  let lastError = null;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(`${CONFIG.endpoint}ingest`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-DownRadar-Secret": CONFIG.secret
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(45000)
      });
      const respText = await res.text();
      console.log(`[Collector] [${CONFIG.runnerKey}] Ingestion response HTTP ${res.status}: ${respText.substring(0, 200)}`);
      if (res.ok) return;
      lastError = new Error(`Ingestion HTTP ${res.status}: ${respText.substring(0, 100)}`);
    } catch (e) {
      lastError = e;
      console.warn(`[Collector] [${CONFIG.runnerKey}] Ingestion attempt ${attempt} error: ${e.message}`);
    }
    if (attempt < 3) await jitter(2000, 4000);
  }

  throw lastError || new Error(`[Collector] [${CONFIG.runnerKey}] Ingestion failed after 3 attempts`);
}

async function run() {
  console.log(`=== ${CONFIG.runnerKey.toUpperCase()} [Partition ${CONFIG.partition}/${CONFIG.totalPartitions}] ===`);
  const targets = await fetchTargets();

  // 1. Fetch real-time global search trends across 5 geos concurrently in <300ms
  const trendsFeed = await fetchGlobalTrendsRss();
  let allActiveSurges = [...(trendsFeed.matches || [])];
  const trendsGlobalHealth = trendsFeed.health || "ok";
  console.log(`[Collector] Ingested ${allActiveSurges.length} regional search surges.`);

  // 2. Launch Headless Playwright Chromium for In-Session Google Trends Worldwide Explore
  console.log(`[Collector] Provisioning Playwright Chromium for Worldwide Trends & Error Extraction...`);
  let browser = null;
  try {
    browser = await chromium.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
    });

    // Scan broad worldwide failure vectors to discover breaking news topics
    const broadSurges = await scanBroadWorldwideVectors(browser);
    if (broadSurges.length > 0) {
      console.log(`[Collector] Discovered ${broadSurges.length} worldwide failure breakout queries.`);
      allActiveSurges.push(...broadSurges);
    }
  } catch (bErr) {
    console.warn(`[Collector] Playwright initialization warning: ${bErr.message}`);
  }

  if (targets.length === 0) {
    console.log(`[Collector] [${CONFIG.runnerKey}] 0 targets assigned to partition ${CONFIG.partition}. Completed.`);
    if (allActiveSurges.length > 0) {
      await transmitPayload([], allActiveSurges);
    }
    if (browser) await browser.close().catch(() => {});
    return;
  }

  console.log(`[Collector] Auditing ${targets.length} targets across Multi-Vector Telemetry Matrix...`);
  const t0 = Date.now();
  const records = [];

  for (const target of targets) {
    // A. Official Statuspage REST (Instant)
    const statusRes = target.statuspage_url ? await evaluateStatuspage(target.statuspage_url) : { score: 0, health: "not_configured" };
    const statusScore = statusRes.score;

    // B. Fast HTTP Downdetector Probe (<100ms)
    const ddSlug = target.downdetector_slug || target.dd_slug || (target.slug === 'chatgpt' ? 'openai' : target.slug);
    const dd = await evaluateDowndetector(ddSlug);

    // C. Search Trends Evaluation from Global RSS
    let trends = evaluateTrendMatch(target, allActiveSurges, trendsGlobalHealth);

    // D. In-Session Deep Google Trends Explore (Worldwide) for elevated, high-priority or sample targets
    const isHighPriority = HIGH_PRIORITY_SLUGS.has(target.slug);
    const shouldExploreDeep = browser && (isHighPriority || dd.score >= 35 || statusScore >= 35 || trends.score >= 25 || (trends.matchCount || 0) > 0);
    let deepBreakoutTerms = [];
    let deepTopQueries = [];
    let deepBreakoutQueries = [];

    if (shouldExploreDeep) {
      const exploreRes = await probeGoogleTrendsExplore(browser, target, CONFIG.timeWindow);
      if (exploreRes.score > trends.score) {
        trends.score = exploreRes.score;
      }
      deepTopQueries = exploreRes.topQueries || [];
      deepBreakoutQueries = exploreRes.breakoutQueries || [];
      if (exploreRes.breakoutQueries && exploreRes.breakoutQueries.length > 0) {
        deepBreakoutTerms = exploreRes.breakoutQueries.map(b => `${b.query} (${b.traffic})`);
        exploreRes.breakoutQueries.forEach(b => {
          allActiveSurges.push({ title: b.query, traffic: b.traffic, geo: target.geo || 'GLOBAL', type: 'rising' });
        });
      }
      if (exploreRes.topQueries && exploreRes.topQueries.length > 0) {
        exploreRes.topQueries.forEach(t => {
          allActiveSurges.push({ title: t.query, traffic: t.formattedValue || '+50%', geo: target.geo || 'GLOBAL', type: 'top' });
        });
      }
    }

    // E. Google News Media Verification: Media is strictly secondary corroboration!
    // Never allow an uncorroborated news blog post to trigger an outage score without telemetry disruption.
    let probeScore = 0;
    let probeHealth = "ok";

    const hasTelemetryDisruption = (trends.score >= 35 || statusScore >= 35 || dd.score >= 35);
    if (hasTelemetryDisruption) {
      const news = await evaluateNewsSignal(target);
      if (news.score >= 65) {
        probeScore = news.score;
        probeHealth = news.health;
      }
    }

    const displayScore = Math.max(dd.score, trends.score, statusScore, probeScore);

    // Normalizing reports count
    const calculatedReports = displayScore === 0
      ? 0
      : (dd.reports > 0
        ? dd.reports
        : (statusScore >= 80 ? 450
          : (displayScore >= 80 ? 550
            : (displayScore >= 65 ? 320
              : (displayScore >= 35 ? 120 : 0)))));

    const reportSource = displayScore === 0
      ? "nominal_baseline"
      : (dd.reports > 0
        ? "downdetector_rsc"
        : (displayScore >= 35 ? "estimated_from_search_velocity" : "nominal_baseline"));

    const historySeries = dd.historySeries || (displayScore >= 35 && calculatedReports >= 50
      ? generateSyntheticHistory(target.slug, calculatedReports, displayScore)
      : null);

    const fallbackTelemetry = (dd.symptoms && dd.symptoms.length > 0)
      ? { symptoms: dd.symptoms, cities: dd.cities }
      : synthesizeSymptomsAndCities(target.slug, displayScore);

    // CRITICAL: breakout_terms must ONLY contain actual search trends queries!
    // NEVER append news headlines, alerts, or media timestamps into search breakouts.
    const allBreakouts = [
      trends.breakouts,
      deepBreakoutTerms.join(", ")
    ].filter(Boolean).join(" | ");

    console.log(`  ✓ ${target.title} (${target.slug}): DD=${dd.score} (rep:${dd.reports}), Trends=${trends.score}, Official=${statusScore}, Probe=${probeScore} -> Score: ${displayScore} (rep:${calculatedReports})`);

    records.push({
      slug: target.slug,
      title: target.title,
      signals: {
        dd: dd.score,
        trends: trends.score,
        official: statusScore,
        probe: probeScore
      },
      fetch_health: {
        dd: dd.health,
        trends: trends.health || trendsGlobalHealth,
        official: statusRes.health,
        probe: probeHealth
      },
      report_source: reportSource,
      breakout_terms: allBreakouts || null,
      top_queries: deepTopQueries,
      breakout_queries: deepBreakoutQueries,
      report_count: calculatedReports,
      baseline: dd.baseline || 0,
      history_series: historySeries,
      symptoms: fallbackTelemetry.symptoms,
      cities: fallbackTelemetry.cities
    });
  }

  if (browser) {
    await browser.close().catch(() => {});
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`[Collector] [${CONFIG.runnerKey}] Audited ${records.length} targets in ${elapsed}s!`);

  if (records.length > 0) {
    await transmitPayload(records, allActiveSurges);
  }
}

run().catch((e) => {
  console.error("[Fatal]", e);
  process.exit(1);
});
