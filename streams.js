/**
 * M7 SOVEREIGN — REAL STREAM CONNECTORS
 * streams.js
 *
 * All 7 domains. Zero API keys. 100% public endpoints.
 * 20 live connectors. Real data. Real events.
 *
 * Domains & Sources:
 *   FINANCE     → CoinGecko, ECB FX, SEC EDGAR RSS
 *   INFORMATION → HackerNews Firebase, Wikipedia Recent Changes, BBC RSS
 *   TECHNOLOGY  → GitHub Public Events, npm Registry, RIPE NCC BGP
 *   AI          → HuggingFace Trending Models, HuggingFace Datasets, ArXiv cs.AI
 *   HEALTH      → OpenFDA Adverse Events, PubMed E-utilities, WHO RSS
 *   ENERGY      → Open-Meteo Renewables, EIA RSS, Open-Meteo UV Index
 *   GOVERNANCE  → Federal Register API, UN News RSS, EUR-Lex RSS
 */

'use strict';

const https        = require('https');
const http         = require('http');
const EventEmitter = require('events');
const url          = require('url');

// ─────────────────────────────────────────────────────────────
// FETCH UTILITIES  (zero dependencies — built-in Node only)
// ─────────────────────────────────────────────────────────────

function request(rawUrl, opts = {}) {
  return new Promise((resolve, reject) => {
    const parsed = url.parse(rawUrl);
    const lib    = parsed.protocol === 'https:' ? https : http;
    const req    = lib.request({
      hostname: parsed.hostname,
      port:     parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path:     parsed.path || '/',
      method:   'GET',
      timeout:  opts.timeout || 12000,
      headers: {
        'Accept':     opts.accept || 'application/json',
        'User-Agent': 'M7-Sovereign/2.0 (public-data-processor)',
        ...opts.headers,
      },
    }, res => {
      const chunks = [];
      res.on('data',  c => chunks.push(c));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        if (res.statusCode >= 400) return reject(new Error(`HTTP ${res.statusCode} ${rawUrl}`));
        resolve({ body, status: res.statusCode, headers: res.headers });
      });
    });
    req.on('error',   reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${rawUrl}`)); });
    req.end();
  });
}

async function fetchJSON(rawUrl, opts = {}) {
  const { body } = await request(rawUrl, opts);
  return JSON.parse(body);
}

async function fetchText(rawUrl, opts = {}) {
  const { body } = await request(rawUrl, { accept: 'text/xml,text/html,*/*', ...opts });
  return body;
}

/** Pull all <tag>content</tag> values from raw XML/RSS */
function xmlValues(xml, tag) {
  const out = [];
  const re  = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'gi');
  let m;
  while ((m = re.exec(xml)) !== null) {
    const v = m[1].replace(/<!\[CDATA\[|\]\]>/g, '').replace(/<[^>]+>/g, '').trim();
    if (v) out.push(v);
  }
  return out;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─────────────────────────────────────────────────────────────
// M7 EVENT SHAPE
// domain  : string   — one of 7 M7 domains
// source  : string   — connector name
// type    : string   — event classification
// data    : object   — raw normalised payload
// ts      : number   — unix ms
// weight  : number   — 0–1 signal importance (drives billing tier)
// ─────────────────────────────────────────────────────────────

function evt(domain, source, type, data, weight = 0.5) {
  return { domain, source, type, data, ts: Date.now(), weight };
}

// ─────────────────────────────────────────────────────────────
// BASE CONNECTOR
// ─────────────────────────────────────────────────────────────

class Connector extends EventEmitter {
  constructor(name, domain, pollMs) {
    super();
    this.name       = name;
    this.domain     = domain;
    this.pollMs     = pollMs;
    this.active     = false;
    this.fetches    = 0;
    this.errors     = 0;
    this.emitted    = 0;
    this.lastFetch  = null;
    this.lastError  = null;
    this._timer     = null;
  }

  start() {
    if (this.active) return;
    this.active = true;
    this._poll();
    this._timer = setInterval(() => this._poll(), this.pollMs);
  }

  stop() {
    this.active = false;
    if (this._timer) clearInterval(this._timer);
    this._timer = null;
  }

  async _poll() {
    try {
      const events = await this.fetch();
      this.fetches++;
      this.lastFetch = Date.now();
      if (!Array.isArray(events) || events.length === 0) return;
      events.forEach(e => { this.emitted++; this.emit('event', e); });
      this.emit('batch', { source: this.name, domain: this.domain, count: events.length });
    } catch (err) {
      this.errors++;
      this.lastError = err.message;
      this.emit('error', { source: this.name, domain: this.domain, error: err.message });
    }
  }

  async fetch() { return []; }   // override

  status() {
    return {
      name:      this.name,
      domain:    this.domain,
      active:    this.active,
      fetches:   this.fetches,
      errors:    this.errors,
      emitted:   this.emitted,
      lastFetch: this.lastFetch,
      lastError: this.lastError,
    };
  }
}

// ═════════════════════════════════════════════════════════════
// DOMAIN 1  ·  FINANCE
// ═════════════════════════════════════════════════════════════

class CoinGeckoConnector extends Connector {
  constructor() { super('coingecko', 'FINANCE', 60_000); }

  async fetch() {
    const coins = await fetchJSON(
      'https://api.coingecko.com/api/v3/coins/markets' +
      '?vs_currency=usd&order=market_cap_desc&per_page=25&page=1' +
      '&price_change_percentage=1h,24h,7d'
    );
    return coins.map(c => evt('FINANCE', this.name, 'CRYPTO_MARKET', {
      id:        c.id,
      symbol:    c.symbol?.toUpperCase(),
      price:     c.current_price,
      mcap:      c.market_cap,
      vol24h:    c.total_volume,
      chg1h:     c.price_change_percentage_1h_in_currency,
      chg24h:    c.price_change_percentage_24h,
      chg7d:     c.price_change_percentage_7d_in_currency,
      high24h:   c.high_24h,
      low24h:    c.low_24h,
      ath:       c.ath,
      rank:      c.market_cap_rank,
    }, Math.min(1, Math.abs(c.price_change_percentage_24h || 0) / 15)));
  }
}

class ECBFXConnector extends Connector {
  constructor() { super('ecb-fx', 'FINANCE', 3_600_000); }

  async fetch() {
    const xml   = await fetchText('https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml');
    const rates = {};
    const re    = /currency='([A-Z]+)'\s+rate='([\d.]+)'/g;
    let m;
    while ((m = re.exec(xml)) !== null) rates[m[1]] = parseFloat(m[2]);
    if (Object.keys(rates).length === 0) return [];
    return [evt('FINANCE', this.name, 'FX_DAILY_RATES', { base: 'EUR', rates, date: new Date().toISOString().slice(0, 10) }, 0.45)];
  }
}

class SECEdgarConnector extends Connector {
  constructor() { super('sec-edgar', 'FINANCE', 300_000); }

  async fetch() {
    const xml    = await fetchText(
      'https://www.sec.gov/cgi-bin/browse-edgar' +
      '?action=getcurrent&type=8-K&dateb=&owner=include&count=20&output=atom'
    );
    const titles  = xmlValues(xml, 'title').slice(1, 16);
    const updated = xmlValues(xml, 'updated').slice(0, 15);
    return titles.map((title, i) => evt('FINANCE', this.name, 'SEC_8K_FILING', {
      title,
      updated: updated[i] || null,
      type:    '8-K',
    }, 0.72));
  }
}

// ═════════════════════════════════════════════════════════════
// DOMAIN 2  ·  INFORMATION
// ═════════════════════════════════════════════════════════════

class HackerNewsConnector extends Connector {
  constructor() { super('hackernews', 'INFORMATION', 120_000); }

  async fetch() {
    const ids  = await fetchJSON('https://hacker-news.firebaseio.com/v0/topstories.json');
    const top  = ids.slice(0, 12);
    const rows = await Promise.allSettled(
      top.map(id => fetchJSON(`https://hacker-news.firebaseio.com/v0/item/${id}.json`))
    );
    return rows
      .filter(r => r.status === 'fulfilled' && r.value?.title)
      .map(r => r.value)
      .map(it => evt('INFORMATION', this.name, 'HN_STORY', {
        id:       it.id,
        title:    it.title,
        url:      it.url || null,
        score:    it.score,
        author:   it.by,
        comments: it.descendants || 0,
        time:     it.time,
      }, Math.min(1, (it.score || 0) / 400)));
  }
}

class WikipediaConnector extends Connector {
  constructor() { super('wikipedia', 'INFORMATION', 60_000); }

  async fetch() {
    const data = await fetchJSON(
      'https://en.wikipedia.org/w/api.php' +
      '?action=query&list=recentchanges' +
      '&rcprop=title|ids|sizes|flags|user|timestamp|comment' +
      '&rclimit=25&rctype=edit&format=json&rcnamespace=0'
    );
    return (data?.query?.recentchanges || []).map(c => evt('INFORMATION', this.name, 'WIKI_EDIT', {
      title:     c.title,
      pageid:    c.pageid,
      user:      c.user,
      timestamp: c.timestamp,
      delta:     (c.newlen || 0) - (c.oldlen || 0),
      minor:     !!c.minor,
      comment:   (c.comment || '').slice(0, 100),
    }, Math.min(1, Math.abs((c.newlen || 0) - (c.oldlen || 0)) / 4000)));
  }
}

class BBCNewsConnector extends Connector {
  constructor() { super('bbc-rss', 'INFORMATION', 300_000); }

  async fetch() {
    const xml    = await fetchText('https://feeds.bbci.co.uk/news/world/rss.xml');
    const titles = xmlValues(xml, 'title').slice(1, 16);
    const descs  = xmlValues(xml, 'description').slice(0, 15);
    const pubds  = xmlValues(xml, 'pubDate').slice(0, 15);
    return titles.map((title, i) => evt('INFORMATION', this.name, 'BBC_HEADLINE', {
      title,
      description: (descs[i] || '').slice(0, 180),
      pubDate:     pubds[i] || null,
      source:      'BBC World',
    }, 0.58));
  }
}

// ═════════════════════════════════════════════════════════════
// DOMAIN 3  ·  TECHNOLOGY
// ═════════════════════════════════════════════════════════════

class GitHubEventsConnector extends Connector {
  constructor() { super('github-events', 'TECHNOLOGY', 90_000); }

  async fetch() {
    const events = await fetchJSON(
      'https://api.github.com/events?per_page=30',
      { headers: { Accept: 'application/vnd.github.v3+json' } }
    );
    return events.map(e => evt('TECHNOLOGY', this.name, `GH_${(e.type || 'EVENT').toUpperCase()}`, {
      id:      e.id,
      type:    e.type,
      actor:   e.actor?.login,
      repo:    e.repo?.name,
      public:  e.public,
      created: e.created_at,
    }, e.type === 'PushEvent' ? 0.62 : e.type === 'WatchEvent' ? 0.45 : 0.38));
  }
}

class NpmConnector extends Connector {
  constructor() { super('npm-registry', 'TECHNOLOGY', 600_000); }

  async fetch() {
    const data = await fetchJSON('https://registry.npmjs.org/-/v1/search?text=is:popular&size=20');
    return (data?.objects || []).map(o => evt('TECHNOLOGY', this.name, 'NPM_PACKAGE_TREND', {
      name:        o.package?.name,
      version:     o.package?.version,
      description: (o.package?.description || '').slice(0, 120),
      keywords:    (o.package?.keywords || []).slice(0, 6),
      score:       o.score?.final,
      quality:     o.score?.detail?.quality,
      popularity:  o.score?.detail?.popularity,
    }, Math.min(1, o.score?.final || 0)));
  }
}

class RIPEBGPConnector extends Connector {
  constructor() { super('ripe-bgp', 'TECHNOLOGY', 600_000); }

  async fetch() {
    const data = await fetchJSON(
      'https://stat.ripe.net/data/ris-prefixes/data.json?resource=0.0.0.0/0&list_prefixes=false'
    );
    const counts = data?.data?.counts;
    if (!counts) return [];
    return [evt('TECHNOLOGY', this.name, 'BGP_GLOBAL_STATE', {
      ipv4Originating: counts.v4?.originating,
      ipv4Transit:     counts.v4?.transit,
      ipv6Originating: counts.v6?.originating,
      ipv6Transit:     counts.v6?.transit,
      queryTime:       data.data?.query_time,
      source:          'RIPE NCC RIS',
    }, 0.5)];
  }
}

// ═════════════════════════════════════════════════════════════
// DOMAIN 4  ·  AI
// ═════════════════════════════════════════════════════════════

class HFTrendingModelsConnector extends Connector {
  constructor() { super('hf-models', 'AI', 300_000); }

  async fetch() {
    const models = await fetchJSON(
      'https://huggingface.co/api/models?sort=trending&limit=25&direction=-1'
    );
    return models.map(m => evt('AI', this.name, 'HF_MODEL_TRENDING', {
      id:           m.modelId || m.id,
      author:       m.author,
      task:         m.pipeline_tag || 'unknown',
      downloads:    m.downloads || 0,
      likes:        m.likes || 0,
      tags:         (m.tags || []).slice(0, 6),
      lastModified: m.lastModified,
      private:      m.private || false,
    }, Math.min(1, (m.likes || 0) / 8000)));
  }
}

class HFDatasetsConnector extends Connector {
  constructor() { super('hf-datasets', 'AI', 600_000); }

  async fetch() {
    const datasets = await fetchJSON(
      'https://huggingface.co/api/datasets?sort=lastModified&limit=20&direction=-1'
    );
    return datasets.map(d => evt('AI', this.name, 'HF_DATASET_UPDATED', {
      id:           d.id || d.datasetId,
      author:       d.author,
      downloads:    d.downloads || 0,
      likes:        d.likes || 0,
      tags:         (d.tags || []).slice(0, 6),
      lastModified: d.lastModified,
    }, 0.42));
  }
}

class ArxivConnector extends Connector {
  constructor() { super('arxiv-ai', 'AI', 600_000); }

  async fetch() {
    const xml = await fetchText(
      'http://export.arxiv.org/api/query' +
      '?search_query=cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.CV' +
      '&sortBy=submittedDate&sortOrder=descending&max_results=15'
    );
    const titles    = xmlValues(xml, 'title').slice(1, 14);
    const summaries = xmlValues(xml, 'summary').slice(0, 13);
    const authors   = xmlValues(xml, 'name').slice(0, 39);

    return titles.slice(0, 10).map((title, i) => evt('AI', this.name, 'ARXIV_AI_PAPER', {
      title:   title.replace(/\s+/g, ' ').trim(),
      summary: (summaries[i] || '').replace(/\s+/g, ' ').trim().slice(0, 240),
      author:  authors[i * 3] || null,
    }, 0.68));
  }
}

// ═════════════════════════════════════════════════════════════
// DOMAIN 5  ·  HEALTH
// ═════════════════════════════════════════════════════════════

class OpenFDAConnector extends Connector {
  constructor() { super('openfda', 'HEALTH', 300_000); }

  async fetch() {
    const data = await fetchJSON(
      'https://api.fda.gov/drug/event.json?limit=10&sort=receiptdate:desc'
    );
    return (data?.results || []).map(r => evt('HEALTH', this.name, 'FDA_ADVERSE_EVENT', {
      receiptDate: r.receiptdate,
      serious:     r.serious === '1',
      hospitalised:r.seriousnesshospitalization === '1',
      country:     r.primarysource?.reportercountry,
      drugs:       (r.patient?.drug   || []).slice(0, 4).map(d => d.medicinalproduct),
      reactions:   (r.patient?.reaction || []).slice(0, 4).map(rx => rx.reactionmeddrapt),
      patientAge:  r.patient?.patientonsetage,
      patientSex:  r.patient?.patientsex,
    }, r.serious === '1' ? 0.82 : 0.42));
  }
}

class PubMedConnector extends Connector {
  constructor() { super('pubmed', 'HEALTH', 300_000); }

  async fetch() {
    const search = await fetchJSON(
      'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi' +
      '?db=pubmed&term=("clinical+trial"[pt]+OR+"meta-analysis"[pt]+OR+"systematic+review"[pt])' +
      '&sort=date&retmax=10&retmode=json'
    );
    const ids = search?.esearchresult?.idlist || [];
    if (ids.length === 0) return [];
    await sleep(400); // NCBI rate limit: 3 req/sec
    const summary = await fetchJSON(
      `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=${ids.join(',')}&retmode=json`
    );
    const result = summary?.result || {};
    return ids
      .map(id => result[id])
      .filter(Boolean)
      .map(item => evt('HEALTH', this.name, 'PUBMED_PUBLICATION', {
        pmid:    item.uid,
        title:   item.title,
        journal: item.source,
        pubDate: item.pubdate,
        authors: (item.authors || []).slice(0, 4).map(a => a.name),
        types:   item.pubtype || [],
      }, 0.64));
  }
}

class WHOConnector extends Connector {
  constructor() { super('who-rss', 'HEALTH', 600_000); }

  async fetch() {
    const xml    = await fetchText('https://www.who.int/rss-feeds/news-english.xml');
    const titles = xmlValues(xml, 'title').slice(1, 12);
    const descs  = xmlValues(xml, 'description').slice(0, 11);
    return titles.map((title, i) => evt('HEALTH', this.name, 'WHO_HEALTH_NEWS', {
      title,
      description: (descs[i] || '').slice(0, 200),
      source:      'WHO',
    }, 0.66));
  }
}

// ═════════════════════════════════════════════════════════════
// DOMAIN 6  ·  ENERGY
// ═════════════════════════════════════════════════════════════

const ENERGY_NODES = [
  { name: 'Frankfurt', lat: 50.11, lon:   8.68 },
  { name: 'London',    lat: 51.51, lon:  -0.13 },
  { name: 'New York',  lat: 40.71, lon: -74.01 },
  { name: 'Tokyo',     lat: 35.69, lon: 139.69 },
  { name: 'Dubai',     lat: 25.20, lon:  55.27 },
  { name: 'Singapore', lat:  1.29, lon: 103.85 },
];

class OpenMeteoWindSolarConnector extends Connector {
  constructor() { super('open-meteo-wind-solar', 'ENERGY', 300_000); }

  async fetch() {
    const results = await Promise.allSettled(
      ENERGY_NODES.map(node =>
        fetchJSON(
          `https://api.open-meteo.com/v1/forecast` +
          `?latitude=${node.lat}&longitude=${node.lon}` +
          `&current=temperature_2m,wind_speed_10m,wind_direction_10m,cloud_cover,relative_humidity_2m` +
          `&hourly=direct_normal_irradiance,diffuse_radiation` +
          `&forecast_days=1&timezone=UTC`
        ).then(d => ({ node, data: d }))
      )
    );
    return results
      .filter(r => r.status === 'fulfilled')
      .map(({ value: { node, data } }) => {
        const cur   = data.current || {};
        const dni   = data.hourly?.direct_normal_irradiance?.[0] ?? 0;
        const diff  = data.hourly?.diffuse_radiation?.[0] ?? 0;
        const wind  = cur.wind_speed_10m || 0;
        // Rough renewable energy proxy scores
        const windPower  = Math.min(1, wind / 25);
        const solarPower = Math.min(1, dni  / 800);
        return evt('ENERGY', this.name, 'RENEWABLE_CONDITIONS', {
          location:    node.name,
          lat:         node.lat,
          lon:         node.lon,
          tempC:       cur.temperature_2m,
          windMps:     wind,
          windDir:     cur.wind_direction_10m,
          cloudPct:    cur.cloud_cover,
          humidity:    cur.relative_humidity_2m,
          solarDNI:    dni,
          solarDiff:   diff,
          windScore:   +windPower.toFixed(3),
          solarScore:  +solarPower.toFixed(3),
          combinedScore: +((windPower + solarPower) / 2).toFixed(3),
        }, (windPower + solarPower) / 2);
      });
  }
}

class EIARSSConnector extends Connector {
  constructor() { super('eia-rss', 'ENERGY', 3_600_000); }

  async fetch() {
    const xml    = await fetchText('https://www.eia.gov/rss/todayinenergy.xml');
    const titles = xmlValues(xml, 'title').slice(1, 12);
    const descs  = xmlValues(xml, 'description').slice(0, 11);
    return titles.map((title, i) => evt('ENERGY', this.name, 'EIA_ENERGY_UPDATE', {
      title,
      description: (descs[i] || '').replace(/<[^>]+>/g, '').slice(0, 200),
      source:      'U.S. EIA',
    }, 0.52));
  }
}

class OpenMeteoUVConnector extends Connector {
  constructor() { super('open-meteo-uv', 'ENERGY', 600_000); }

  async fetch() {
    const results = await Promise.allSettled(
      ENERGY_NODES.slice(0, 4).map(node =>
        fetchJSON(
          `https://currentuvindex.com/api/v1/uvi?lat=${node.lat}&lng=${node.lon}`
        ).then(d => ({ node, data: d })).catch(() => null)
      )
    );
    return results
      .filter(r => r.status === 'fulfilled' && r.value?.data?.now?.uvi !== undefined)
      .map(({ value: { node, data } }) => evt('ENERGY', this.name, 'SOLAR_UV_INDEX', {
        location: node.name,
        uvi:      data.now.uvi,
        risk:     data.now.risk,
        lat:      node.lat,
        lon:      node.lon,
      }, Math.min(1, (data.now.uvi || 0) / 11)));
  }
}

// ═════════════════════════════════════════════════════════════
// DOMAIN 7  ·  GOVERNANCE
// ═════════════════════════════════════════════════════════════

class FederalRegisterConnector extends Connector {
  constructor() { super('federal-register', 'GOVERNANCE', 600_000); }

  async fetch() {
    const data = await fetchJSON(
      'https://www.federalregister.gov/api/v1/articles.json' +
      '?order=newest&per_page=10' +
      '&fields[]=title&fields[]=type&fields[]=agencies' +
      '&fields[]=publication_date&fields[]=document_number&fields[]=abstract'
    );
    return (data?.results || []).map(r => evt('GOVERNANCE', this.name, `US_FEDREG_${(r.type || 'NOTICE').toUpperCase().replace(/\s+/g, '_')}`, {
      title:      r.title,
      type:       r.type,
      agencies:   (r.agencies || []).map(a => a.name).slice(0, 3),
      pubDate:    r.publication_date,
      docNumber:  r.document_number,
      abstract:   (r.abstract || '').slice(0, 220),
    }, r.type === 'Rule' ? 0.85 : r.type === 'Proposed Rule' ? 0.72 : 0.50));
  }
}

class UNNewsConnector extends Connector {
  constructor() { super('un-news', 'GOVERNANCE', 600_000); }

  async fetch() {
    const xml    = await fetchText('https://news.un.org/feed/subscribe/en/news/all/rss.xml');
    const titles = xmlValues(xml, 'title').slice(1, 12);
    const descs  = xmlValues(xml, 'description').slice(0, 11);
    const dates  = xmlValues(xml, 'pubDate').slice(0, 11);
    return titles.map((title, i) => evt('GOVERNANCE', this.name, 'UN_NEWS', {
      title,
      description:  (descs[i] || '').replace(/<[^>]+>/g, '').slice(0, 200),
      pubDate:      dates[i] || null,
      source:       'United Nations',
      jurisdiction: 'GLOBAL',
    }, 0.62));
  }
}

class EURLexConnector extends Connector {
  constructor() { super('eurlex', 'GOVERNANCE', 3_600_000); }

  async fetch() {
    const xml    = await fetchText(
      'https://eur-lex.europa.eu/rss/rss.xml?locale=en&type=legislation&lang=en'
    );
    const titles = xmlValues(xml, 'title').slice(1, 12);
    const dates  = xmlValues(xml, 'pubDate').slice(0, 11);
    return titles.map((title, i) => evt('GOVERNANCE', this.name, 'EU_LEGISLATION', {
      title,
      pubDate:      dates[i] || null,
      source:       'EUR-Lex',
      jurisdiction: 'EU',
    }, 0.68));
  }
}

// ─────────────────────────────────────────────────────────────
// REGISTRY — all 20 connectors
// ─────────────────────────────────────────────────────────────

const REGISTRY = {
  FINANCE:     [ new CoinGeckoConnector(), new ECBFXConnector(),       new SECEdgarConnector()         ],
  INFORMATION: [ new HackerNewsConnector(), new WikipediaConnector(),  new BBCNewsConnector()          ],
  TECHNOLOGY:  [ new GitHubEventsConnector(), new NpmConnector(),      new RIPEBGPConnector()          ],
  AI:          [ new HFTrendingModelsConnector(), new HFDatasetsConnector(), new ArxivConnector()      ],
  HEALTH:      [ new OpenFDAConnector(), new PubMedConnector(),        new WHOConnector()              ],
  ENERGY:      [ new OpenMeteoWindSolarConnector(), new EIARSSConnector(), new OpenMeteoUVConnector()  ],
  GOVERNANCE:  [ new FederalRegisterConnector(), new UNNewsConnector(), new EURLexConnector()          ],
};

/**
 * Start all 20 connectors.
 * domainEngineMap: Map<domainName, DomainEngine>
 * onEvent:        optional callback(event)
 * Returns stopAll() function.
 */
function startAll(domainEngineMap, onEvent) {
  const all = Object.values(REGISTRY).flat();

  all.forEach(c => {
    c.on('event', ev => {
      const engine = domainEngineMap?.get(ev.domain);
      if (engine?.ingest) engine.ingest(ev);
      if (typeof onEvent === 'function') onEvent(ev);
    });
    c.on('error', e  => console.warn(`[STREAM ERR] ${e.source}: ${e.error}`));
    c.on('batch', b  => console.log( `[STREAM]     ${b.domain}:${b.source} → ${b.count} events`));
    c.start();
  });

  console.log(`[M7 STREAMS] ${all.length} connectors started across 7 domains`);
  return () => { all.forEach(c => c.stop()); console.log('[M7 STREAMS] All stopped'); };
}

function getStatus() {
  return Object.fromEntries(
    Object.entries(REGISTRY).map(([d, cs]) => [d, cs.map(c => c.status())])
  );
}

module.exports = { REGISTRY, startAll, getStatus };
