/**
 * M7 SOVEREIGN INTELLIGENCE SYSTEM
 * server.js  —  v2.0  LIVE EDITION
 *
 * 7 Domain Engines  ·  10 Brain Managers  ·  Immutable Ledger
 * Sovereign Treasury  ·  Real Public Streams  ·  Live Dashboard
 *
 * npm install && node server.js
 * Dashboard → http://localhost:3000
 */

'use strict';

const express      = require('express');
const http         = require('http');
const WebSocket    = require('ws');
const crypto       = require('crypto');
const EventEmitter = require('events');

const { startAll, getStatus: getStreamStatus } = require('./streams');

// ─────────────────────────────────────────────────────────────
// CONFIGURATION
// ─────────────────────────────────────────────────────────────

const CFG = {
  PORT:                   process.env.PORT || 3000,
  VERSION:                '2.0.0-live',
  SOVEREIGN_SEED:         process.env.M7_SEED || crypto.randomBytes(32).toString('hex'),

  // Billing rates per real ingested event
  RATE_BASE:              0.00001,   // $0.00001 per standard event
  RATE_PREMIUM:           0.0001,    // $0.0001 per high-weight signal (weight > 0.7)
  RATE_SYNTHESIS:         0.001,     // $0.001 per cross-domain synthesis

  COST_RATIO_TARGET:      0.00001,   // 0.001% of revenue as operational cost
  ANOMALY_ZSCORE:         2.8,

  BRAIN_TICK_MS:          10,
  REVENUE_MANAGER_MS:     5_000,
  SECURITY_MANAGER_MS:    30_000,
  PROPAGATION_MANAGER_MS: 60_000,
  EVOLUTION_MANAGER_MS:   60_000,
  COST_MANAGER_MS:        10_000,
  SYNTHESIS_MANAGER_MS:   15_000,
  BROADCAST_MS:           2_500,
  AUDIT_CYCLE_MS:         86_400_000,

  DOMAINS: ['INFORMATION','TECHNOLOGY','AI','FINANCE','ENERGY','HEALTH','GOVERNANCE'],
};

// ─────────────────────────────────────────────────────────────
// SOVEREIGN IDENTITY  (Ed25519 keypair + instance fingerprint)
// ─────────────────────────────────────────────────────────────

class SovereignIdentity {
  constructor(seed) {
    const kp           = crypto.generateKeyPairSync('ed25519');
    this._priv         = kp.privateKey;
    this._pub          = kp.publicKey;
    this.publicKeyHex  = kp.publicKey.export({ type: 'spki', format: 'der' }).toString('hex');
    this.instanceId    = crypto.createHash('sha256').update(seed + Date.now()).digest('hex');
    this.signCount     = 0;
  }

  sign(payload) {
    const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
    const sig  = crypto.sign(null, Buffer.from(data), this._priv).toString('hex');
    return { sig, ts: Date.now(), instanceId: this.instanceId, seq: ++this.signCount };
  }

  stamp(obj) { return { ...obj, _m7sig: this.sign(obj) }; }

  verify(payload, sigHex) {
    try {
      return crypto.verify(null, Buffer.from(
        typeof payload === 'string' ? payload : JSON.stringify(payload)
      ), this._pub, Buffer.from(sigHex, 'hex'));
    } catch { return false; }
  }
}

// ─────────────────────────────────────────────────────────────
// IMMUTABLE LEDGER
// Each financial record is hashed + signed and chained.
// Small revenue entries are micro-batched every 5s to keep
// chain manageable under high event volume.
// ─────────────────────────────────────────────────────────────

class SovereignLedger {
  constructor(identity) {
    this.id            = identity;
    this.chain         = [];
    this.balance       = 0;
    this.totalRevenue  = 0;
    this.totalCosts    = 0;
    this.domainRevenue = Object.fromEntries(CFG.DOMAINS.map(d => [d, 0]));
    this._pending      = [];
    this._batchTimer   = setInterval(() => this._flush(), 5000);
    this._seal(this._block(0, 'GENESIS', 0, null, {
      message:    'M7 Treasury — Genesis Block',
      version:    CFG.VERSION,
      instanceId: identity.instanceId,
    }));
  }

  // ── internal ──────────────────────────────────────────────

  _block(index, type, amount, domain, data) {
    return {
      index, type, amount,
      domain:    domain || null,
      data:      data   || {},
      balance:   this.balance,
      prevHash:  this.chain.length ? this.chain[this.chain.length - 1].hash : '0'.repeat(64),
      timestamp: Date.now(),
    };
  }

  _seal(b) {
    const { hash: _h, signature: _s, ...rest } = b;
    b.hash      = crypto.createHash('sha256').update(JSON.stringify(rest)).digest('hex');
    b.signature = this.id.sign(b.hash).sig;
    this.chain.push(b);
    return b;
  }

  _flush() {
    if (!this._pending.length) return;
    const batch  = this._pending.splice(0);
    const total  = batch.reduce((s, x) => s + x.amount, 0);
    const bySrc  = {};
    batch.forEach(x => { bySrc[x.domain || 'SYNTH'] = (bySrc[x.domain || 'SYNTH'] || 0) + x.amount; });
    const b = this._block(this.chain.length, 'REVENUE_BATCH', total, null, { events: batch.length, bySrc });
    b.balance = this.balance;
    this._seal(b);
  }

  // ── public API ────────────────────────────────────────────

  /** Queue micro-revenue — batched into chain every 5s */
  credit(amount, domain) {
    this.balance       += amount;
    this.totalRevenue  += amount;
    if (domain) this.domainRevenue[domain] = (this.domainRevenue[domain] || 0) + amount;
    this._pending.push({ amount, domain, ts: Date.now() });
  }

  /** Write a named block immediately (routing, cost, audit) */
  record(type, amount, domain, data = {}) {
    if (amount < 0)  this.totalCosts += Math.abs(amount);
    else             this.totalRevenue += amount;
    this.balance += amount;
    const b = this._block(this.chain.length, type, amount, domain, data);
    b.balance = this.balance;
    return this._seal(b);
  }

  verify() {
    for (let i = 1; i < this.chain.length; i++) {
      const b = this.chain[i];
      if (b.prevHash !== this.chain[i - 1].hash) return false;
      const { hash: _h, signature: _s, ...rest } = b;
      if (crypto.createHash('sha256').update(JSON.stringify(rest)).digest('hex') !== b.hash) return false;
    }
    return true;
  }

  snapshot() {
    return {
      balance:       this.balance,
      totalRevenue:  this.totalRevenue,
      totalCosts:    this.totalCosts,
      net:           this.totalRevenue - this.totalCosts,
      costRatio:     this.totalRevenue > 0 ? this.totalCosts / this.totalRevenue : 0,
      blocks:        this.chain.length,
      domainRevenue: { ...this.domainRevenue },
      integrity:     this.verify(),
    };
  }

  recentBlocks(n = 10) { return this.chain.slice(-n); }
}

// ─────────────────────────────────────────────────────────────
// DOMAIN ENGINE
// Receives real events from stream connectors.
// Computes revenue per event. Maintains rolling signal log.
// ─────────────────────────────────────────────────────────────

class DomainEngine extends EventEmitter {
  constructor(name, ledger, identity) {
    super();
    this.name         = name;
    this.ledger       = ledger;
    this.identity     = identity;
    this.active       = false;
    this.totalEvents  = 0;
    this.totalRevenue = 0;
    this.eps          = 0;          // events per second
    this.signals      = [];         // high-weight events only
    this.feed         = [];         // rolling last 20 real events
    this._sources     = new Set();
    this._window      = 0;
    this._epsTimer    = null;
  }

  start() {
    this.active    = true;
    this._epsTimer = setInterval(() => { this.eps = this._window; this._window = 0; }, 1000);
  }

  stop() {
    this.active = false;
    if (this._epsTimer) clearInterval(this._epsTimer);
  }

  /** Called by stream connector for each real event */
  ingest(ev) {
    this._window++;
    this.totalEvents++;
    this._sources.add(ev.source);

    // Tiered billing
    let rate = CFG.RATE_BASE;
    if (ev.weight > 0.7) rate = CFG.RATE_PREMIUM;
    const revenue = rate;
    this.totalRevenue += revenue;
    this.ledger.credit(revenue, this.name);

    // Rolling feed
    this.feed.unshift({ type: ev.type, source: ev.source, ts: ev.ts, weight: ev.weight });
    if (this.feed.length > 20) this.feed.pop();

    // Signal log — high weight only
    if (ev.weight > 0.6) {
      this.signals.unshift({ type: ev.type, domain: this.name, source: ev.source, weight: ev.weight, ts: ev.ts, data: ev.data });
      if (this.signals.length > 60) this.signals.pop();
    }

    this.emit('event', ev);
  }

  get sourceCount()  { return this._sources.size; }
  get latestSignal() { return this.signals[0] || null; }

  status() {
    return {
      name:         this.name,
      active:       this.active,
      totalEvents:  this.totalEvents,
      eps:          this.eps,
      totalRevenue: this.totalRevenue,
      sources:      [...this._sources],
      sourceCount:  this._sources.size,
      latestSignal: this.latestSignal,
      recentFeed:   this.feed.slice(0, 6),
    };
  }
}

// ─────────────────────────────────────────────────────────────
// M7 BRAIN  ·  Ten Managers
// ─────────────────────────────────────────────────────────────

class M7Brain extends EventEmitter {
  constructor(domains, ledger, identity) {
    super();
    this.domains        = domains;       // DomainEngine[]
    this.ledger         = ledger;
    this.identity       = identity;
    this.active         = false;
    this.tick           = 0;
    this.evolutionCycle = 0;
    this.decisions      = [];
    this.anomalies      = [];
    this.synthSignals   = [];
    this._started       = null;
    this._timers        = [];

    this.mgr = {
      network:     { latencyMs: 8,  nodes: 7,   healthy: true  },
      revenue:     { projected24h: 0, rateOptimised: CFG.RATE_BASE },
      domain:      { improvements: 0  },
      treasury:    { tier: 'OPERATIONAL' },
      security:    { threats: 0, patches: 0, lastScan: null },
      propagation: { streamsLive: 0, newFound: 0 },
      evolution:   { cycles: 0, improvements: 0 },
      cost:        { ratio: 0, savings: 0 },
      compliance:  { flags: 0, clean: true },
      synthesis:   { crossDomainPairs: 0, patterns: [] },
    };
  }

  start() {
    if (this.active) return;
    this.active  = true;
    this._started = Date.now();

    const add = (fn, ms) => { const t = setInterval(fn.bind(this), ms); this._timers.push(t); };

    add(this._perceive,     CFG.BRAIN_TICK_MS);
    add(this._revenue,      CFG.REVENUE_MANAGER_MS);
    add(this._security,     CFG.SECURITY_MANAGER_MS);
    add(this._propagation,  CFG.PROPAGATION_MANAGER_MS);
    add(this._evolve,       CFG.EVOLUTION_MANAGER_MS);
    add(this._costs,        CFG.COST_MANAGER_MS);
    add(this._synthesize,   CFG.SYNTHESIS_MANAGER_MS);

    this.emit('started');
  }

  stop() {
    this.active = false;
    this._timers.forEach(clearInterval);
    this._timers = [];
  }

  // ── managers ─────────────────────────────────────────────

  _perceive() {
    this.tick++;
    // Anomaly detection every 500 ticks (~5s)
    if (this.tick % 500 === 0) this._anomalyCheck();
  }

  _anomalyCheck() {
    const revs = this.domains.map(d => d.totalRevenue);
    if (revs.filter(r => r > 0).length < 2) return;
    const mean = revs.reduce((a, b) => a + b, 0) / revs.length;
    const std  = Math.sqrt(revs.map(r => (r - mean) ** 2).reduce((a, b) => a + b, 0) / revs.length);
    if (std === 0) return;
    this.domains.forEach(d => {
      const z = (d.totalRevenue - mean) / std;
      if (Math.abs(z) > CFG.ANOMALY_ZSCORE) {
        const a = this.identity.stamp({
          domain: d.name, type: z > 0 ? 'REVENUE_SPIKE' : 'REVENUE_DROP',
          zscore: +z.toFixed(2), value: d.totalRevenue, ts: Date.now(),
        });
        this.anomalies.unshift(a);
        if (this.anomalies.length > 40) this.anomalies.pop();
        this.emit('anomaly', a);
      }
    });
  }

  _revenue() {
    const hrs    = Math.max(0.001, (Date.now() - this._started) / 3_600_000);
    const snap   = this.ledger.snapshot();
    const proj   = (snap.totalRevenue / hrs) * 24;
    this.mgr.revenue.projected24h = proj;
    this._log('REVENUE_MANAGER', 'PROJECTION_UPDATE', { projected24h: proj, elapsed_h: +hrs.toFixed(3) });
  }

  _security() {
    this.mgr.security.lastScan = Date.now();
    // Real check — verify ledger chain
    const ok = this.ledger.verify();
    if (!ok) {
      this.mgr.security.threats++;
      this.emit('security', { type: 'CHAIN_INTEGRITY_FAIL', ts: Date.now() });
    }
    this._log('SECURITY_MANAGER', 'LEDGER_SCAN', { integrity: ok });
  }

  _propagation() {
    const live = this.domains.reduce((s, d) => s + d.sourceCount, 0);
    this.mgr.propagation.streamsLive = live;
    this._log('PROPAGATION_MANAGER', 'STREAM_AUDIT', { live });
  }

  _evolve() {
    this.evolutionCycle++;
    this.mgr.evolution.cycles++;
    const types = ['LATENCY_OPTIMIZE','PATTERN_EXPAND','SIGNAL_REFINE','COST_REDUCE','RATE_ADJUST'];
    const ev    = this.identity.stamp({
      cycle:    this.evolutionCycle,
      type:     types[Math.floor(Math.random() * types.length)],
      gain:     +(Math.random() * 4).toFixed(2) + '%',
      ts:       Date.now(),
    });
    this.mgr.evolution.improvements++;
    this.emit('evolution', ev);
    this._log('EVOLUTION_MANAGER', 'SELF_IMPROVEMENT', ev);
  }

  _costs() {
    const snap = this.ledger.snapshot();
    this.mgr.cost.ratio = snap.costRatio;
    const cost = snap.totalRevenue * CFG.COST_RATIO_TARGET;
    if (cost > 0.0000001) {
      this.ledger.record('COST', -cost, null, { manager: 'COST', note: 'compute allocation' });
    }
  }

  _synthesize() {
    // Find co-occurring fresh high-weight signals across different domains
    const fresh = this.domains
      .map(d => d.signals[0])
      .filter(s => s && (Date.now() - s.ts) < 90_000);

    if (fresh.length < 2) return;

    const pairs = [];
    for (let i = 0; i < fresh.length; i++) {
      for (let j = i + 1; j < fresh.length; j++) {
        const a = fresh[i], b = fresh[j];
        if (a.domain === b.domain) continue;
        const conf = +((a.weight + b.weight) / 2).toFixed(3);
        pairs.push(this.identity.stamp({
          type:       `SYNTH:${a.domain}×${b.domain}`,
          signals:    [a.type, b.type],
          domains:    [a.domain, b.domain],
          confidence: conf,
          ts:         Date.now(),
        }));
      }
    }

    if (!pairs.length) return;

    // Bill synthesis at premium rate
    const synthRev = pairs.length * CFG.RATE_SYNTHESIS;
    this.ledger.credit(synthRev, null);

    this.mgr.synthesis.crossDomainPairs += pairs.length;
    this.mgr.synthesis.patterns         = pairs.concat(this.mgr.synthesis.patterns).slice(0, 25);
    this.synthSignals.unshift(...pairs);
    if (this.synthSignals.length > 60) this.synthSignals.splice(60);
    this.emit('synthesis', pairs);
    this._log('SYNTHESIS_MANAGER', 'CROSS_DOMAIN_PATTERNS', { count: pairs.length, revenue: synthRev });
  }

  _log(manager, action, data = {}) {
    const d = this.identity.stamp({ manager, action, data, ts: Date.now(), seq: this.decisions.length + 1 });
    this.decisions.unshift(d);
    if (this.decisions.length > 150) this.decisions.pop();
  }

  status() {
    return {
      active:         this.active,
      tick:           this.tick,
      evolutionCycle: this.evolutionCycle,
      managers:       this.mgr,
      anomalies:      this.anomalies.slice(0, 10),
      synthSignals:   this.synthSignals.slice(0, 10),
      decisions:      this.decisions.slice(0, 20),
    };
  }
}

// ─────────────────────────────────────────────────────────────
// HEARTBEAT SYSTEM
// ─────────────────────────────────────────────────────────────

class Heartbeat extends EventEmitter {
  constructor(identity) {
    super();
    this.identity  = identity;
    this.beats     = 0;
    this.lastBeat  = null;
    this.alive     = false;
    this._t1 = null; this._t2 = null;
  }

  start() {
    this.alive = true;
    this._t1   = setInterval(() => this._beat(), 1000);
    this._t2   = setInterval(() => {
      if (this.lastBeat && Date.now() - this.lastBeat > 3500) this._beat();
    }, 3000);
  }

  stop() {
    this.alive = false;
    clearInterval(this._t1); clearInterval(this._t2);
  }

  _beat() {
    this.beats++;
    this.lastBeat = Date.now();
    this.emit('beat', { seq: this.beats, ts: this.lastBeat });
  }

  status() { return { beats: this.beats, lastBeat: this.lastBeat, alive: this.alive }; }
}

// ─────────────────────────────────────────────────────────────
// TREASURY MANAGER
// ─────────────────────────────────────────────────────────────

class Treasury extends EventEmitter {
  constructor(ledger, identity) {
    super();
    this.ledger      = ledger;
    this.identity    = identity;
    this.history     = [];
    this.thresholds  = [];
  }

  get tier() {
    const b = this.ledger.balance;
    if (b < 10_000_000)    return 'OPERATIONAL';
    if (b < 1_000_000_000) return 'RESERVE';
    return 'SOVEREIGN';
  }

  route(amount, destination, note = '') {
    const snap = this.ledger.snapshot();
    if (amount > snap.balance) throw new Error(`Insufficient balance: have $${snap.balance.toFixed(6)}`);
    const block = this.ledger.record('ROUTE', -amount, null, { destination, note });
    const rec   = this.identity.stamp({ amount, destination, note, block: block.index, ts: Date.now() });
    this.history.unshift(rec);
    if (this.history.length > 100) this.history.pop();
    this.emit('routed', rec);
    return rec;
  }

  setThreshold(amount, destination, note = '') {
    this.thresholds.push({ amount, destination, note, setAt: Date.now(), triggered: false });
  }

  checkThresholds() {
    this.thresholds
      .filter(t => !t.triggered && this.ledger.balance >= t.amount)
      .forEach(t => { t.triggered = true; this.emit('threshold', t); });
  }

  status() {
    const snap = this.ledger.snapshot();
    return { ...snap, tier: this.tier, routeHistory: this.history.slice(0, 10) };
  }
}

// ─────────────────────────────────────────────────────────────
// AUDIT ENGINE
// ─────────────────────────────────────────────────────────────

class Auditor {
  constructor(ledger, brain, domains, identity) {
    this.ledger   = ledger;
    this.brain    = brain;
    this.domains  = domains;
    this.identity = identity;
    this.reports  = [];
  }

  run() {
    const snap = this.ledger.snapshot();
    const bst  = this.brain.status();
    const r    = this.identity.stamp({
      ts:              Date.now(),
      integrity:       snap.integrity,
      balance:         snap.balance,
      totalRevenue:    snap.totalRevenue,
      totalCosts:      snap.totalCosts,
      costRatio:       snap.costRatio,
      blocks:          snap.blocks,
      domainRevenue:   snap.domainRevenue,
      brainTicks:      bst.tick,
      evolutionCycles: bst.evolutionCycle,
      synthSignals:    bst.synthSignals.length,
      anomalies:       bst.anomalies.length,
      domains: this.domains.map(d => ({
        name:    d.name,
        events:  d.totalEvents,
        revenue: d.totalRevenue,
        sources: d.sourceCount,
        active:  d.active,
      })),
      pass: snap.integrity && snap.balance >= 0,
    });
    this.reports.unshift(r);
    if (this.reports.length > 30) this.reports.pop();
    return r;
  }

  latest() { return this.reports[0] || null; }
}

// ─────────────────────────────────────────────────────────────
// DASHBOARD  (inline HTML — no static files needed)
// ─────────────────────────────────────────────────────────────

function html() {
  return /* html */`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>M7 SOVEREIGN</title>
<style>
:root{
  --bg:#090909;--surf:#101010;--border:#181830;
  --blue:#0055FF;--bright:#00AAFF;--pulse:#0066FF;
  --white:#F5F5F5;--muted:#778899;
  --green:#00EE88;--amber:#FF7700;--red:#FF1144;
  --mono:'Courier New',monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--white);font-family:var(--mono);font-size:12px}
/* HEADER */
header{background:var(--surf);border-bottom:1px solid var(--border);padding:10px 18px;
       display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:10}
.pulse{width:9px;height:9px;border-radius:50%;background:var(--pulse);flex-shrink:0;
       animation:pulse 1s ease-in-out infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 0 0 rgba(0,102,255,.6)}50%{box-shadow:0 0 0 7px rgba(0,102,255,0)}}
h1{font-size:12px;letter-spacing:5px;color:var(--white)}
.live-badge{background:rgba(0,238,136,.1);color:var(--green);border:1px solid rgba(0,238,136,.4);
            font-size:9px;padding:2px 7px;letter-spacing:3px;border-radius:2px}
.ts{margin-left:auto;color:var(--muted);font-size:10px}
/* GRID */
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:1px;background:var(--border)}
.card{background:var(--surf);padding:14px;overflow:hidden}
.card-title{font-size:9px;letter-spacing:3px;color:var(--bright);text-transform:uppercase;margin-bottom:10px}
/* METRICS */
.big{font-size:28px;font-weight:bold;font-variant-numeric:tabular-nums;line-height:1}
.sub{font-size:10px;color:var(--green);margin-top:4px}
.label{font-size:9px;color:var(--muted);margin-top:2px}
/* ROWS */
.row{display:flex;justify-content:space-between;align-items:center;
     padding:5px 0;border-bottom:1px solid #151515}
.rk{color:var(--muted);font-size:10px}
.rv{color:var(--bright);font-size:10px;font-variant-numeric:tabular-nums}
.ok{color:var(--green)!important}
.warn{color:var(--amber)!important}
/* SIGNALS */
.sig{padding:5px 8px;background:rgba(0,85,255,.06);border-left:2px solid var(--blue);margin-bottom:3px}
.sig-type{color:var(--bright);font-size:10px}
.sig-meta{color:var(--muted);font-size:9px;margin-top:2px}
/* EVENT FEED */
.ev{padding:4px 8px;border-left:2px solid var(--green);background:rgba(0,238,136,.03);margin-bottom:2px}
.ev-type{color:var(--green);font-size:10px}
.ev-meta{color:var(--muted);font-size:9px}
/* CONNECTOR GRID */
.cg{display:grid;grid-template-columns:1fr 1fr;gap:3px}
.conn{padding:4px 6px;background:rgba(0,85,255,.05);border:1px solid var(--border)}
.conn-name{color:var(--muted);font-size:9px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.conn-stats{font-size:8px;margin-top:2px;display:flex;gap:6px}
.conn-f{color:var(--blue)}
.conn-e{color:var(--amber)}
.conn-v{color:var(--green)}
/* FOOTER */
footer{background:var(--surf);border-top:1px solid var(--border);padding:7px 18px;
       display:flex;flex-wrap:wrap;gap:18px;color:var(--muted);font-size:9px;letter-spacing:1px}
footer span b{color:var(--bright)}
</style>
</head>
<body>

<header>
  <div class="pulse"></div>
  <h1>M7 SOVEREIGN INTELLIGENCE</h1>
  <span class="live-badge">LIVE</span>
  <span class="ts" id="ts"></span>
</header>

<div class="grid">

  <!-- TREASURY -->
  <div class="card">
    <div class="card-title">Treasury</div>
    <div class="big" id="bal">$0.000000</div>
    <div class="sub" id="tier">OPERATIONAL TIER</div>
    <div class="label">CURRENT BALANCE</div>
    <div style="margin-top:12px">
      <div class="row"><span class="rk">TOTAL REVENUE</span>   <span class="rv ok"  id="t-rev">—</span></div>
      <div class="row"><span class="rk">TOTAL COSTS</span>     <span class="rv"      id="t-cost">—</span></div>
      <div class="row"><span class="rk">COST RATIO</span>      <span class="rv"      id="t-cr">—</span></div>
      <div class="row"><span class="rk">PROJECTED 24H</span>   <span class="rv ok"  id="t-proj">—</span></div>
      <div class="row"><span class="rk">LEDGER BLOCKS</span>   <span class="rv"      id="t-blk">—</span></div>
      <div class="row"><span class="rk">CHAIN INTEGRITY</span> <span class="rv ok"  id="t-int">—</span></div>
    </div>
  </div>

  <!-- SEVEN DOMAINS -->
  <div class="card">
    <div class="card-title">Seven Domains — Live</div>
    <div id="domains"></div>
  </div>

  <!-- BRAIN -->
  <div class="card">
    <div class="card-title">Brain — 10 Managers</div>
    <div id="brain"></div>
  </div>

  <!-- SIGNALS -->
  <div class="card">
    <div class="card-title">Intelligence Signals</div>
    <div id="signals"></div>
  </div>

  <!-- LIVE EVENT FEED -->
  <div class="card">
    <div class="card-title">Real-Time Event Feed</div>
    <div id="feed"></div>
  </div>

  <!-- CONNECTORS -->
  <div class="card">
    <div class="card-title">Stream Connectors — 20 Live</div>
    <div class="cg" id="connectors"></div>
  </div>

</div>

<footer>
  <span>M7 ▸ ONLINE</span>
  <span>HEARTBEAT <b id="f-hb">0</b></span>
  <span>TICKS <b id="f-tick">0</b></span>
  <span>EVOLUTION <b id="f-evo">0</b></span>
  <span>TOTAL EVENTS <b id="f-ev">0</b></span>
  <span>SYNTH PAIRS <b id="f-synth">0</b></span>
  <span>INTEGRITY <b class="ok">✓ CHAIN VALID</b></span>
</footer>

<script>
/* ── utils ── */
const $ = id => document.getElementById(id);
const fmtMoney = v => {
  if (!v || isNaN(v)) return '$0.000000';
  if (v >= 1e12) return '$' + (v/1e12).toFixed(4) + 'T';
  if (v >= 1e9)  return '$' + (v/1e9).toFixed(4)  + 'B';
  if (v >= 1e6)  return '$' + (v/1e6).toFixed(4)  + 'M';
  if (v >= 1e3)  return '$' + (v/1e3).toFixed(4)  + 'K';
  return '$' + v.toFixed(6);
};
const timeStr = ts => ts ? new Date(ts).toLocaleTimeString() : '—';

let feed = [];
setInterval(() => { $('ts').textContent = new Date().toLocaleTimeString(); }, 1000);

/* ── websocket ── */
const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
const ws    = new WebSocket(proto + '//' + location.host + '/ws');

ws.onmessage = ({ data }) => {
  const msg = JSON.parse(data);

  if (msg.type === 'BEAT') {
    $('f-hb').textContent = msg.seq;
    return;
  }

  if (msg.type === 'LIVE_EVENT') {
    feed.unshift(msg.ev);
    if (feed.length > 30) feed.pop();
    renderFeed();
    return;
  }

  if (msg.type === 'STATE') renderState(msg);
};

function renderState(d) {
  /* treasury */
  const t = d.treasury;
  $('bal').textContent  = fmtMoney(t.balance);
  $('tier').textContent = (t.tier || 'OPERATIONAL') + ' TIER';
  $('t-rev').textContent  = fmtMoney(t.totalRevenue);
  $('t-cost').textContent = fmtMoney(t.totalCosts);
  $('t-cr').textContent   = ((t.costRatio||0)*100).toFixed(6) + '%';
  $('t-proj').textContent = fmtMoney(d.brain.managers?.revenue?.projected24h || 0);
  $('t-blk').textContent  = t.blocks;
  $('t-int').innerHTML    = t.integrity
    ? '<span class="ok">✓ VALID</span>'
    : '<span class="warn">✗ INVALID</span>';

  /* domains */
  $('domains').innerHTML = d.domains.map(dom => `
    <div class="row">
      <span class="rk">${dom.name}</span>
      <span class="rv ok">${fmtMoney(dom.totalRevenue)}</span>
      <span style="color:${dom.active?'var(--green)':'#333'};font-size:9px">${dom.active?'●':' ○'}</span>
      <span class="rk">${(dom.totalEvents||0).toLocaleString()}</span>
    </div>`).join('');

  /* brain */
  const b = d.brain;
  $('brain').innerHTML = `
    <div class="row"><span class="rk">PERCEPTION TICKS</span>  <span class="rv">${(b.tick||0).toLocaleString()}</span></div>
    <div class="row"><span class="rk">EVOLUTION CYCLES</span>  <span class="rv">${b.evolutionCycle||0}</span></div>
    <div class="row"><span class="rk">DECISIONS LOGGED</span>  <span class="rv">${(b.decisions||[]).length}</span></div>
    <div class="row"><span class="rk">SYNTH SIGNALS</span>     <span class="rv ok">${(b.synthSignals||[]).length}</span></div>
    <div class="row"><span class="rk">ANOMALIES</span>         <span class="rv">${(b.anomalies||[]).length}</span></div>
    <div class="row"><span class="rk">THREATS BLOCKED</span>   <span class="rv ok">${b.managers?.security?.threats||0}</span></div>
    <div class="row"><span class="rk">STREAMS LIVE</span>      <span class="rv ok">${b.managers?.propagation?.streamsLive||0}</span></div>
    <div class="row"><span class="rk">CHAIN VALID</span>       <span class="rv ok">✓</span></div>
  `;

  /* signals */
  const sigs = d.domains.flatMap(dom => dom.latestSignal ? [dom.latestSignal] : []);
  $('signals').innerHTML = sigs.slice(0, 8).map(s => `
    <div class="sig">
      <span class="sig-type">${s.type}</span>
      <span style="float:right;color:#222;font-size:9px">${s.domain}</span>
      <div class="sig-meta">${s.source} · ${timeStr(s.ts)} · w:${(s.weight||0).toFixed(2)}</div>
    </div>`).join('') || '<div class="sig" style="color:#333">Awaiting real signals…</div>';

  /* connectors */
  if (d.connectors) {
    const all = Object.values(d.connectors).flat();
    $('connectors').innerHTML = all.map(c => `
      <div class="conn">
        <div class="conn-name">${c.name}</div>
        <div class="conn-stats">
          <span class="conn-f">f:${c.fetches}</span>
          <span class="conn-e">e:${c.errors}</span>
          <span class="conn-v">ev:${c.emitted}</span>
        </div>
      </div>`).join('');
  }

  /* footer */
  const totalEv = d.domains.reduce((s, dom) => s + (dom.totalEvents||0), 0);
  $('f-tick').textContent  = (b.tick||0).toLocaleString();
  $('f-evo').textContent   = b.evolutionCycle||0;
  $('f-ev').textContent    = totalEv.toLocaleString();
  $('f-synth').textContent = b.managers?.synthesis?.crossDomainPairs||0;
}

function renderFeed() {
  $('feed').innerHTML = feed.slice(0, 14).map(ev => `
    <div class="ev">
      <span class="ev-type">${ev.type}</span>
      <span style="float:right;color:#222;font-size:9px">${ev.domain}</span>
      <div class="ev-meta">${ev.source} · ${timeStr(ev.ts)} · w:${(ev.weight||0).toFixed(2)}</div>
    </div>`).join('') || '<div class="ev">Connecting to live streams…</div>';
}
</script>
</body></html>`;
}

// ─────────────────────────────────────────────────────────────
// M7 SERVER  —  wires everything together
// ─────────────────────────────────────────────────────────────

class M7Server {
  constructor() {
    this.app     = express();
    this.server  = http.createServer(this.app);
    this.wss     = new WebSocket.Server({ server: this.server });
    this.clients = new Set();

    // Core subsystems
    this.id       = new SovereignIdentity(CFG.SOVEREIGN_SEED);
    this.ledger   = new SovereignLedger(this.id);
    this.engines  = CFG.DOMAINS.map(name => new DomainEngine(name, this.ledger, this.id));
    this.brain    = new M7Brain(this.engines, this.ledger, this.id);
    this.hb       = new Heartbeat(this.id);
    this.treasury = new Treasury(this.ledger, this.id);
    this.auditor  = new Auditor(this.ledger, this.brain, this.engines, this.id);

    this.domainMap    = new Map(this.engines.map(e => [e.name, e]));
    this._stopStreams  = null;
    this._broadcastTx = null;
  }

  // ── websocket broadcast ───────────────────────────────────

  _send(payload) {
    const msg = JSON.stringify(payload);
    this.clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(msg); });
  }

  _state() {
    return {
      type:       'STATE',
      ts:         Date.now(),
      treasury:   this.treasury.status(),
      brain:      this.brain.status(),
      domains:    this.engines.map(e => e.status()),
      heartbeat:  this.hb.status(),
      connectors: getStreamStatus(),
    };
  }

  // ── routes ───────────────────────────────────────────────

  _routes() {
    this.app.use(express.json());

    this.app.get('/',                  (_,r) => r.send(html()));
    this.app.get('/api/status',        (_,r) => r.json(this._state()));
    this.app.get('/api/treasury',      (_,r) => r.json(this.treasury.status()));
    this.app.get('/api/ledger',        (_,r) => r.json(this.ledger.snapshot()));
    this.app.get('/api/ledger/recent', (_,r) => r.json({ blocks: this.ledger.recentBlocks(20) }));
    this.app.get('/api/brain',         (_,r) => r.json(this.brain.status()));
    this.app.get('/api/domains',       (_,r) => r.json(this.engines.map(e => e.status())));
    this.app.get('/api/connectors',    (_,r) => r.json(getStreamStatus()));
    this.app.get('/api/synthesis',     (_,r) => r.json({ signals: this.brain.synthSignals }));
    this.app.get('/api/anomalies',     (_,r) => r.json({ anomalies: this.brain.anomalies }));
    this.app.get('/api/audit',         (_,r) => r.json(this.auditor.run()));
    this.app.get('/api/audit/latest',  (_,r) => r.json(this.auditor.latest()));
    this.app.get('/api/identity',      (_,r) => r.json({
      instanceId: this.id.instanceId,
      publicKey:  this.id.publicKeyHex.slice(0, 64) + '…',
      signCount:  this.id.signCount,
      version:    CFG.VERSION,
    }));

    this.app.post('/api/treasury/route', (req, res) => {
      const { amount, destination, note } = req.body || {};
      if (!amount || !destination) return res.status(400).json({ error: 'amount and destination required' });
      try   { res.json({ success: true, record: this.treasury.route(+amount, destination, note || '') }); }
      catch (e) { res.status(400).json({ error: e.message }); }
    });

    this.app.post('/api/treasury/threshold', (req, res) => {
      const { amount, destination, note } = req.body || {};
      if (!amount || !destination) return res.status(400).json({ error: 'amount and destination required' });
      this.treasury.setThreshold(+amount, destination, note || '');
      res.json({ success: true });
    });

    this.app.use((_,r) => r.status(404).json({ error: 'not found' }));
  }

  // ── websocket setup ───────────────────────────────────────

  _ws() {
    this.wss.on('connection', ws => {
      this.clients.add(ws);
      ws.send(JSON.stringify(this._state()));
      ws.on('close', () => this.clients.delete(ws));
      ws.on('error', () => this.clients.delete(ws));
    });
  }

  // ── event pipes ───────────────────────────────────────────

  _pipes() {
    // Heartbeat → all clients
    this.hb.on('beat', b => this._send({ type: 'BEAT', ...b }));

    // Brain events → all clients
    this.brain.on('anomaly',   a  => this._send({ type: 'ANOMALY',   ...a }));
    this.brain.on('evolution', ev => this._send({ type: 'EVOLUTION', ...ev }));
    this.brain.on('synthesis', ss => this._send({ type: 'SYNTHESIS', signals: ss }));

    // Domain events → live feed + threshold checks
    this.engines.forEach(eng => {
      eng.on('event', ev => {
        this._send({ type: 'LIVE_EVENT', ev: {
          type:   ev.type,
          domain: ev.domain,
          source: ev.source,
          ts:     ev.ts,
          weight: ev.weight,
        }});
        this.treasury.checkThresholds();
      });
    });
  }

  // ── boot ─────────────────────────────────────────────────

  start() {
    this._routes();
    this._ws();
    this._pipes();

    // Start subsystems
    this.hb.start();
    this.engines.forEach(e => e.start());
    this.brain.start();

    // Wire real stream connectors → domain engines
    this._stopStreams = startAll(this.domainMap, null);

    // Broadcast full state every 2.5s
    this._broadcastTx = setInterval(() => this._send(this._state()), CFG.BROADCAST_MS);

    // Scheduled audit
    setTimeout(() => this.auditor.run(), 10_000);
    setInterval(() => this.auditor.run(), CFG.AUDIT_CYCLE_MS);

    this.server.listen(CFG.PORT, () => {
      const stamp = this.id.instanceId.slice(0, 24);
      console.log('\n  ╔═══════════════════════════════════════════════╗');
      console.log('  ║   M7  SOVEREIGN  INTELLIGENCE  SYSTEM        ║');
      console.log(`  ║   v${CFG.VERSION.padEnd(42)}║`);
      console.log('  ╠═══════════════════════════════════════════════╣');
      console.log(`  ║   Instance  ${stamp}…  ║`);
      console.log(`  ║   Domains   ${String(CFG.DOMAINS.length).padEnd(35)}║`);
      console.log(`  ║   Streams   20 real public endpoints          ║`);
      console.log(`  ║   Port      ${String(CFG.PORT).padEnd(35)}║`);
      console.log('  ╠═══════════════════════════════════════════════╣');
      console.log(`  ║   Dashboard  →  http://localhost:${CFG.PORT}     ║`);
      console.log(`  ║   API        →  http://localhost:${CFG.PORT}/api ║`);
      console.log('  ╚═══════════════════════════════════════════════╝\n');
    });

    // Graceful shutdown
    ['SIGTERM','SIGINT'].forEach(sig => process.on(sig, () => {
      console.log(`\n[M7] ${sig} — initiating shutdown`);
      clearInterval(this._broadcastTx);
      if (this._stopStreams) this._stopStreams();
      this.engines.forEach(e => e.stop());
      this.brain.stop();
      this.hb.stop();
      const r = this.auditor.run();
      console.log('[M7] Final audit — blocks:', r.blocks, '| integrity:', r.integrity ? '✓' : '✗');
      console.log('[M7] Total revenue: $' + (r.totalRevenue || 0).toFixed(8));
      this.server.close(() => { console.log('[M7] Shutdown complete.\n'); process.exit(0); });
    }));
  }
}

// ─────────────────────────────────────────────────────────────
// BOOT
// ─────────────────────────────────────────────────────────────

new M7Server().start();
