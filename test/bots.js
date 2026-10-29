#!/usr/bin/env node
// Load + chaos + convergence test for the 3-node board.
// Usage: node bots.js [BOTS=50] [DURATION=180] [KILL_AT=60] [KILL_FOR=30] [KILL_NODE=node2] [RATE=10] [NO_KILL=1] [HOST=localhost]
// (args may also be given as env vars or --key=value)
'use strict';
const WebSocket = require('ws');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { execFile } = require('child_process');
const { performance } = require('perf_hooks');

for (const a of process.argv.slice(2)) {
  const m = a.match(/^-*([A-Za-z_]+)=(.*)$/);
  if (m) process.env[m[1].toUpperCase()] = m[2];
}
const E = process.env;
const HOST = E.HOST || 'localhost';
const BOTS = +(E.BOTS || 50), DURATION = +(E.DURATION || 180), RATE = +(E.RATE || 10);
const KILL_AT = +(E.KILL_AT || 60), KILL_FOR = +(E.KILL_FOR || 30), KILL_NODE = E.KILL_NODE || 'node2';
const NO_KILL = E.NO_KILL === '1' || E.NO_KILL === 'true';
const PORTS = [8001, 8002, 8003];
const SHARED = 30;
const RUN = Date.now().toString(36);
const ROOT = path.join(__dirname, '..');

const T0 = Date.now();
const log = (...a) => console.log(`[${((Date.now() - T0) / 1000).toFixed(1).padStart(6)}s]`, ...a);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const rnd = n => Math.floor(Math.random() * n);
const uuid = () => crypto.randomUUID();
const color = () => '#' + rnd(0xfff).toString(16).padStart(3, '0');
const cmpTs = (a, b) => (a[0] - b[0]) || (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0);

// ---------- global bookkeeping ----------
const sent = new Map();       // opId -> { bot, t, type, echo, cross }
const seen = new Map();       // opId -> ts (first time any bot saw it applied)
const expected = new Map();   // shapeId -> { ts, x, y } from max-ts create/move seen
const createdIds = new Set(), lineIds = new Set();
const foreign = new Set();    // ops applied during the run that bots did not send (e.g. browser users)
let stopping = false, sending = false, reconnects = 0, dupRecv = 0;

function onApplied(bot, m) {
  const now = performance.now();
  if (bot.got.has(m.opId)) { dupRecv++; return; }
  bot.got.add(m.opId);
  bot.unacked.delete(m.opId);               // own echo == ack
  if (!seen.has(m.opId)) {
    seen.set(m.opId, m.ts);
    if ((m.type === 'move_shape' || m.type === 'create_shape') && Array.isArray(m.ts)) {
      const e = expected.get(m.id);
      if (!e || cmpTs(m.ts, e.ts) > 0) expected.set(m.id, { ts: m.ts, x: m.x, y: m.y });
    }
  }
  const s = sent.get(m.opId);
  if (!s) { foreign.add(m.opId); return; }
  const dt = now - s.t;
  if (bot.idx === s.bot) { if (s.echo == null) s.echo = dt; }
  else if (s.cross == null || dt < s.cross) s.cross = dt;
}

// ---------- bots ----------
class Bot {
  constructor(idx) {
    this.idx = idx; this.portIdx = idx % PORTS.length;
    this.unacked = new Map(); this.got = new Set(); this.seq = 0; this.ws = null;
    this.connect();
  }
  connect() {
    if (stopping) return;
    const ws = this.ws = new WebSocket(`ws://${HOST}:${PORTS[this.portIdx]}/`);
    ws.on('open', () => { for (const m of this.unacked.values()) ws.send(JSON.stringify(m)); });
    ws.on('message', d => {
      let m; try { m = JSON.parse(d); } catch { return; }
      if (m.type !== 'snapshot' && m.opId) onApplied(this, m);
    });
    ws.on('error', () => {});
    ws.on('close', () => {
      if (stopping) return;
      reconnects++;
      this.portIdx = (this.portIdx + 1) % PORTS.length;
      setTimeout(() => this.connect(), 200);
    });
  }
  send(m) {
    m.opId = uuid();
    sent.set(m.opId, { bot: this.idx, t: performance.now(), type: m.type });
    this.unacked.set(m.opId, m);
    if (m.type === 'create_shape') createdIds.add(m.id);
    if (m.type === 'draw_line') lineIds.add(m.id);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(m));
    // else: queued in unacked, flushed on reconnect (like the browser client)
  }
  create(id) { this.send({ type: 'create_shape', id, x: rnd(1000), y: rnd(700), w: 20 + rnd(150), h: 20 + rnd(100), color: color() }); }
  tick() {
    if (!sending) return;
    const r = Math.random();
    if (r < 0.60) this.send({ type: 'move_shape', id: 's' + rnd(SHARED), x: rnd(1000), y: rnd(700) });
    else if (r < 0.85) {
      const pts = Array.from({ length: 3 + rnd(8) }, () => [rnd(1000), rnd(700)]);
      this.send({ type: 'draw_line', id: `${RUN}-l${this.idx}-${this.seq++}`, points: pts, color: color(), width: 1 + rnd(5) });
    } else this.create(`${RUN}-s${this.idx}-${this.seq++}`);
  }
}

// ---------- helpers ----------
function getState(port) {
  return new Promise(res => {
    const req = http.get({ host: HOST, port, path: '/state', timeout: 5000 }, r => {
      let b = ''; r.setEncoding('utf8'); r.on('data', c => b += c);
      r.on('end', () => { try { res({ port, body: b, json: JSON.parse(b) }); } catch (e) { res({ port, err: 'bad json: ' + e.message }); } });
    });
    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', e => res({ port, err: e.message || e.code || 'connection error' }));
  });
}
function compose(cmd) {
  log(`>> docker compose ${cmd} ${KILL_NODE}`);
  execFile('docker', ['compose', cmd, KILL_NODE], { cwd: ROOT }, (err, so, se) =>
    log(err ? `!! docker compose ${cmd} failed: ${(se || err.message).trim()}` : `<< docker compose ${cmd} ${KILL_NODE} done`));
}
const pct = (arr, p) => arr.length ? arr[Math.min(arr.length - 1, Math.floor(p / 100 * arr.length))] : NaN;
const stats = arr => {
  arr.sort((a, b) => a - b);
  const avg = arr.reduce((s, x) => s + x, 0) / (arr.length || 1);
  return `n=${arr.length} avg=${avg.toFixed(1)}ms p50=${pct(arr, 50).toFixed(1)}ms p99=${pct(arr, 99).toFixed(1)}ms max=${(arr[arr.length - 1] ?? NaN).toFixed(1)}ms`;
};

// ---------- main ----------
(async () => {
  log(`run=${RUN} host=${HOST} bots=${BOTS} rate=${RATE}/s duration=${DURATION}s ` +
      (NO_KILL ? 'no kill' : `kill ${KILL_NODE} at ${KILL_AT}s for ${KILL_FOR}s`));
  const base = await Promise.all(PORTS.map(getState));
  const baseOps = Math.max(0, ...base.map(s => (s.json && s.json.ops) || 0));
  base.forEach(s => s.err && log(`warn: baseline /state :${s.port} -> ${s.err}`));
  log(`baseline ops=${baseOps}`);

  const bots = Array.from({ length: BOTS }, (_, i) => new Bot(i));
  for (let k = 0; k < SHARED; k++) bots[k % BOTS].create('s' + k);   // shared hot shapes
  await sleep(1000);
  sending = true;
  const timers = bots.map(b => { const t = setInterval(() => b.tick(), 1000 / RATE); return t; });

  if (!NO_KILL) {
    setTimeout(() => compose('stop'), KILL_AT * 1000);
    setTimeout(() => compose('start'), (KILL_AT + KILL_FOR) * 1000);
  }
  const prog = setInterval(() => log(`sent=${sent.size} seen=${seen.size} unacked=${bots.reduce((s, b) => s + b.unacked.size, 0)} reconnects=${reconnects}`), 10000);

  await sleep(DURATION * 1000 - 1000);
  sending = false; timers.forEach(clearInterval); clearInterval(prog);
  log(`stopped sending; waiting for acks (max 5s)`);
  const dl = Date.now() + 5000;
  while (Date.now() < dl && bots.some(b => b.unacked.size)) await sleep(100);
  const unacked = bots.reduce((s, b) => s + b.unacked.size, 0);
  await sleep(3000);
  stopping = true;
  bots.forEach(b => b.ws && b.ws.terminate());

  // ---------- verification ----------
  const states = await Promise.all(PORTS.map(getState));
  console.log('\n================ RESULTS ================');
  const hashes = [];
  for (const s of states) {
    if (s.err) { console.log(`:${s.port}  ERROR ${s.err}`); hashes.push('ERR'); continue; }
    const h = crypto.createHash('sha256').update(s.body).digest('hex');
    hashes.push(h);
    console.log(`:${s.port}  sha256=${h.slice(0, 16)}…  shapes=${s.json.shapes.length} lines=${s.json.lines.length} ops=${s.json.ops}`);
  }
  const identical = hashes.every(h => h !== 'ERR' && h === hashes[0]);
  console.log(`states identical: ${identical ? 'YES' : 'NO'}`);

  const ref = states.find(s => s.json);
  let missShapes = NaN, missLines = NaN, mismatches = NaN, opsDelta = NaN;
  if (ref) {
    missShapes = missLines = mismatches = 0;
    const shapes = new Map(ref.json.shapes.map(s => [s.id, s]));
    const lines = new Set(ref.json.lines.map(l => l.id));
    for (const id of createdIds) if (!shapes.has(id)) missShapes++;
    for (const id of lineIds) if (!lines.has(id)) missLines++;
    const examples = [];
    for (const [id, e] of expected) {
      const s = shapes.get(id);
      if (s && (s.x !== e.x || s.y !== e.y)) {
        mismatches++;
        if (examples.length < 5) examples.push(`${id}: state=(${s.x},${s.y}) ts=${JSON.stringify(s.ts)} expected=(${e.x},${e.y}) ts=${JSON.stringify(e.ts)}`);
      }
    }
    opsDelta = ref.json.ops - baseOps;
    examples.forEach(x => console.log('  mismatch ' + x));
  }
  const total = createdIds.size + lineIds.size, missing = missShapes + missLines;
  const neverSeen = [...sent.keys()].filter(id => !seen.has(id)).length;
  console.log(`created shapes: ${createdIds.size} (missing ${missShapes}), lines: ${lineIds.size} (missing ${missLines})`);
  console.log(`lost create/line actions: ${missing}/${total} = ${(100 * missing / (total || 1)).toFixed(3)}%`);
  console.log(`unique ops sent: ${sent.size} (+${foreign.size} foreign), ops applied (delta vs baseline): ${opsDelta}, never echoed to any bot: ${neverSeen}, unacked by own echo: ${unacked}`);
  console.log(`final position mismatches vs max-ts echoed move: ${mismatches}/${expected.size}`);

  const echo = [], cross = [];
  for (const s of sent.values()) { if (s.echo != null) echo.push(s.echo); if (s.cross != null) cross.push(s.cross); }
  console.log(`cross-bot delivery (min over other bots): ${stats(cross)}`);
  console.log(`echo latency:                             ${stats(echo)}`);
  console.log(`reconnects=${reconnects} duplicate deliveries=${dupRecv} foreign ops=${foreign.size}`);

  const checks = [
    ['all 3 /state identical', identical],
    ['no missing shapes/lines', ref && missing === 0],
    ['ops count == unique ops sent (+foreign)', opsDelta === sent.size + foreign.size],
    ['every op echoed to some bot', neverSeen === 0],
    ['final positions == max-ts move', ref && mismatches === 0],
  ];
  console.log('');
  for (const [n, ok] of checks) console.log(`${ok ? 'PASS' : 'FAIL'}  ${n}`);
  if (unacked) console.log(`WARN  ${unacked} ops never got own echo (server may not re-echo duplicates)`);
  const pass = checks.every(c => c[1]);
  console.log(`\n${pass ? '=== PASS ===' : '=== FAIL ==='}`);
  process.exit(pass ? 0 : 1);
})().catch(e => { console.error(e); process.exit(2); });
