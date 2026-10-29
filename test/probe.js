'use strict';
// Latency probe: run alongside bots.js (separate process) to measure cross-node
// delivery under load without the load generator skewing the numbers.
// Usage: node probe.js [seconds]   env: HOST (localhost), INTERVAL ms (10)
const WebSocket = require('ws');

const HOST = process.env.HOST || 'localhost';
const SECONDS = Number(process.argv[2] || 20);
const INTERVAL = Number(process.env.INTERVAL || 10);
const PORTS = [8001, 8002, 8003];

const sent = new Map(); // opId -> send time (ns)
const lat = { cross: [], echo: [] };
const socks = PORTS.map(p => new WebSocket(`ws://${HOST}:${p}/`, { perMessageDeflate: false }));

socks.forEach((ws, i) => ws.on('message', d => {
  const s = d.toString();
  if (!s.includes('"probe-')) return;
  const now = process.hrtime.bigint();
  const m = JSON.parse(s);
  const t0 = sent.get(m.opId);
  if (t0 === undefined) return;
  const ms = Number(now - t0) / 1e6;
  // sender is socket 0 (8001): its own copy is the echo, others are cross-node
  (i === 0 ? lat.echo : lat.cross).push(ms);
}));

const stats = a => {
  if (!a.length) return 'n=0';
  a.sort((x, y) => x - y);
  const avg = a.reduce((s, v) => s + v, 0) / a.length;
  const q = p => a[Math.min(a.length - 1, Math.floor(a.length * p))].toFixed(2);
  return `n=${a.length} avg=${avg.toFixed(2)}ms p50=${q(0.5)} p90=${q(0.9)} p99=${q(0.99)} max=${a[a.length - 1].toFixed(1)}`;
};

Promise.all(socks.map(ws => new Promise(r => ws.on('open', r)))).then(() => {
  let k = 0;
  socks[0].send(JSON.stringify({ type: 'create_shape', id: 'probe-shape', x: 0, y: 0, w: 10, h: 10, opId: 'probe-create' }));
  const iv = setInterval(() => {
    const opId = 'probe-' + k++;
    sent.set(opId, process.hrtime.bigint());
    socks[0].send(JSON.stringify({ type: 'move_shape', id: 'probe-shape', x: k % 500, y: 0, opId }));
  }, INTERVAL);
  setTimeout(() => {
    clearInterval(iv);
    setTimeout(() => {
      console.log('cross-node (8001 -> 8002/8003):', stats(lat.cross));
      console.log('echo       (8001 -> 8001):     ', stats(lat.echo));
      process.exit(0);
    }, 1000);
  }, SECONDS * 1000);
});
