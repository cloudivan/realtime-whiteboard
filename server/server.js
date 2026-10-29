'use strict';
// Replicated whiteboard node.
// Conflict resolution: LWW CRDT. Every op gets ts = [hlc, originId] at the node
// that first receives it; all nodes order ops by (hlc, originId) so state is
// independent of delivery order. Replication: live push to peers + periodic
// anti-entropy using per-origin op logs and version vectors.

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket, Sender } = require('ws');

const NODE_ID = process.env.NODE_ID || 'node1';
const PORT = Number(process.env.PORT || 8001);
// Replication runs on a separate port that docker-compose does not publish,
// so only nodes on the internal Docker network can act as peers.
const PEER_PORT = Number(process.env.PEER_PORT || PORT + 1000);
const PEERS = (process.env.PEERS || '').split(',').map(s => s.trim()).filter(Boolean);
const DATA_DIR = process.env.DATA_DIR || '/data';
const LOG_FILE = path.join(DATA_DIR, 'ops.log');
const SYNC_MS = 500;
const MAX_BATCH = 50000;

// Fresh origin per process lifetime: seq numbers are never reused after restart.
const ORIGIN = `${NODE_ID}-${Date.now().toString(36)}`;

// ---------- CRDT state ----------
const logs = new Map();     // origin -> [op] (index = seq-1)
const opIndex = new Map();  // opId -> first op seen with that opId
const shapes = new Map();   // id -> {id,x,y,w,h,color,pts,cts,created}
const lines = new Map();    // id -> {id,points,color,width,ts}
let hlcLast = 0;
let mySeq = 0;
let clearedAt = null;       // LWW: ts of the latest clear_board; hides everything created before it

function cmpTs(a, b) {
  return (a[0] - b[0]) || (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0);
}
function newer(ts, cur) { return !cur || cmpTs(ts, cur) > 0; }

function nextHlc() {
  hlcLast = Math.max(Date.now(), hlcLast + 1);
  return hlcLast;
}

function vv() {
  const v = {};
  for (const [o, arr] of logs) v[o] = arr.length;
  return v;
}

// Applying is commutative + idempotent, so duplicates of the same opId accepted
// by two different nodes still converge (both are applied, LWW picks the max ts).
function apply(op) {
  stateVersion++;
  const ts = op.ts;
  if (op.type === 'create_shape') {
    let s = shapes.get(op.id);
    if (!s) shapes.set(op.id, s = { id: op.id, x: 0, y: 0, w: 0, h: 0, color: '', pts: null, cts: null, created: false });
    s.created = true;
    if (newer(ts, s.cts)) { s.w = op.w; s.h = op.h; s.color = op.color; s.cts = ts; }
    if (newer(ts, s.pts)) { s.x = op.x; s.y = op.y; s.pts = ts; }
  } else if (op.type === 'move_shape') {
    let s = shapes.get(op.id);
    if (!s) shapes.set(op.id, s = { id: op.id, x: 0, y: 0, w: 0, h: 0, color: '', pts: null, cts: null, created: false });
    if (newer(ts, s.pts)) { s.x = op.x; s.y = op.y; s.pts = ts; }
  } else if (op.type === 'draw_line') {
    const l = lines.get(op.id);
    if (!l || newer(ts, l.ts)) lines.set(op.id, { id: op.id, points: op.points, color: op.color, width: op.width, ts });
  } else if (op.type === 'clear_board') {
    if (newer(ts, clearedAt)) clearedAt = ts;
  }
  if (!opIndex.has(op.opId)) opIndex.set(op.opId, op);
  if (ts[0] > hlcLast) hlcLast = ts[0];
}

// Stores op in its origin log if it is the next seq. Returns true if new.
function ingest(op) {
  let arr = logs.get(op.origin);
  if (!arr) logs.set(op.origin, arr = []);
  if (op.seq !== arr.length + 1) return false; // duplicate or gap (gap filled by anti-entropy)
  arr.push(op);
  apply(op);
  return true;
}

// Serialized state cached per version: a burst of reconnects or /state calls costs one build.
let stateVersion = 0, cacheVersion = -1, cacheStateStr = '', cacheSnapMsg = '';
function stateStr() {
  if (cacheVersion !== stateVersion) {
    cacheStateStr = JSON.stringify(snapshot());
    cacheSnapMsg = '{"type":"snapshot","state":' + cacheStateStr + '}';
    cacheVersion = stateVersion;
  }
  return cacheStateStr;
}
function snapshotMsg() { stateStr(); return cacheSnapMsg; }

function snapshot() {
  const sh = [];
  for (const s of shapes.values()) {
    if (s.created && newer(s.cts, clearedAt)) sh.push({ id: s.id, x: s.x, y: s.y, w: s.w, h: s.h, color: s.color, ts: s.pts });
  }
  sh.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const ln = [];
  for (const l of lines.values()) {
    if (newer(l.ts, clearedAt)) ln.push({ id: l.id, points: l.points, color: l.color, width: l.width, ts: l.ts });
  }
  ln.sort((a, b) => cmpTs(a.ts, b.ts) || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const st = { shapes: sh, lines: ln, ops: opIndex.size };
  if (clearedAt) st.clearedAt = clearedAt;
  return st;
}

// ---------- input normalization (tolerant of protocol variants) ----------
const TYPE_ALIASES = {
  create_shape: 'create_shape', create: 'create_shape', createshape: 'create_shape', create_rect: 'create_shape',
  add_shape: 'create_shape', rect: 'create_shape', shape: 'create_shape', add: 'create_shape',
  move_shape: 'move_shape', move: 'move_shape', moveshape: 'move_shape', drag: 'move_shape', update_shape: 'move_shape',
  clear_board: 'clear_board',
  draw_line: 'draw_line', line: 'draw_line', drawline: 'draw_line', draw: 'draw_line', stroke: 'draw_line', add_line: 'draw_line',
};
const num = (v, d = 0) => (Number.isFinite(Number(v)) ? Number(v) : d);

function normalize(m) {
  if (!m || typeof m !== 'object') return null;
  const src = m.payload && typeof m.payload === 'object' ? { ...m.payload, ...m } : m;
  const type = TYPE_ALIASES[String(src.type || src.action || src.op || '').toLowerCase()];
  if (!type) return null;
  const opId = String(src.opId || src.op_id || src.actionId || src.action_id || src.msgId || crypto.randomUUID());
  const id = String(src.id || src.shapeId || src.shape_id || src.lineId || src.line_id || crypto.randomUUID());
  if (type === 'clear_board') return { type, opId, id: 'board' };
  if (type === 'create_shape') {
    return { type, opId, id, x: num(src.x), y: num(src.y), w: num(src.w ?? src.width, 100), h: num(src.h ?? src.height, 60), color: String(src.color || '#4af') };
  }
  if (type === 'move_shape') {
    return { type, opId, id, x: num(src.x), y: num(src.y) };
  }
  let points = src.points;
  if (!Array.isArray(points)) {
    points = [[num(src.x1 ?? src.fromX), num(src.y1 ?? src.fromY)], [num(src.x2 ?? src.toX), num(src.y2 ?? src.toY)]];
  }
  return { type, opId, id, points, color: String(src.color || '#222'), width: num(src.width ?? src.strokeWidth, 2) };
}

// ---------- persistence ----------
// Async append stream: writes never block the event loop.
let logStream = null;
function persist(str) {
  if (logStream) logStream.write(str + '\n');
}
function loadLog() {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch {}
  let text;
  try { text = fs.readFileSync(LOG_FILE, 'utf8'); } catch { return; }
  let n = 0;
  for (const line of text.split('\n')) {
    if (!line) continue;
    try { if (ingest(JSON.parse(line))) n++; } catch {}
  }
  console.log(`[${NODE_ID}] replayed ${n} ops from disk`);
}
function openLog() {
  logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
  logStream.on('error', e => console.error('persist failed', e.message));
}

// ---------- networking ----------
const clients = new Set();
const subscribers = new Set(); // incoming peer links: we push ops to these
const dialed = new Set();      // outgoing peer links: we pull from these (send our vv, receive ops)

// Client broadcast: frame the message once, write the same bytes to every socket.
// Sockets stay corked until the end of the tick so many frames share one syscall.
const corked = new Set();
let uncorkScheduled = false;
function uncorkAll() {
  uncorkScheduled = false;
  for (const s of corked) s.uncork();
  corked.clear();
}
function broadcast(str, except) {
  if (!clients.size) return;
  const frame = Buffer.concat(Sender.frame(Buffer.from(str), { fin: true, rsv1: false, opcode: 1, mask: false, readOnly: false }));
  for (const ws of clients) {
    if (ws === except || ws.readyState !== WebSocket.OPEN) continue;
    const sock = ws._socket;
    if (!corked.has(sock)) { sock.cork(); corked.add(sock); }
    sock.write(frame);
  }
  if (!uncorkScheduled && corked.size) { uncorkScheduled = true; setImmediate(uncorkAll); }
}

// What clients receive: the op without replication-internal fields.
function clientStr(op) {
  const { origin, seq, ...rest } = op;
  return JSON.stringify(rest);
}

// New op that was just ingested here (from a client or a peer): fan out locally.
function publishLocal(op, str) {
  broadcast(clientStr(op));
  persist(str);
}

// Each subscriber link tracks what it has already been sent per origin (ws.sent).
// A link is a single ordered TCP stream, so sending strictly from that pointer
// never creates gaps or duplicates; a new link starts again from the peer's vv.
const WIRE = Symbol("wire");
function wire(op) {
  let s = op[WIRE];
  if (!s) { s = JSON.stringify(op); Object.defineProperty(op, WIRE, { value: s }); }
  return s;
}

function sendFrom(ws, from, only) {
  if (ws.readyState !== WebSocket.OPEN) return 0;
  const out = [];
  for (const [o, arr] of logs) {
    if (only && o !== only) continue;
    const start = Math.max(from[o] || 0, ws.sent[o] || 0);
    if (arr.length > start) {
      const end = Math.min(arr.length, start + MAX_BATCH);
      for (let i = start; i < end; i++) out.push(wire(arr[i]));
      ws.sent[o] = end;
    }
  }
  if (out.length) ws.send('{"t":"ops","ops":[' + out.join(',') + ']}');
  return out.length;
}

// Live push of our own new ops, batched per tick.
let pushScheduled = false;
function schedulePush() {
  if (pushScheduled) return;
  pushScheduled = true;
  setImmediate(() => {
    pushScheduled = false;
    for (const ws of subscribers) if (ws.synced) sendFrom(ws, {}, ORIGIN);
  });
}

function onClientMessage(ws, data) {
  try { handleClientMessage(ws, data); } catch (e) { console.error('bad client message', e.message); }
}

// Rebuilds a preview from validated fields so nothing else is relayed.
function cleanPreview(m) {
  const p = { type: 'preview', kind: m.kind === 'rect' ? 'rect' : 'line', id: String(m.id).slice(0, 128), color: String(m.color || '#222').slice(0, 32) };
  if (p.kind === 'line') {
    p.from = Math.max(0, Math.floor(num(m.from)));
    p.width = num(m.width, 2);
    p.points = (Array.isArray(m.points) ? m.points.slice(0, 5000) : []).map(q => (Array.isArray(q) ? [num(q[0]), num(q[1])] : [0, 0]));
  } else {
    p.x = num(m.x); p.y = num(m.y); p.w = num(m.w); p.h = num(m.h);
  }
  return JSON.stringify(p);
}

function handleClientMessage(ws, data) {
  let m;
  try { m = JSON.parse(data); } catch { return; }
  if (m && (m.type === 'get_state' || m.type === 'snapshot')) {
    ws.send(snapshotMsg());
    return;
  }
  // Live drawing preview: relayed to everyone, never stored (not part of /state).
  if (m && m.type === 'preview') {
    const str = cleanPreview(m);
    broadcast(str, ws);
    for (const p of subscribers) if (p.readyState === WebSocket.OPEN) p.send('{"t":"preview","m":' + str + '}');
    return;
  }
  const n = normalize(m);
  if (!n) return;
  const known = opIndex.get(n.opId);
  if (known) { ws.send(clientStr(known)); return; } // resend after reconnect: just ack
  const op = { ...n, ts: [nextHlc(), ORIGIN], origin: ORIGIN, seq: ++mySeq };
  ingest(op);
  publishLocal(op, wire(op));
  schedulePush();
}

const OP_TYPES = new Set(['create_shape', 'move_shape', 'draw_line', 'clear_board']);
function validOp(op) {
  return !!op && typeof op === 'object' && OP_TYPES.has(op.type) && typeof op.origin === 'string' && Number.isInteger(op.seq)
    && typeof op.opId === 'string' && typeof op.id === 'string' && Array.isArray(op.ts) && op.ts.length === 2
    && Number.isFinite(op.ts[0]) && typeof op.ts[1] === 'string';
}

function onPeerMessage(ws, data) {
  ws.lastSeen = Date.now();
  let m;
  try { m = JSON.parse(data); } catch { return; }
  if (!m || typeof m !== 'object') return;
  try {
    if (m.t === 'preview') {
      if (m.m && typeof m.m === 'object') broadcast(cleanPreview(m.m));
    } else if (m.t === 'vv') {
      // From a peer that pulls from us: send what it lacks, then stream live ops to it.
      ws.synced = true;
      if (!sendFrom(ws, m.vv && typeof m.vv === 'object' ? m.vv : {}) && ws.readyState === WebSocket.OPEN) ws.send('{"t":"ok"}');
    } else if (m.t === 'ops' && Array.isArray(m.ops)) {
      for (const op of m.ops) {
        if (validOp(op) && ingest(op)) publishLocal(op, wire(op));
      }
    }
  } catch (e) { console.error('bad peer message', e.message); }
}

function setupPeer(ws, set) {
  ws.lastSeen = Date.now();
  ws.sent = {};
  set.add(ws);
  ws.on('message', d => onPeerMessage(ws, d));
  ws.on('close', () => set.delete(ws));
  ws.on('error', () => {});
}

function dialPeer(url) {
  const ws = new WebSocket(url, { perMessageDeflate: false });
  ws.on('open', () => {
    setupPeer(ws, dialed);
    ws.send(JSON.stringify({ t: 'vv', vv: vv() }));
  });
  ws.on('error', () => {});
  ws.on('close', () => { dialed.delete(ws); setTimeout(() => dialPeer(url), 300); });
}

// Anti-entropy: every pull link re-sends our vv; the peer answers with anything we lack
// (for all origins, so a node that was down catches up from any survivor). Also liveness.
setInterval(() => {
  const msg = JSON.stringify({ t: 'vv', vv: vv() });
  const now = Date.now();
  for (const set of [dialed, subscribers]) {
    for (const ws of set) {
      if (now - ws.lastSeen > 3000) { ws.terminate(); continue; }
      if (set === dialed && ws.readyState === WebSocket.OPEN) ws.send(msg);
    }
  }
}, SYNC_MS);

const indexHtml = path.join(__dirname, 'public', 'index.html');
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (url === '/state') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(stateStr());
  } else if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ node: NODE_ID, origin: ORIGIN, clients: clients.size, peers: dialed.size + subscribers.size, vv: vv() }));
  } else if (url === '/' || url === '/index.html') {
    fs.readFile(indexHtml, (err, buf) => {
      if (err) { res.writeHead(404); res.end('no client'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      res.end(buf);
    });
  } else {
    res.writeHead(404); res.end('not found');
  }
});

const wssClients = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 4 * 1024 * 1024 });
const wssPeers = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 512 * 1024 * 1024 });

server.on('upgrade', (req, socket, head) => {
  wssClients.handleUpgrade(req, socket, head, ws => {
    ws._socket.setNoDelay(true);
    clients.add(ws);
    ws.on('message', d => onClientMessage(ws, d));
    ws.on('close', () => clients.delete(ws));
    ws.on('error', () => {});
    ws.send(snapshotMsg());
  });
});

const peerServer = http.createServer((req, res) => { res.writeHead(404); res.end(); });
peerServer.on('upgrade', (req, socket, head) => {
  wssPeers.handleUpgrade(req, socket, head, ws => { ws._socket.setNoDelay(true); setupPeer(ws, subscribers); });
});

function shutdown() {
  // Push anything not yet sent to peers, then close clients so they fail over.
  for (const ws of subscribers) if (ws.synced) sendFrom(ws, {}, ORIGIN);
  uncorkAll();
  for (const ws of clients) ws.close(1001, 'node shutting down');
  if (logStream) logStream.end();
  setTimeout(() => process.exit(0), 150);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

loadLog();
openLog();
peerServer.listen(PEER_PORT, process.env.PEER_HOST || '0.0.0.0');
server.listen(PORT, () => {
  console.log(`[${NODE_ID}] origin=${ORIGIN} listening on ${PORT} (peers on ${PEER_PORT}), peers=${PEERS.join(',')}`);
  PEERS.forEach(dialPeer);
});
