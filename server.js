const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static('public'));

// ── In-memory state ──────────────────────────────────────────────────────────
const entities = new Map();   // id → entity object
const streams  = new Map();   // streamName → { data, ts }
let   latestFrame = null;       // latest frame as base64 string
let   latestFrameBuffer = null; // latest frame as raw Buffer
let   latestFrameTs = 0;

// ── Helpers ──────────────────────────────────────────────────────────────────
function broadcast(msg) {
  const str = JSON.stringify(msg);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(str); });
}

function entityToJSON(e) {
  return {
    id: e.id,
    name: e.name,
    slug: e.slug,
    entity_type_id: e.entity_type_id || 'default',
    parent_id: e.parent_id || null,
    state: e.state || {},
    description: e.description || null,
    tags: e.tags || [],
    metadata: e.metadata || {},
    device_id: e.device_id || null,
    status: e.status || 'active',
    created_at: e.created_at,
    last_seen: e.last_seen
  };
}

// ── REST: Entities ────────────────────────────────────────────────────────────
// GET /entities — list all
app.get('/entities', (req, res) => {
  res.json([...entities.values()].map(entityToJSON));
});

// POST /entities — register or update entity
app.post('/entities', (req, res) => {
  const { id, name, slug, state, metadata, tags, description } = req.body;
  const eid = id || uuidv4();
  const now = new Date().toISOString();
  const existing = entities.get(eid);
  const entity = {
    id: eid,
    name: name || slug || eid,
    slug: slug || name || eid,
    entity_type_id: req.body.entity_type_id || 'default',
    parent_id: req.body.parent_id || null,
    state: state || (existing ? existing.state : {}),
    description: description || (existing ? existing.description : null),
    tags: tags || (existing ? existing.tags : []),
    metadata: metadata || (existing ? existing.metadata : {}),
    device_id: req.body.device_id || null,
    status: 'active',
    created_at: existing ? existing.created_at : now,
    last_seen: now
  };
  entities.set(eid, entity);
  broadcast({ type: 'entity_update', entity: entityToJSON(entity) });
  res.json(entityToJSON(entity));
});

// PATCH /entities/:id/state — update state only
app.patch('/entities/:id/state', (req, res) => {
  const entity = entities.get(req.params.id);
  if (!entity) return res.status(404).json({ detail: 'Not Found' });
  entity.state = { ...entity.state, ...req.body };
  entity.last_seen = new Date().toISOString();
  entities.set(entity.id, entity);
  broadcast({ type: 'state_update', id: entity.id, state: entity.state });
  res.json(entityToJSON(entity));
});

// DELETE /entities/:id
app.delete('/entities/:id', (req, res) => {
  const existed = entities.delete(req.params.id);
  if (!existed) return res.status(404).json({ detail: 'Not Found' });
  broadcast({ type: 'entity_removed', id: req.params.id });
  res.json({ ok: true });
});

// ── REST: Streams ─────────────────────────────────────────────────────────────
app.get('/streams', (req, res) => {
  const out = {};
  streams.forEach((v, k) => { out[k] = v; });
  res.json(out);
});

app.post('/streams/:name', (req, res) => {
  streams.set(req.params.name, { data: req.body, ts: Date.now() });
  broadcast({ type: 'stream_update', name: req.params.name, data: req.body });
  res.json({ ok: true });
});

// ── REST: Video frame ─────────────────────────────────────────────────────────
// POST /video/frame/td — TD posts raw JPEG bytes or base64
// Accept raw binary (Content-Type: image/jpeg) OR JSON {frame: base64}
app.post('/video/frame/td', express.raw({ type: '*/*', limit: '10mb' }), (req, res) => {
  if (req.body && Buffer.isBuffer(req.body) && req.body.length > 0) {
    // Raw binary — store as buffer
    latestFrameBuffer = req.body;
    latestFrame = null; // clear base64 cache
  } else if (req.body && req.body.frame) {
    // JSON with base64
    latestFrame = req.body.frame;
    latestFrameBuffer = null;
  } else if (req.body) {
    // Try treating as raw buffer
    latestFrameBuffer = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);
    latestFrame = null;
  }
  latestFrameTs = Date.now();
  broadcast({ type: 'frame', ts: latestFrameTs });
  res.json({ ok: true, ts: latestFrameTs });
});

// GET /video/frame/td — proxy from original Maestra server
// TD posts to maestra-dashboard-production.up.railway.app — we relay it here
const UPSTREAM_FRAME_URL = 'https://maestra-dashboard-production.up.railway.app/video/frame/td';
app.get('/video/frame/td', async (req, res) => {
  try {
    const upstream = await fetch(`${UPSTREAM_FRAME_URL}?t=${Date.now()}`, {
      headers: { 'Cache-Control': 'no-store' }
    });
    if (!upstream.ok) {
      // Fall back to locally stored frame if upstream fails
      if (latestFrameBuffer) {
        res.set('Content-Type', 'image/jpeg');
        res.set('Cache-Control', 'no-store');
        return res.send(latestFrameBuffer);
      }
      return res.status(204).send();
    }
    const buf = Buffer.from(await upstream.arrayBuffer());
    res.set('Content-Type', upstream.headers.get('content-type') || 'image/jpeg');
    res.set('Cache-Control', 'no-store');
    res.send(buf);
  } catch (e) {
    // Fall back to locally stored frame
    if (latestFrameBuffer) {
      res.set('Content-Type', 'image/jpeg');
      res.set('Cache-Control', 'no-store');
      return res.send(latestFrameBuffer);
    }
    res.status(204).send();
  }
});

// ── REST: Audio reactivity ────────────────────────────────────────────────────
// POST /audio — accepts frequency band data, broadcasts to all WS clients
// Expected body: { sub, bass, mid, high, rms, spectral_centroid, bpm? }
let latestAudio = null;

app.post('/audio', (req, res) => {
  latestAudio = { ...req.body, ts: Date.now() };
  broadcast({ type: 'audio', data: latestAudio });
  res.json({ ok: true });
});

app.get('/audio', (req, res) => {
  if (!latestAudio) return res.status(204).send();
  if (Date.now() - latestAudio.ts > 5000) return res.status(204).send();
  res.json(latestAudio);
});

// ── REST: Heartbeat / health ──────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({ ok: true, entities: entities.size }));
app.get('/', (req, res) => res.json({ service: 'maestra-backend-v2', ok: true }));

// ── WebSocket ─────────────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  // Send current state on connect
  ws.send(JSON.stringify({
    type: 'init',
    entities: [...entities.values()].map(entityToJSON)
  }));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'heartbeat') {
      // Update entity last_seen
      if (msg.id && entities.has(msg.id)) {
        const e = entities.get(msg.id);
        e.last_seen = new Date().toISOString();
        entities.set(e.id, e);
      }
      ws.send(JSON.stringify({ type: 'heartbeat_ack', ts: Date.now() }));
    }

    if (msg.type === 'register') {
      // Entity self-registration via WS
      const now = new Date().toISOString();
      const eid = msg.id || uuidv4();
      const existing = entities.get(eid);
      const entity = {
        id: eid,
        name: msg.name || eid,
        slug: msg.slug || msg.name || eid,
        entity_type_id: msg.entity_type_id || 'default',
        parent_id: null,
        state: msg.state || (existing ? existing.state : {}),
        description: msg.description || null,
        tags: msg.tags || [],
        metadata: msg.metadata || {},
        device_id: null,
        status: 'active',
        created_at: existing ? existing.created_at : now,
        last_seen: now
      };
      entities.set(eid, entity);
      broadcast({ type: 'entity_update', entity: entityToJSON(entity) });
    }

    if (msg.type === 'state_update' && msg.id) {
      const entity = entities.get(msg.id);
      if (entity) {
        entity.state = { ...entity.state, ...msg.state };
        entity.last_seen = new Date().toISOString();
        entities.set(entity.id, entity);
        broadcast({ type: 'state_update', id: entity.id, state: entity.state });
      }
    }
  });

  ws.on('close', () => {});
});

// ── Stale entity cleanup — mark offline after 30s no heartbeat ────────────────
setInterval(() => {
  const now = Date.now();
  let changed = false;
  entities.forEach((e) => {
    const age = now - new Date(e.last_seen).getTime();
    if (age > 30000 && e.status !== 'offline') {
      e.status = 'offline';
      changed = true;
      broadcast({ type: 'entity_update', entity: entityToJSON(e) });
    } else if (age <= 30000 && e.status === 'offline') {
      e.status = 'active';
      changed = true;
      broadcast({ type: 'entity_update', entity: entityToJSON(e) });
    }
  });
}, 5000);

// ── Transcription relay — poll upstream Maestra for p5/p6 state ──────────────
const UPSTREAM_ENTITIES = 'https://maestra-dashboard-production.up.railway.app/entities';
let _lastP6 = '';
setInterval(async () => {
  try {
    const res = await fetch(UPSTREAM_ENTITIES);
    if (!res.ok) return;
    const entities_list = await res.json();
    const mirror = entities_list.find(e => e.name && e.name.includes("Mirror"));
    if (!mirror || !mirror.state) return;
    const p6 = mirror.state.p6 || '';
    const p5 = mirror.state.p5 || '';
    if (p6 && p6 !== _lastP6) {
      _lastP6 = p6;
      const nouns = p5 ? [p5] : [];
      broadcast({ type: 'transcription', text: p6, nouns, ts: Date.now() });
    }
  } catch(e) { /* silent */ }
}, 1500);

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`maestra-backend-v2 running on port ${PORT}`);
});
