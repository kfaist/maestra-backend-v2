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
const entityFrames = new Map(); // entityId → { buffer, ts }
const slotLocks = new Map();    // entityId → { token, lockedAt, lockedBy }
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

// ── REST: Per-entity video frames ────────────────────────────────────────────
// POST /video/frame/:entityId — any entity posts raw JPEG bytes (with slot locking)
app.post('/video/frame/:entityId', express.raw({ type: '*/*', limit: '10mb' }), (req, res) => {
  const eid = req.params.entityId;
  const incomingToken = req.headers['x-lock-token'] || null;
  const lockInfo = slotLocks.get(eid);

  // SLOT LOCKING: if slot is locked and token doesn't match, reject
  if (lockInfo && incomingToken !== lockInfo.token) {
    return res.status(403).json({
      error: 'Slot locked',
      entity_id: eid,
      locked_by: lockInfo.lockedBy,
      locked_at: lockInfo.lockedAt,
      hint: 'Send X-Lock-Token header or POST /video/unlock/:entityId to release'
    });
  }

  let buf;
  if (req.body && Buffer.isBuffer(req.body) && req.body.length > 0) {
    buf = req.body;
  } else if (req.body) {
    buf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);
  }
  if (!buf || buf.length < 100) return res.status(400).json({ error: 'Frame too small' });

  // AUTO-LOCK: if no lock exists, create one and return the token
  let token = incomingToken;
  if (!lockInfo) {
    token = require('crypto').randomBytes(16).toString('hex');
    const deviceName = req.headers['x-device-name'] || 'unknown';
    slotLocks.set(eid, { token, lockedAt: new Date().toISOString(), lockedBy: deviceName });
    console.log(`[LOCK] Slot "${eid}" locked by "${deviceName}" with token ${token.slice(0,8)}...`);
  }

  const ts = Date.now();
  entityFrames.set(eid, { buffer: buf, ts });
  // Also update the global frame for backward compat
  if (eid === 'td') { latestFrameBuffer = buf; latestFrame = null; latestFrameTs = ts; }
  broadcast({ type: 'frame', entity_id: eid, ts });
  res.json({ ok: true, entity_id: eid, ts, lock_token: lockInfo ? undefined : token });
});

// POST /video/unlock/:entityId — release a slot lock (needs token or master key)
app.post('/video/unlock/:entityId', express.json(), (req, res) => {
  const eid = req.params.entityId;
  const lockInfo = slotLocks.get(eid);
  if (!lockInfo) return res.json({ ok: true, message: 'Slot was not locked' });
  const token = req.headers['x-lock-token'] || req.body?.token;
  const masterKey = req.headers['x-master-key'] || req.body?.master_key;
  if (token === lockInfo.token || masterKey === (process.env.MASTER_KEY || 'maestra-unlock-2026')) {
    console.log(`[UNLOCK] Slot "${eid}" unlocked (was locked by "${lockInfo.lockedBy}")`);
    slotLocks.delete(eid);
    return res.json({ ok: true, unlocked: eid });
  }
  res.status(403).json({ error: 'Invalid token or master key' });
});

// GET /video/locks — show all current slot locks
app.get('/video/locks', (req, res) => {
  const locks = {};
  slotLocks.forEach((v, k) => {
    locks[k] = { locked_by: v.lockedBy, locked_at: v.lockedAt, token_prefix: v.token.slice(0, 8) + '...' };
  });
  res.json(locks);
});

// GET /video/frame/:entityId — serve latest frame for any entity
app.get('/video/frame/:entityId', (req, res) => {
  const eid = req.params.entityId;
  const entry = entityFrames.get(eid);
  if (entry && entry.buffer) {
    res.set('Content-Type', 'image/jpeg');
    res.set('Cache-Control', 'no-store');
    res.set('X-Frame-Age-Ms', String(Date.now() - entry.ts));
    return res.send(entry.buffer);
  }
  // Fallback: try global frame for any entity
  if (latestFrameBuffer) {
    res.set('Content-Type', 'image/jpeg');
    res.set('Cache-Control', 'no-store');
    return res.send(latestFrameBuffer);
  }
  res.status(404).json({ error: 'No frame for ' + eid });
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

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`maestra-backend-v2 running on port ${PORT}`);
});
