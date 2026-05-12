const { default: makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const express = require('express');
const qrcode = require('qrcode');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const WATCHDOG_INTERVAL_MS = 30 * 60 * 1000; // 30 min
const ALERT_AFTER_MS       = 30 * 60 * 1000; // notificar al gym si lleva 30 min offline

const app = express();
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'DELETE'],
  credentials: false,
}));
app.use(express.json());

const PORT = process.env.PORT || 8080;
const DEFAULT_SESSION = process.env.SESSION_ID || 'default';

// Estado por sesión
const qrs              = {};          // id -> dataURL
const statuses         = {};          // id -> 'disconnected' | 'connecting' | 'qr' | 'active'
const sessions         = {};          // id -> sock
const retries          = {};          // id -> number
const initializing     = new Set();   // ids en proceso de arranque
const sseClients       = {};          // id -> Set<res>
const disconnectedSince = {};         // id -> timestamp (ms) cuando se desconectó
const alertedDisconnect = new Set();  // ids a los que ya se notificó en este ciclo

// ─── helpers ────────────────────────────────────────────────────────────────

function authDir(id) {
  return path.join(__dirname, `auth_${id}`);
}

function clearAuth(id) {
  const dir = authDir(id);
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  console.log(`[${id}] auth wiped`);
}

function ensureAuth(id) {
  const dir = authDir(id);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function broadcast(id, payload) {
  const set = sseClients[id];
  if (!set || set.size === 0) return;
  const msg = `data: ${JSON.stringify(payload)}\n\n`;
  set.forEach(res => { try { res.write(msg); } catch (_) {} });
}

// ─── supabase session save ───────────────────────────────────────────────────

async function saveSessionToSupabase(id) {
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey  = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!supabaseUrl || !serviceKey) {
    console.warn(`[${id}] SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY no definidos`);
    return;
  }
  try {
    const credsPath  = path.join(authDir(id), 'creds.json');
    const creds_json = fs.existsSync(credsPath) ? fs.readFileSync(credsPath, 'utf8') : null;

    // Guardar sesión individual por gym
    const r1 = await fetch(`${supabaseUrl}/rest/v1/whatsapp_sessions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'apikey': serviceKey,
        'Authorization': `Bearer ${serviceKey}`,
        'Prefer': 'resolution=merge-duplicates',
      },
      body: JSON.stringify({ gym_id: id, creds_json, updated_at: new Date().toISOString() }),
    });
    console.log(`[${id}] whatsapp_sessions → HTTP ${r1.status}`);

    // Marcar whatsapp_connected = true en gym_settings
    const r2 = await fetch(`${supabaseUrl}/rest/v1/gym_settings?gym_id=eq.${id}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'apikey': serviceKey,
        'Authorization': `Bearer ${serviceKey}`,
      },
      body: JSON.stringify({ whatsapp_connected: true }),
    });
    console.log(`[${id}] gym_settings whatsapp_connected → HTTP ${r2.status}`);
  } catch (err) {
    console.error(`[${id}] saveSessionToSupabase error: ${err.message}`);
  }
}

// ─── core ────────────────────────────────────────────────────────────────────

async function initWA(id, wipeCreds = false) {
  // Guardia de instancia única
  if (sessions[id] || initializing.has(id)) return;
  initializing.add(id);

  if (wipeCreds) {
    clearAuth(id);
    retries[id] = 0;
  } else {
    ensureAuth(id);
  }

  statuses[id] = 'connecting';
  broadcast(id, { status: 'connecting' });

  let state, saveCreds;
  try {
    ({ state, saveCreds } = await useMultiFileAuthState(authDir(id)));
  } catch (err) {
    console.error(`[${id}] auth state corrupto: ${err.message} — limpiando y reintentando`);
    initializing.delete(id);
    clearAuth(id);
    setTimeout(() => initWA(id, false), 3000);
    return;
  }

  const sock = makeWASocket({
    auth: state,
    browser: ['Ubuntu', 'Chrome', '20.0.0'],
    printQRInTerminal: false,
    connectTimeoutMs: 60000,
    keepAliveIntervalMs: 30000,
    markOnlineOnConnect: false,
    shouldIgnoreHistory: true,
  });

  sessions[id] = sock;
  initializing.delete(id);

  sock.ev.on('creds.update', async () => {
    await saveCreds();
    if (statuses[id] === 'active') await saveSessionToSupabase(id);
  });

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      statuses[id] = 'qr';
      retries[id]  = 0;
      qrs[id] = await qrcode.toDataURL(qr);
      console.log(`[${id}] QR listo`);
      broadcast(id, { status: 'qr', qr: qrs[id] });
    }

    if (connection === 'open') {
      statuses[id] = 'active';
      retries[id]  = 0;
      delete qrs[id];
      delete disconnectedSince[id];
      alertedDisconnect.delete(id);
      console.log(`[${id}] conectado`);
      broadcast(id, { status: 'active' });
      await saveSessionToSupabase(id);
    }

    if (connection === 'close') {
      delete sessions[id];
      statuses[id] = 'disconnected';
      if (!disconnectedSince[id]) disconnectedSince[id] = Date.now();
      broadcast(id, { status: 'disconnected' });

      const code = lastDisconnect?.error?.output?.statusCode;
      const msg  = lastDisconnect?.error?.message || '';
      console.log(`[${id}] cierre: code=${code} msg=${msg}`);

      const mustWipe =
        code === DisconnectReason.loggedOut  ||
        code === DisconnectReason.badSession ||
        msg.includes('Connection Failure');

      if (mustWipe) {
        setTimeout(() => initWA(id, true), 3000);
      } else if (code === DisconnectReason.restartRequired) {
        setTimeout(() => initWA(id, false), 1000);
      } else {
        // backoff exponencial: 2s, 4s, 8s … max 60s
        const attempt = retries[id] || 0;
        const delay   = Math.min(2000 * 2 ** attempt, 60000);
        retries[id]   = attempt + 1;
        console.log(`[${id}] reintento #${retries[id]} en ${delay}ms`);
        setTimeout(() => initWA(id, false), delay);
      }
    }
  });

  sock.ws.on('error', (err) => {
    console.error(`[${id}] ws error: ${err.message}`);
  });
}

// ─── rutas ───────────────────────────────────────────────────────────────────

// SSE: el navegador se suscribe aquí para recibir eventos en tiempo real
app.get('/qr/:id/events', (req, res) => {
  const id = req.params.id;

  res.set({
    'Content-Type':  'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection':    'keep-alive',
    'X-Accel-Buffering': 'no',   // evita buffering en nginx/Railway
  });
  res.flushHeaders();

  if (!sseClients[id]) sseClients[id] = new Set();
  sseClients[id].add(res);

  // Estado actual inmediato
  const now = { status: statuses[id] || 'disconnected' };
  if (qrs[id]) now.qr = qrs[id];
  res.write(`data: ${JSON.stringify(now)}\n\n`);

  // Heartbeat cada 20s para que Railway no cierre la conexión idle
  const hb = setInterval(() => { try { res.write(': ping\n\n'); } catch (_) {} }, 20000);

  req.on('close', () => {
    clearInterval(hb);
    sseClients[id].delete(res);
    if (sseClients[id].size === 0) delete sseClients[id];
  });

  if (!sessions[id] && !initializing.has(id)) initWA(id);
});

// JSON endpoint para el backend de Next.js — long-poll hasta 10s esperando el QR
app.get('/qr/:id/data', (req, res) => {
  const id = req.params.id;
  if (!sessions[id] && !initializing.has(id)) initWA(id);

  // Si ya hay QR o está activo, responder de inmediato
  if (qrs[id] || statuses[id] === 'active') {
    const payload = { status: statuses[id] };
    if (qrs[id]) payload.qr = qrs[id];
    return res.json(payload);
  }

  // Long-poll: esperar hasta 10s a que aparezca el QR
  const WAIT_MS = 10_000;
  const TICK_MS = 300;
  let elapsed = 0;

  const poll = setInterval(() => {
    elapsed += TICK_MS;
    if (qrs[id] || statuses[id] === 'active') {
      clearInterval(poll);
      const payload = { status: statuses[id] };
      if (qrs[id]) payload.qr = qrs[id];
      return res.json(payload);
    }
    if (elapsed >= WAIT_MS) {
      clearInterval(poll);
      res.json({ status: statuses[id] || 'connecting' });
    }
  }, TICK_MS);

  req.on('close', () => clearInterval(poll));
});

// Página HTML con feedback visual en tiempo real
app.get('/qr/:id', (req, res) => {
  const id = req.params.id;
  if (!sessions[id] && !initializing.has(id)) initWA(id);

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>WhatsApp QR · ${id}</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{background:#0d1117;color:#e6edf3;font-family:system-ui,sans-serif;
         display:flex;flex-direction:column;align-items:center;
         justify-content:center;min-height:100vh;gap:1.5rem;padding:2rem}
    h1{font-size:1.1rem;opacity:.5;letter-spacing:.05em;text-transform:uppercase}
    #badge{font-size:1.4rem;font-weight:600;display:flex;align-items:center;gap:.5rem}
    #qr img{width:280px;height:280px;border-radius:12px;border:4px solid #238636}
    @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
    .pulse{animation:pulse 1.4s ease-in-out infinite}
    #log{font-size:.75rem;opacity:.35;margin-top:.5rem}
  </style>
</head>
<body>
  <h1>Sesión · <code>${id}</code></h1>
  <div id="badge"><span class="pulse">⏳</span> Iniciando...</div>
  <div id="qr"></div>
  <div id="log"></div>
  <script>
    const badge = document.getElementById('badge');
    const qrDiv = document.getElementById('qr');
    const log   = document.getElementById('log');
    const es    = new EventSource('/qr/${id}/events');

    es.onmessage = e => {
      const d = JSON.parse(e.data);
      log.textContent = new Date().toLocaleTimeString() + ' — ' + d.status;

      if (d.status === 'active') {
        badge.innerHTML = '✅ Sesión activa — puedes cerrar esta pestaña';
        qrDiv.innerHTML = '';
        es.close();
      } else if (d.status === 'qr' && d.qr) {
        badge.innerHTML = '📱 Escanea con WhatsApp';
        qrDiv.innerHTML = '<img src="' + d.qr + '" alt="QR">';
      } else if (d.status === 'connecting') {
        badge.innerHTML = '<span class="pulse">⏳</span> Conectando a WhatsApp...';
        qrDiv.innerHTML = '';
      } else {
        badge.innerHTML = '<span class="pulse">🔄</span> Reconectando...';
        qrDiv.innerHTML = '';
      }
    };

    es.onerror = () => {
      badge.innerHTML = '⚠️ Conexión perdida — recargando en 4s...';
      setTimeout(() => location.reload(), 4000);
    };
  </script>
</body>
</html>`);
});

app.get(['/session-status', '/session-status/:id'], (req, res) => {
  const id = req.params.id || DEFAULT_SESSION;
  res.json({ status: statuses[id] || 'disconnected', retries: retries[id] || 0 });
});

app.post(['/restart', '/session/:id/restart'], (req, res) => {
  const id = req.params.id || DEFAULT_SESSION;
  if (sessions[id]) { try { sessions[id].ws.close(); } catch (_) {} }
  delete sessions[id];
  initializing.delete(id);
  delete qrs[id];
  statuses[id] = 'disconnected';
  setImmediate(() => initWA(id, true));
  res.json({ ok: true });
});

app.delete('/session/:id', (req, res) => {
  const id = req.params.id;
  if (sessions[id]) { try { sessions[id].ws.close(); } catch (_) {} }
  delete sessions[id];
  initializing.delete(id);
  delete qrs[id];
  statuses[id] = 'disconnected';
  res.json({ ok: true });
});

// Soft reconnect: cierra socket y reconecta sin borrar credenciales (no pide QR)
app.post('/session/:id/reconnect', (req, res) => {
  const id = req.params.id;
  if (sessions[id]) { try { sessions[id].ws.close(); } catch (_) {} }
  delete sessions[id];
  initializing.delete(id);
  delete qrs[id];
  statuses[id] = 'disconnected';
  retries[id] = 0;
  setImmediate(() => initWA(id, false));
  res.json({ ok: true });
});

// POST /send/:id — envía un mensaje de WhatsApp desde la sesión activa
app.post('/send/:id', async (req, res) => {
  const id  = req.params.id;
  const { phone, message } = req.body;

  if (!phone || !message) return res.status(400).json({ error: 'phone y message requeridos' });

  const sock = sessions[id];
  if (!sock || statuses[id] !== 'active') {
    return res.status(503).json({ error: 'sesión no activa', status: statuses[id] || 'disconnected' });
  }

  try {
    const jid = phone.replace(/[^0-9]/g, '') + '@s.whatsapp.net';
    await sock.sendMessage(jid, { text: message });
    res.json({ ok: true });
  } catch (err) {
    console.error(`[${id}] send error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// Alias para compatibilidad con el keepalive de Next.js
app.get('/status/:id', (req, res) => {
  const id = req.params.id;
  res.json({ status: statuses[id] || 'disconnected' });
});

// Restaurar sesión desde creds_json guardado en Supabase
app.post('/session/:id/restore', async (req, res) => {
  const id = req.params.id;
  const { creds_json } = req.body;

  if (!creds_json) return res.status(400).json({ error: 'creds_json requerido' });

  if (sessions[id] && statuses[id] === 'active') {
    return res.status(409).json({ ok: true, message: 'sesión ya activa' });
  }

  try {
    ensureAuth(id);
    const credsPath = path.join(authDir(id), 'creds.json');
    const content   = typeof creds_json === 'string' ? creds_json : JSON.stringify(creds_json);
    fs.writeFileSync(credsPath, content, 'utf8');
    console.log(`[${id}] creds.json restaurado desde Supabase`);

    // Arrancar sesión con las credenciales restauradas
    if (!sessions[id] && !initializing.has(id)) {
      setImmediate(() => initWA(id, false));
    }

    res.json({ ok: true });
  } catch (err) {
    console.error(`[${id}] restore error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

app.get('/health', (_req, res) => {
  res.json({ ok: true, sessions: statuses });
});

// ─── watchdog interno ────────────────────────────────────────────────────────

async function notifyDisconnect(id) {
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey  = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!supabaseUrl || !serviceKey) return;

  try {
    // Anti-spam: no insertar si ya hay una notif sin leer en las últimas 2h
    const since = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();
    const check = await fetch(
      `${supabaseUrl}/rest/v1/notifications?gym_id=eq.${id}&type=eq.wa_disconnected&read=eq.false&created_at=gte.${since}&select=id&limit=1`,
      { headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` } }
    );
    const existing = await check.json();
    if (Array.isArray(existing) && existing.length > 0) return;

    await fetch(`${supabaseUrl}/rest/v1/notifications`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        Prefer: 'return=minimal',
      },
      body: JSON.stringify({
        gym_id: id,
        type:   'wa_disconnected',
        title:  '⚠️ Tu conexión de WhatsApp se cerró',
        body:   'Lleva más de 30 minutos sin conexión. Ingresá para escanear el QR y reconectar.',
        link:   '/dashboard/ajustes',
        read:   false,
      }),
    });
    console.log(`[watchdog][${id}] notificación wa_disconnected enviada`);
  } catch (err) {
    console.error(`[watchdog][${id}] error notificando: ${err.message}`);
  }
}

setInterval(async () => {
  const ids = Object.keys(statuses);
  if (!ids.length) return;
  console.log(`[watchdog] tick — ${ids.length} sesión(es): ${ids.map(id => `${id}:${statuses[id]}`).join(', ')}`);

  for (const id of ids) {
    const status = statuses[id];

    // Sesión activa: nada que hacer
    if (status === 'active') continue;

    // Sesión en proceso de reconexión: darle tiempo
    if (status === 'connecting' || initializing.has(id)) continue;

    // Desconectada y sin reintento en curso: forzar reconexión
    if (status === 'disconnected' && !sessions[id] && !initializing.has(id)) {
      console.log(`[watchdog][${id}] desconectado — forzando reconexión`);
      initWA(id, false);
    }

    // Si lleva más de ALERT_AFTER_MS desconectada, notificar al gym
    const since = disconnectedSince[id];
    if (since && Date.now() - since >= ALERT_AFTER_MS && !alertedDisconnect.has(id)) {
      alertedDisconnect.add(id);
      await notifyDisconnect(id);
    }
  }
}, WATCHDOG_INTERVAL_MS);

// ─────────────────────────────────────────────────────────────────────────────

app.listen(PORT, async () => {
  console.log(`Servidor en puerto ${PORT}`);

  // Restaurar todas las sesiones guardadas en Supabase al arrancar
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey  = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (supabaseUrl && serviceKey) {
    try {
      const r = await fetch(`${supabaseUrl}/rest/v1/whatsapp_sessions?select=gym_id,creds_json&creds_json=not.is.null`, {
        headers: {
          'apikey': serviceKey,
          'Authorization': `Bearer ${serviceKey}`,
        },
      });
      const rows = await r.json();
      if (Array.isArray(rows) && rows.length > 0) {
        console.log(`[boot] restaurando ${rows.length} sesión(es) desde Supabase`);
        for (const row of rows) {
          if (!row.gym_id || !row.creds_json) continue;
          try {
            ensureAuth(row.gym_id);
            const credsPath = path.join(authDir(row.gym_id), 'creds.json');
            const content   = typeof row.creds_json === 'string' ? row.creds_json : JSON.stringify(row.creds_json);
            fs.writeFileSync(credsPath, content, 'utf8');
            console.log(`[boot] creds restauradas para gym ${row.gym_id}`);
            setImmediate(() => initWA(row.gym_id, false));
          } catch (err) {
            console.error(`[boot] error restaurando ${row.gym_id}: ${err.message}`);
          }
        }
      } else {
        console.log('[boot] no hay sesiones guardadas en Supabase');
        initWA(DEFAULT_SESSION);
      }
    } catch (err) {
      console.error('[boot] error consultando Supabase:', err.message);
      initWA(DEFAULT_SESSION);
    }
  } else {
    console.warn('[boot] SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY no definidos — solo sesión default');
    initWA(DEFAULT_SESSION);
  }
});
