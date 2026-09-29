/**
 * Noit — service de notifications type ntfy
 *
 * Publier (ESP32, curl, scripts…) :
 *   POST /api/v1/publish/:topic
 *   Body JSON : { title?, message, priority?, tags? }
 *   Header optionnel : Authorization: Bearer <PUBLISH_TOKEN>
 *
 * S'abonner (app mobile) :
 *   POST /api/v1/subscribe  { topic, fcmToken, deviceName? }
 *
 * Historique :
 *   GET /api/v1/topic/:topic/messages?limit=50
 *
 * Santé :
 *   GET /health
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const admin = require('firebase-admin');

const PORT = process.env.PORT || 3000;
const PUBLISH_TOKEN = (process.env.PUBLISH_TOKEN || '').trim();

// ---------------------------------------------------------------------------
// Firebase Admin (push FCM)
// ---------------------------------------------------------------------------
let firebaseReady = false;
try {
  const projectId = process.env.FIREBASE_PROJECT_ID;
  const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
  let privateKey = process.env.FIREBASE_PRIVATE_KEY || '';
  privateKey = privateKey.replace(/^"|"$/g, '').replace(/\\n/g, '\n');

  if (projectId && clientEmail && privateKey.includes('BEGIN')) {
    admin.initializeApp({
      credential: admin.credential.cert({ projectId, clientEmail, privateKey }),
    });
    firebaseReady = true;
    console.log('[Firebase] Admin initialisé — push activé.');
  } else {
    console.warn('[Firebase] Variables manquantes — push désactivé (API OK sinon).');
  }
} catch (e) {
  console.warn('[Firebase] Init échouée :', e.message);
}

// ---------------------------------------------------------------------------
// PostgreSQL
// ---------------------------------------------------------------------------
// Connexion DB : préfère les variables séparées (évite les soucis de caractères spéciaux)
// ou DATABASE_URL si elle est déjà correcte (copie depuis le service Difa qui marche).
function buildPool() {
  const url = (process.env.DATABASE_URL || '').trim();
  const host = (process.env.DB_HOST || '').trim();
  const password = process.env.DB_PASSWORD; // ne pas trim excessif — peut contenir espaces
  const user = (process.env.DB_USER || '').trim();
  const database = (process.env.DB_NAME || 'postgres').trim();
  const port = parseInt(process.env.DB_PORT || '6543', 10);

  if (host && password != null && user) {
    console.log(`[DB] Connexion via DB_HOST=${host} user=${user} port=${port}`);
    return new Pool({
      host,
      port,
      user,
      password: String(password),
      database,
      ssl: { rejectUnauthorized: false },
    });
  }

  if (url) {
    console.log('[DB] Connexion via DATABASE_URL');
    return new Pool({
      connectionString: url,
      ssl: url.includes('localhost') ? false : { rejectUnauthorized: false },
    });
  }

  console.error('[DB] Définis DATABASE_URL  OU  DB_HOST + DB_USER + DB_PASSWORD (+ DB_PORT, DB_NAME)');
  process.exit(1);
}

const pool = buildPool();

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS topics (
      name TEXT PRIMARY KEY,
      created_at TIMESTAMPTZ DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS subscriptions (
      id UUID PRIMARY KEY,
      topic TEXT NOT NULL REFERENCES topics(name) ON DELETE CASCADE,
      fcm_token TEXT NOT NULL,
      device_name TEXT,
      created_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE(topic, fcm_token)
    );

    CREATE TABLE IF NOT EXISTS messages (
      id UUID PRIMARY KEY,
      topic TEXT NOT NULL,
      title TEXT,
      body TEXT NOT NULL,
      priority INT DEFAULT 3,
      tags TEXT,
      source TEXT,
      created_at TIMESTAMPTZ DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS idx_messages_topic_created
      ON messages(topic, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_subs_topic ON subscriptions(topic);
  `);
  console.log('[DB] Tables prêtes.');
}

// ---------------------------------------------------------------------------
// Express
// ---------------------------------------------------------------------------
const app = express();
app.set('trust proxy', 1);
app.use(cors());
app.use(express.json({ limit: '256kb' }));
app.use(express.text({ type: ['text/plain', 'text/*'], limit: '64kb' }));

const publishLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Trop de publications. Réessayez dans une minute.' },
});

function requirePublishAuth(req, res, next) {
  if (!PUBLISH_TOKEN) return next(); // mode ouvert
  const h = req.headers.authorization || '';
  const token = h.startsWith('Bearer ') ? h.slice(7) : req.headers['x-publish-token'];
  if (token !== PUBLISH_TOKEN) {
    return res.status(401).json({ error: 'Token de publication invalide.' });
  }
  next();
}

function normalizeTopic(raw) {
  const t = String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_\-./]/g, '')
    .slice(0, 64);
  return t || null;
}

async function ensureTopic(name) {
  await pool.query(
    `INSERT INTO topics (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`,
    [name],
  );
}

async function pushToTopic(topic, title, body, priority, data = {}) {
  if (!firebaseReady) {
    console.log('[Push] Firebase off — message stocké seulement.');
    return { success: 0, failure: 0 };
  }

  const { rows } = await pool.query(
    `SELECT fcm_token FROM subscriptions WHERE topic = $1`,
    [topic],
  );
  if (!rows.length) {
    console.log(`[Push] Aucun abonné sur topic="${topic}"`);
    return { success: 0, failure: 0 };
  }

  const tokens = [...new Set(rows.map((r) => r.fcm_token).filter(Boolean))];
  const prio = priority >= 4 ? 'high' : 'normal';

  try {
    const resp = await admin.messaging().sendEachForMulticast({
      tokens,
      notification: {
        title: title || topic,
        body: body,
      },
      android: {
        priority: prio,
        notification: {
          channelId: 'noit_v1',
          sound: 'default',
          priority: priority >= 4 ? 'high' : 'default',
          visibility: 'public',
        },
      },
      data: {
        topic: String(topic),
        title: String(title || topic),
        body: String(body),
        priority: String(priority),
        ...Object.fromEntries(
          Object.entries(data).map(([k, v]) => [k, String(v)]),
        ),
      },
    });

    // Nettoyer tokens invalides
    const toDelete = [];
    resp.responses.forEach((r, i) => {
      if (
        !r.success &&
        r.error &&
        (r.error.code === 'messaging/registration-token-not-registered' ||
          r.error.code === 'messaging/invalid-registration-token')
      ) {
        toDelete.push(tokens[i]);
      }
    });
    if (toDelete.length) {
      await pool.query(`DELETE FROM subscriptions WHERE fcm_token = ANY($1::text[])`, [
        toDelete,
      ]);
      console.log(`[Push] ${toDelete.length} token(s) invalide(s) supprimé(s)`);
    }

    console.log(
      `[Push] topic=${topic} success=${resp.successCount} failure=${resp.failureCount}`,
    );
    return { success: resp.successCount, failure: resp.failureCount };
  } catch (e) {
    console.error('[Push] erreur :', e.message);
    return { success: 0, failure: tokens.length };
  }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------
app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'noit',
    firebase: firebaseReady,
    publishAuth: Boolean(PUBLISH_TOKEN),
  });
});

/**
 * Publier sur un topic
 * - JSON : { title?, message|body, priority?, tags?, source? }
 * - texte brut : corps = body entier
 * Compatible style ntfy : PUT aussi accepté
 */
async function handlePublish(req, res) {
  const topic = normalizeTopic(req.params.topic);
  if (!topic) return res.status(400).json({ error: 'Topic invalide.' });

  let title = '';
  let body = '';
  let priority = 3;
  let tags = '';
  let source = req.headers['x-source'] || 'api';

  if (typeof req.body === 'string') {
    body = req.body.trim();
  } else if (req.body && typeof req.body === 'object') {
    title = (req.body.title || '').toString().slice(0, 120);
    body = (req.body.message || req.body.body || req.body.msg || '').toString();
    priority = Math.min(5, Math.max(1, parseInt(req.body.priority, 10) || 3));
    tags = (req.body.tags || '').toString().slice(0, 120);
    source = (req.body.source || source).toString().slice(0, 64);
  }

  // Headers style ntfy
  if (req.headers['x-title']) title = String(req.headers['x-title']).slice(0, 120);
  if (req.headers['x-priority']) {
    priority = Math.min(5, Math.max(1, parseInt(req.headers['x-priority'], 10) || 3));
  }
  if (req.headers['x-tags']) tags = String(req.headers['x-tags']).slice(0, 120);

  if (!body || !body.trim()) {
    return res.status(400).json({ error: 'Message vide. Envoyez "message" ou du texte brut.' });
  }
  body = body.trim().slice(0, 4000);
  if (!title) title = topic;

  await ensureTopic(topic);
  const id = uuidv4();
  await pool.query(
    `INSERT INTO messages (id, topic, title, body, priority, tags, source)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [id, topic, title, body, priority, tags || null, source],
  );

  const push = await pushToTopic(topic, title, body, priority, {
    messageId: id,
    tags: tags || '',
    source,
  });

  res.status(200).json({
    id,
    topic,
    title,
    message: body,
    priority,
    tags: tags || null,
    source,
    push,
  });
}

app.post('/api/v1/publish/:topic', publishLimiter, requirePublishAuth, handlePublish);
app.put('/api/v1/publish/:topic', publishLimiter, requirePublishAuth, handlePublish);
// Alias court type ntfy
app.post('/:topic', publishLimiter, requirePublishAuth, handlePublish);
app.put('/:topic', publishLimiter, requirePublishAuth, handlePublish);

/** S'abonner à un topic avec un token FCM */
app.post('/api/v1/subscribe', async (req, res) => {
  const topic = normalizeTopic(req.body?.topic);
  const fcmToken = (req.body?.fcmToken || req.body?.token || '').toString().trim();
  const deviceName = (req.body?.deviceName || '').toString().slice(0, 80) || null;

  if (!topic) return res.status(400).json({ error: 'Topic requis.' });
  if (!fcmToken || fcmToken.length < 20) {
    return res.status(400).json({ error: 'fcmToken invalide.' });
  }

  await ensureTopic(topic);
  const id = uuidv4();
  await pool.query(
    `INSERT INTO subscriptions (id, topic, fcm_token, device_name)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (topic, fcm_token)
     DO UPDATE SET device_name = EXCLUDED.device_name, created_at = now()`,
    [id, topic, fcmToken, deviceName],
  );

  res.json({ ok: true, topic, subscribed: true });
});

/** Se désabonner */
app.post('/api/v1/unsubscribe', async (req, res) => {
  const topic = normalizeTopic(req.body?.topic);
  const fcmToken = (req.body?.fcmToken || req.body?.token || '').toString().trim();
  if (!topic || !fcmToken) {
    return res.status(400).json({ error: 'topic et fcmToken requis.' });
  }
  await pool.query(`DELETE FROM subscriptions WHERE topic = $1 AND fcm_token = $2`, [
    topic,
    fcmToken,
  ]);
  res.json({ ok: true, topic, subscribed: false });
});

/** Liste des abonnements d'un token */
app.get('/api/v1/subscriptions', async (req, res) => {
  const fcmToken = (req.query.fcmToken || '').toString().trim();
  if (!fcmToken) return res.status(400).json({ error: 'fcmToken requis.' });
  const { rows } = await pool.query(
    `SELECT topic, device_name, created_at FROM subscriptions WHERE fcm_token = $1 ORDER BY topic`,
    [fcmToken],
  );
  res.json({ data: rows });
});

/** Historique d'un topic */
app.get('/api/v1/topic/:topic/messages', async (req, res) => {
  const topic = normalizeTopic(req.params.topic);
  if (!topic) return res.status(400).json({ error: 'Topic invalide.' });
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit, 10) || 50));
  const { rows } = await pool.query(
    `SELECT id, topic, title, body, priority, tags, source, created_at
     FROM messages WHERE topic = $1
     ORDER BY created_at DESC LIMIT $2`,
    [topic, limit],
  );
  res.json({ data: rows });
});

/** Liste des topics connus */
app.get('/api/v1/topics', async (_req, res) => {
  const { rows } = await pool.query(
    `SELECT t.name,
            (SELECT COUNT(*)::int FROM subscriptions s WHERE s.topic = t.name) AS subscribers,
            (SELECT COUNT(*)::int FROM messages m WHERE m.topic = t.name) AS messages,
            t.created_at
     FROM topics t
     ORDER BY t.name`,
  );
  res.json({ data: rows });
});

// 404 JSON
app.use((req, res) => {
  res.status(404).json({
    error: 'Route inconnue',
    hint: 'POST /api/v1/publish/:topic  ou  POST /api/v1/subscribe',
  });
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------
initDb()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Noit démarré sur le port ${PORT}`);
      console.log(`Publier : POST /api/v1/publish/<topic>`);
      console.log(`S'abonner : POST /api/v1/subscribe`);
      console.log(`Santé : GET /health`);
    });
  })
  .catch((e) => {
    console.error('[DB] Impossible de démarrer :', e.message);
    process.exit(1);
  });
