import 'dotenv/config';
import { createServer } from 'node:http';
import { createHmac, timingSafeEqual } from 'node:crypto';
import { Server } from 'socket.io';
import { Redis as IoRedis } from 'ioredis';
import { createLogger } from '@rezervae-connect/shared';
import { eventBus } from '@rezervae-connect/events';
import type {
  QRGeneratedEvent,
  InstanceConnectedEvent,
  InstanceDisconnectedEvent,
  CampaignProgressEvent,
  CampaignFinishedEvent,
  InboxMessageEvent,
  InboxMessageSentEvent,
  InboxThreadUpdatedEvent,
  ConnectEvent,
} from '@rezervae-connect/events';

const logger = createLogger('websocket');

// Connect eventBus to Redis for cross-process events
await eventBus.connectRedis();

const httpServer = createServer();

// CORS whitelist: allow known origins + localhost for dev
const allowedOrigins = (process.env.WS_CORS_ORIGINS ?? 'http://localhost:3000,http://localhost:5173')
  .split(',')
  .map((o) => o.trim());

const io = new Server(httpServer, {
  cors: { origin: allowedOrigins, methods: ['GET', 'POST'] },
});

// --- Redis for slug→tenantId resolution ---
const redisUrl = process.env.REDIS_URL ?? 'redis://localhost:6379';
const redis = new IoRedis(redisUrl, { maxRetriesPerRequest: 3, lazyConnect: true });
redis.connect().catch((err: unknown) => logger.warn({ err }, 'Redis connect failed (slug resolution disabled)'));

// --- WebSocket authentication ---
const wsSecret = process.env.WS_SECRET ?? process.env.INTERNAL_SECRET ?? 'dev-secret';

function verifyWsToken(token: string, tenantId: string): boolean {
  // Token format: HMAC-SHA256(wsSecret, tenantId)
  const expected = createHmac('sha256', wsSecret).update(tenantId).digest('hex');
  try {
    return timingSafeEqual(Buffer.from(token, 'hex'), Buffer.from(expected, 'hex'));
  } catch {
    return false;
  }
}

// --- Socket.IO connection handling ---

io.on('connection', (socket) => {
  logger.info({ socketId: socket.id }, 'Client connected');

  socket.on('join', async (tenantIdOrSlug: string, token?: string) => {
    // Resolve tenantId from slug if needed
    let tenantId: string | null = null;

    if (/^[0-9a-f-]{36}$/.test(tenantIdOrSlug)) {
      tenantId = tenantIdOrSlug;
    } else {
      try {
        tenantId = await redis.hget('tenant-slugs', tenantIdOrSlug);
      } catch (err) {
        logger.warn({ slug: tenantIdOrSlug, err }, 'Failed to resolve tenant slug from Redis');
      }
    }

    if (!tenantId) {
      logger.warn({ socketId: socket.id, slug: tenantIdOrSlug }, 'Unknown tenant');
      socket.emit('error', { message: 'Unknown tenant' });
      return;
    }

    // Verify auth token (if WS_AUTH_REQUIRED is set or in production)
    const authRequired = process.env.WS_AUTH_REQUIRED === 'true' || process.env.NODE_ENV === 'production';
    if (authRequired && !verifyWsToken(token ?? '', tenantId)) {
      logger.warn({ socketId: socket.id, tenantId }, 'WebSocket auth failed');
      socket.emit('error', { message: 'Unauthorized' });
      return;
    }

    socket.join(`tenant:${tenantId}`);
    logger.info({ socketId: socket.id, tenantId }, 'Joined tenant room');
  });

  socket.on('disconnect', () => {
    logger.info({ socketId: socket.id }, 'Client disconnected');
  });
});

// --- EventBus → Socket.IO bridge ---
// Subscribe to events and forward them to the correct tenant room.
// Event names match the frontend's current expectations.

function emitToTenant(tenantId: string, event: string, data: unknown): void {
  io.to(`tenant:${tenantId}`).emit(event, data);
}

eventBus.on('qr.generated', (event) => {
  const e = event as QRGeneratedEvent;
  emitToTenant(e.tenantId, 'qrCodeUpdate', {
    session: e.data.sessionName,
    qrCode: e.data.qrCode,
  });
});

eventBus.on('instance.connected', (event) => {
  const e = event as InstanceConnectedEvent;
  emitToTenant(e.tenantId, 'botConnected', e.data.sessionName);
  emitToTenant(e.tenantId, 'clientStatus', {
    session: e.data.sessionName,
    status: 'conectado',
  });
});

eventBus.on('instance.disconnected', (event) => {
  const e = event as InstanceDisconnectedEvent;
  emitToTenant(e.tenantId, 'clientStatus', {
    session: e.data.sessionName,
    status: 'desconectado',
    reason: e.data.reason,
  });
});

eventBus.on('campaign.progress', (event) => {
  const e = event as CampaignProgressEvent;
  emitToTenant(e.tenantId, 'campaignProgress', {
    campaignId: e.data.campaignId,
    sent: e.data.sent,
    total: e.data.total,
    errors: e.data.errors,
  });
});

eventBus.on('campaign.finished', (event) => {
  const e = event as CampaignFinishedEvent;
  emitToTenant(e.tenantId, 'campaignProgress', {
    campaignId: e.data.campaignId,
    status: 'finished',
    stats: e.data.stats,
  });
});

// Generic status messages (logs)
eventBus.on('message.sent', (event) => {
  emitToTenant(event.tenantId, 'statusMessages', {
    type: 'sent',
    data: (event as ConnectEvent & { data: unknown }).data,
  });
});

eventBus.on('message.failed', (event) => {
  emitToTenant(event.tenantId, 'statusMessages', {
    type: 'failed',
    data: (event as ConnectEvent & { data: unknown }).data,
  });
});

// --- Inbox events ---

eventBus.on('inbox.message', (event) => {
  const e = event as InboxMessageEvent;
  emitToTenant(e.tenantId, 'inbox:message', e.data);
});

eventBus.on('inbox.message.sent', (event) => {
  const e = event as InboxMessageSentEvent;
  emitToTenant(e.tenantId, 'inbox:message:sent', e.data);
});

eventBus.on('inbox.thread.updated', (event) => {
  const e = event as InboxThreadUpdatedEvent;
  emitToTenant(e.tenantId, 'inbox:thread:updated', e.data);
});

// --- Start server ---

const port = Number(process.env.WS_PORT ?? 3101);

httpServer.listen(port, () => {
  logger.info({ port }, 'Rezervae Connect WebSocket ready');
});
