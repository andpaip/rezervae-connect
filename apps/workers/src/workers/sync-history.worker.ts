import { Worker, type Job } from 'bullmq';
import { eq, and, sql } from 'drizzle-orm';
import { getRedisConnectionOptions, getRedisClient, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, conversationSessions, conversationMessages, inboxThreads, whatsappInstances } from '@rezervae-connect/database';
import { eventBus } from '@rezervae-connect/events';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('sync-history-worker');

export interface SyncHistoryJob {
  tenantId: string;
  threadId: string;
  traceId: string;
  correlationId: string;
}

async function processSyncHistory(job: Job<SyncHistoryJob>): Promise<{ synced: number }> {
  const { tenantId, threadId, traceId, correlationId } = job.data;
  const ctx = { tenantId, threadId, jobId: job.id };

  // Rate limit: max 1 sync per thread every 30s
  const redis = getRedisClient();
  const cooldownKey = `sync-cooldown:${threadId}`;
  const locked = await redis.set(cooldownKey, '1', 'EX', 30, 'NX');
  if (!locked) {
    logger.info(ctx, 'Sync skipped (cooldown active)');
    return { synced: 0 };
  }

  // 1. Load thread → session → instance
  const [thread] = await db
    .select({
      id: inboxThreads.id,
      conversationSessionId: inboxThreads.conversationSessionId,
    })
    .from(inboxThreads)
    .where(and(eq(inboxThreads.id, threadId), eq(inboxThreads.tenantId, tenantId)));

  if (!thread?.conversationSessionId) {
    logger.warn(ctx, 'Thread or session not found for sync');
    return { synced: 0 };
  }

  const [session] = await db
    .select()
    .from(conversationSessions)
    .where(eq(conversationSessions.id, thread.conversationSessionId));

  if (!session) {
    logger.warn(ctx, 'Conversation session not found');
    return { synced: 0 };
  }

  // Resolve instance with active WPP session
  const [instance] = session.instanceId
    ? await db.select().from(whatsappInstances).where(
        and(eq(whatsappInstances.id, session.instanceId), eq(whatsappInstances.status, 'connected')),
      )
    : await db.select().from(whatsappInstances).where(
        and(eq(whatsappInstances.tenantId, tenantId), eq(whatsappInstances.status, 'connected')),
      );

  if (!instance) {
    logger.info(ctx, 'No connected instance — sync skipped');
    return { synced: 0 };
  }

  // 2. Build chatId (phone@c.us or phone@lid)
  const phone = session.customerPhone;
  const chatId = /^55\d{10,11}$/.test(phone) ? `${phone}@c.us` : `${phone}@lid`;

  // 3. Fetch messages from WPP via provider
  const { getProvider } = await import('../registry.js');
  const provider = getProvider();

  if (!provider.getMessages) {
    logger.warn(ctx, 'Provider does not support getMessages');
    return { synced: 0 };
  }

  const wppMessages = await provider.getMessages(instance.sessionName, chatId, 50);
  if (wppMessages.length === 0) {
    logger.info(ctx, 'No messages from WPP');
    return { synced: 0 };
  }

  // 4. Dedup: providerMessageId + content/direction/timestamp fallback
  const existingRows = await db
    .select({
      pid: conversationMessages.providerMessageId,
      content: conversationMessages.content,
      direction: conversationMessages.direction,
      createdAt: conversationMessages.createdAt,
    })
    .from(conversationMessages)
    .where(eq(conversationMessages.sessionId, session.id));

  const existingPids = new Set(existingRows.map((r) => r.pid).filter(Boolean));

  // Build content+direction fingerprints for fallback dedup.
  // Use 10-minute buckets + adjacent bucket to handle boundary cases.
  // Worker created_at can differ from WPP timestamp by several minutes.
  const BUCKET_MS = 10 * 60_000; // 10 min
  const contentFingerprints = new Set<string>();
  for (const r of existingRows) {
    const tsMs = new Date(r.createdAt).getTime();
    const bucket = Math.floor(tsMs / BUCKET_MS);
    const snippet = (r.content ?? '').substring(0, 80);
    // Add current bucket AND adjacent ones for ±10min tolerance
    contentFingerprints.add(`${r.direction}|${bucket - 1}|${snippet}`);
    contentFingerprints.add(`${r.direction}|${bucket}|${snippet}`);
    contentFingerprints.add(`${r.direction}|${bucket + 1}|${snippet}`);
  }

  // 5. Filter new messages
  const newMessages = wppMessages.filter((m) => {
    if (!m.id) return false;
    // Primary dedup: exact providerMessageId match
    if (existingPids.has(m.id)) return false;
    // Secondary dedup: content + direction + ~10min window
    const dir = m.fromMe ? 'outbound' : 'inbound';
    const bucket = Math.floor((m.timestamp * 1000) / BUCKET_MS);
    const snippet = m.body.substring(0, 80);
    if (contentFingerprints.has(`${dir}|${bucket}|${snippet}`)) return false;
    return true;
  });

  if (newMessages.length === 0) {
    logger.info(ctx, 'All messages already in DB');
    return { synced: 0 };
  }

  // 6. Batch insert
  const values = newMessages.map((m) => ({
    sessionId: session.id,
    tenantId,
    direction: m.fromMe ? 'outbound' as const : 'inbound' as const,
    sender: m.fromMe ? 'device' : m.from,
    type: m.type === 'chat' ? 'text' : m.type,
    content: m.body,
    providerMessageId: m.id,
    status: m.fromMe ? 'sent' : 'delivered',
    sentAt: m.fromMe ? new Date(m.timestamp * 1000) : null,
    deliveredAt: m.fromMe ? null : new Date(m.timestamp * 1000),
    createdAt: new Date(m.timestamp * 1000),
  }));

  await db.insert(conversationMessages).values(values);

  logger.info({ ...ctx, synced: values.length }, 'History sync complete');

  // 7. Emit event so FE refreshes
  eventBus.emit({
    tenantId,
    traceId,
    correlationId,
    timestamp: new Date().toISOString(),
    version: '1',
    type: 'inbox.thread.updated' as const,
    data: { threadId, action: 'sync' as const },
  });

  return { synced: values.length };
}

export function createSyncHistoryWorker() {
  const worker = new Worker<SyncHistoryJob>(
    QUEUE_NAMES.SYNC_HISTORY,
    processSyncHistory,
    {
      connection: getRedisConnectionOptions(),
      concurrency: 2,
    },
  );

  worker.on('failed', (job, err) => {
    logger.error({ jobId: job?.id, err: err.message }, 'sync-history job failed');
  });

  return worker;
}
