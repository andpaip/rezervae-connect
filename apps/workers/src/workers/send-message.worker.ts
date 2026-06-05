import { Worker, type Job } from 'bullmq';
import { eq } from 'drizzle-orm';
import { getRedisConnectionOptions, getRedisClient, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, messageLogs, auditLogs } from '@rezervae-connect/database';
import { eventBus } from '@rezervae-connect/events';
import { createLogger, type SendResult } from '@rezervae-connect/shared';

const logger = createLogger('send-message-worker');

// --- Per-instance rate limiting (anti-ban) ---
// Mirrors the legacy pinkmeupbot pattern: sequential sends per number with human-like delays.
// Different instances (different WhatsApp numbers) send in parallel.
const SEND_INTERVAL_MS = 12_000; // 12s base between msgs on same number (legacy uses 15s)
const JITTER_MAX_MS = 6_000;     // 0-6s random jitter on top
const DAILY_LIMIT = 300;         // max msgs per instance per day (same as legacy)

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export interface SendMessageJob {
  tenantId: string;
  instanceId: string;
  sessionName: string;
  messageLogId: string;
  to: string;
  content: string;
  type: 'text' | 'image' | 'list';
  imageUrl?: string;
  caption?: string;
  buttonText?: string;
  sections?: Array<{
    title: string;
    rows: Array<{ rowId: string; title: string; description?: string }>;
  }>;
  traceId: string;
  correlationId: string;
}

/**
 * Per-instance rate limiter.
 * Acquires a Redis lock so only 1 message sends at a time per WhatsApp number,
 * then enforces a minimum interval + random jitter between sends.
 */
async function acquireInstanceSlot(instanceId: string): Promise<void> {
  const redis = getRedisClient();
  const lockKey = `send-lock:${instanceId}`;
  const lastKey = `send-last:${instanceId}`;

  // Spin until we acquire the per-instance lock (max 2min TTL as safety)
  let acquired = false;
  for (let attempt = 0; attempt < 60; attempt++) {
    const ok = await redis.set(lockKey, '1', 'EX', 120, 'NX');
    if (ok) { acquired = true; break; }
    await sleep(2000); // Another msg is sending on this instance, wait
  }

  if (!acquired) {
    // Force-acquire after timeout (safety — lock TTL should have expired)
    await redis.set(lockKey, '1', 'EX', 120);
    logger.warn({ instanceId }, 'Force-acquired send lock after timeout');
  }

  // Enforce minimum interval since last send on this instance
  const lastSent = await redis.get(lastKey);
  if (lastSent) {
    const elapsed = Date.now() - parseInt(lastSent, 10);
    if (elapsed < SEND_INTERVAL_MS) {
      const jitter = Math.floor(Math.random() * JITTER_MAX_MS);
      const waitMs = SEND_INTERVAL_MS - elapsed + jitter;
      logger.info({ instanceId, waitMs }, 'Rate limit: waiting before send');
      await sleep(waitMs);
    }
  }
}

async function releaseInstanceSlot(instanceId: string): Promise<void> {
  const redis = getRedisClient();
  await redis.set(`send-last:${instanceId}`, Date.now().toString(), 'EX', 86400);
  await redis.del(`send-lock:${instanceId}`);
}

async function checkDailyLimit(instanceId: string): Promise<boolean> {
  const redis = getRedisClient();
  const today = new Date().toISOString().slice(0, 10);
  const dailyKey = `daily-count:${instanceId}:${today}`;

  const count = await redis.incr(dailyKey);
  if (count === 1) await redis.expire(dailyKey, 172800); // 48h TTL

  if (count > DAILY_LIMIT) {
    logger.warn({ instanceId, count, limit: DAILY_LIMIT }, 'Daily send limit reached for instance');
    // Decrement back since we won't actually send
    await redis.decr(dailyKey);
    return false;
  }
  return true;
}

async function processSendMessage(job: Job<SendMessageJob>): Promise<SendResult> {
  const { tenantId, instanceId, messageLogId, to, type, traceId, correlationId, sessionName } = job.data;
  const ctx = { tenantId, instanceId, messageLogId, to, type, traceId, correlationId, jobId: job.id };

  logger.info(ctx, 'Processing send-message job');

  // Check daily limit before acquiring lock
  const withinLimit = await checkDailyLimit(instanceId);
  if (!withinLimit) {
    await db.update(messageLogs).set({
      status: 'failed',
      error: 'Daily send limit reached (300/day)',
    }).where(eq(messageLogs.id, messageLogId));
    throw new Error('Daily send limit reached');
  }

  // Acquire per-instance slot (sequential sending per WhatsApp number)
  await acquireInstanceSlot(instanceId);

  try {
    const { getProvider } = await import('../registry.js');
    const provider = getProvider();

    let result: SendResult;

    try {
      switch (type) {
        case 'image':
          result = await provider.sendImage({
            sessionName,
            to,
            content: job.data.caption ?? '',
            imageUrl: job.data.imageUrl!,
            caption: job.data.caption ?? '',
          });
          break;
        case 'list':
          result = await provider.sendListMessage({
            sessionName,
            to,
            content: job.data.content,
            buttonText: job.data.buttonText!,
            sections: job.data.sections!,
          });
          break;
        default:
          result = await provider.sendMessage({
            sessionName,
            to,
            content: job.data.content,
          });
      }
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      result = { success: false, error };
    }

    // Update message_logs
    if (result.success) {
      await db.update(messageLogs).set({
        status: 'sent',
        providerMessageId: result.providerMessageId,
        sentAt: new Date(),
      }).where(eq(messageLogs.id, messageLogId));

      eventBus.emit({
        type: 'message.sent',
        tenantId,
        traceId,
        correlationId,
        timestamp: new Date().toISOString(),
        version: '1.0',
        data: { messageLogId, to, status: 'sent' },
      });
    } else {
      await db.update(messageLogs).set({
        status: 'failed',
        error: result.error,
      }).where(eq(messageLogs.id, messageLogId));

      eventBus.emit({
        type: 'message.failed',
        tenantId,
        traceId,
        correlationId,
        timestamp: new Date().toISOString(),
        version: '1.0',
        data: { messageLogId, to, error: result.error ?? 'unknown' },
      });
    }

    // Audit
    await db.insert(auditLogs).values({
      tenantId,
      actor: 'worker',
      entityType: 'message',
      entityId: messageLogId,
      action: result.success ? 'sent' : 'failed',
      newState: { status: result.success ? 'sent' : 'failed', providerMessageId: result.providerMessageId },
      metadata: { traceId, correlationId, jobId: job.id },
    });

    if (!result.success) {
      throw new Error(result.error ?? 'Send failed');
    }

    return result;
  } finally {
    await releaseInstanceSlot(instanceId);
  }
}

export function createSendMessageWorker() {
  const worker = new Worker<SendMessageJob>(
    QUEUE_NAMES.SEND_MESSAGE,
    processSendMessage,
    {
      connection: getRedisConnectionOptions(),
      concurrency: 10, // High global concurrency — parallel across different instances
      // No global limiter — rate limiting is per-instance via Redis locks
    },
  );

  worker.on('failed', async (job, err) => {
    logger.error({ jobId: job?.id, err: err.message }, 'send-message job failed');

    // Move to DLQ after exhausting retries
    if (job && (job.attemptsMade >= (job.opts.attempts ?? 3))) {
      const { getQueues } = await import('@rezervae-connect/queue');
      const queues = getQueues();
      await queues.sendMessageDlq.add('dlq', {
        originalJob: job.data,
        error: err.message,
        failedAt: new Date().toISOString(),
        attempts: job.attemptsMade,
      });
      logger.warn({ jobId: job.id }, 'Job moved to DLQ');
    }
  });

  worker.on('completed', (job) => {
    logger.info({ jobId: job.id }, 'send-message job completed');
  });

  return worker;
}
