import { Worker, type Job } from 'bullmq';
import { eq } from 'drizzle-orm';
import { getRedisConnectionOptions, getRedisClient, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, messageLogs, auditLogs } from '@rezervae-connect/database';
import { eventBus } from '@rezervae-connect/events';
import { createLogger, type SendResult } from '@rezervae-connect/shared';

const logger = createLogger('send-message-worker');

// --- Per-instance rate limiting (anti-ban) ---
// Sequential sends per WhatsApp number with human-like delays.
// Worker runs with concurrency=1 for guaranteed serialization — no race conditions.
const SEND_INTERVAL_MS = 15_000; // 15s base between msgs on same instance
const JITTER_MAX_MS = 8_000;     // 0-8s random jitter on top (total: 15-23s)
const DAILY_LIMIT = 300;         // max msgs per instance per day

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
 * Enforce minimum interval between sends on the same instance.
 * With concurrency=1 there are no race conditions — each job waits for the
 * previous one to finish before starting. The Redis timestamp is a safety net
 * that survives worker restarts.
 */
async function enforceRateLimit(instanceId: string): Promise<void> {
  const redis = getRedisClient();
  const lastKey = `send-last:${instanceId}`;

  const lastSent = await redis.get(lastKey);
  if (lastSent) {
    const elapsed = Date.now() - parseInt(lastSent, 10);
    if (elapsed < SEND_INTERVAL_MS) {
      const jitter = Math.floor(Math.random() * JITTER_MAX_MS);
      const waitMs = SEND_INTERVAL_MS - elapsed + jitter;
      logger.info({ instanceId, waitMs, elapsed }, 'Anti-ban: waiting before send');
      await sleep(waitMs);
    }
  }

  // Stamp BEFORE send — even if send fails, next message respects the interval
  await redis.set(lastKey, Date.now().toString(), 'EX', 86400);
}

async function checkDailyLimit(instanceId: string): Promise<boolean> {
  const redis = getRedisClient();
  const today = new Date().toISOString().slice(0, 10);
  const dailyKey = `daily-count:${instanceId}:${today}`;

  const count = await redis.incr(dailyKey);
  if (count === 1) await redis.expire(dailyKey, 172800); // 48h TTL

  if (count > DAILY_LIMIT) {
    logger.warn({ instanceId, count, limit: DAILY_LIMIT }, 'Daily send limit reached for instance');
    await redis.decr(dailyKey);
    return false;
  }
  return true;
}

async function processSendMessage(job: Job<SendMessageJob>): Promise<SendResult> {
  const { tenantId, instanceId, messageLogId, to, type, traceId, correlationId, sessionName } = job.data;
  const ctx = { tenantId, instanceId, messageLogId, to, type, traceId, correlationId, jobId: job.id };

  logger.info(ctx, 'Processing send-message job');

  // Guard: check if message was already sent (prevents re-send on retry)
  const [existingLog] = await db.select({ status: messageLogs.status }).from(messageLogs).where(eq(messageLogs.id, messageLogId));
  if (existingLog?.status === 'sent') {
    logger.info(ctx, 'Message already sent, skipping retry');
    return { success: true };
  }

  // Check daily limit
  const withinLimit = await checkDailyLimit(instanceId);
  if (!withinLimit) {
    await db.update(messageLogs).set({
      status: 'failed',
      error: 'Daily send limit reached (300/day)',
    }).where(eq(messageLogs.id, messageLogId));
    throw new Error('Daily send limit reached');
  }

  // Enforce anti-ban interval (concurrency=1 guarantees no parallel sends)
  await enforceRateLimit(instanceId);

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
    // WPPConnect @lid resolution error is expected — message was sent successfully
    if (error.includes('No LID for user')) {
      logger.info({ jobId: job.id, to }, 'Ignoring @lid error — message likely sent');
      result = { success: true, providerMessageId: undefined };
    } else {
      result = { success: false, error };
    }
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
}

export function createSendMessageWorker() {
  const worker = new Worker<SendMessageJob>(
    QUEUE_NAMES.SEND_MESSAGE,
    processSendMessage,
    {
      connection: getRedisConnectionOptions(),
      concurrency: 1, // Sequential processing — anti-ban without race conditions
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
