import { Worker, type Job } from 'bullmq';
import { eq } from 'drizzle-orm';
import { getRedisConnectionOptions, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, messageLogs, auditLogs } from '@rezervae-connect/database';
import { eventBus } from '@rezervae-connect/events';
import { createLogger, type SendResult } from '@rezervae-connect/shared';

const logger = createLogger('send-message-worker');

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

async function processSendMessage(job: Job<SendMessageJob>): Promise<SendResult> {
  const { tenantId, messageLogId, to, type, traceId, correlationId, sessionName } = job.data;
  const ctx = { tenantId, messageLogId, to, type, traceId, correlationId, jobId: job.id };

  logger.info(ctx, 'Processing send-message job');

  // Provider will be injected at worker startup via a registry
  // For now, this worker expects a global provider reference
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
}

export function createSendMessageWorker() {
  const worker = new Worker<SendMessageJob>(
    QUEUE_NAMES.SEND_MESSAGE,
    processSendMessage,
    {
      connection: getRedisConnectionOptions(),
      concurrency: 5,
      limiter: { max: 20, duration: 60_000 },
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
