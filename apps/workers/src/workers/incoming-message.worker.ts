import { Worker, type Job } from 'bullmq';
import { getRedisConnectionOptions, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, auditLogs } from '@rezervae-connect/database';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('incoming-message-worker');

export interface IncomingMessageJob {
  tenantId: string;
  sessionName: string;
  from: string;
  to: string;
  body: string;
  messageType: string;
  isGroupMsg: boolean;
  senderName?: string;
  listResponse?: {
    singleSelectReply?: { selectedRowId?: string };
  };
  timestamp: number;
  providerMessageId: string;
  traceId: string;
  correlationId: string;
}

async function processIncomingMessage(job: Job<IncomingMessageJob>): Promise<void> {
  const { tenantId, from, body, messageType, traceId, correlationId, sessionName } = job.data;
  const ctx = { tenantId, from, messageType, traceId, correlationId, jobId: job.id };

  logger.info(ctx, 'Processing incoming message');

  // Audit
  await db.insert(auditLogs).values({
    tenantId,
    actor: 'system',
    entityType: 'message',
    action: 'received',
    newState: { from, body: body.substring(0, 200), type: messageType, sessionName },
    metadata: { traceId, correlationId, jobId: job.id },
  });

  // TODO: Fase 5A — Orchestrator will handle routing:
  // 1. Find/create conversation_session
  // 2. If list_response → parse rowId → call Core API
  // 3. If text → route to AI/bot/human
  logger.info(ctx, 'Incoming message processed (routing stub)');
}

export function createIncomingMessageWorker() {
  const worker = new Worker<IncomingMessageJob>(
    QUEUE_NAMES.INCOMING_MESSAGE,
    processIncomingMessage,
    {
      connection: getRedisConnectionOptions(),
      concurrency: 10,
    },
  );

  worker.on('failed', async (job, err) => {
    logger.error({ jobId: job?.id, err: err.message }, 'incoming-message job failed');

    if (job && (job.attemptsMade >= (job.opts.attempts ?? 3))) {
      const { getQueues } = await import('@rezervae-connect/queue');
      const queues = getQueues();
      await queues.incomingMessageDlq.add('dlq', {
        originalJob: job.data,
        error: err.message,
        failedAt: new Date().toISOString(),
        attempts: job.attemptsMade,
      });
    }
  });

  worker.on('completed', (job) => {
    logger.debug({ jobId: job.id }, 'incoming-message job completed');
  });

  return worker;
}
