import { Worker, type Job } from 'bullmq';
import { getRedisConnectionOptions, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, auditLogs } from '@rezervae-connect/database';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('ai-processing-worker');

export interface AIProcessingJob {
  tenantId: string;
  sessionId: string;
  messageBody: string;
  customerPhone: string;
  context: Record<string, unknown>;
  traceId: string;
  correlationId: string;
}

async function processAIJob(job: Job<AIProcessingJob>): Promise<void> {
  const { tenantId, sessionId, traceId, correlationId } = job.data;
  const ctx = { tenantId, sessionId, traceId, correlationId, jobId: job.id };

  logger.info(ctx, 'Processing AI job (stub)');

  // TODO: Fase 5C — AI Tools Registry integration
  // 1. Load conversation context
  // 2. Call AI provider (Claude/OpenAI)
  // 3. Execute tool calls if any
  // 4. Enqueue response message

  await db.insert(auditLogs).values({
    tenantId,
    actor: 'worker',
    entityType: 'ai',
    action: 'processed',
    newState: { sessionId },
    metadata: { traceId, correlationId, jobId: job.id },
  });

  logger.info(ctx, 'AI processing complete (stub)');
}

export function createAIProcessingWorker() {
  const worker = new Worker<AIProcessingJob>(
    QUEUE_NAMES.AI_PROCESSING,
    processAIJob,
    {
      connection: getRedisConnectionOptions(),
      concurrency: 5,
    },
  );

  worker.on('failed', async (job, err) => {
    logger.error({ jobId: job?.id, err: err.message }, 'ai-processing job failed');

    if (job && (job.attemptsMade >= (job.opts.attempts ?? 3))) {
      const { getQueues } = await import('@rezervae-connect/queue');
      const queues = getQueues();
      await queues.aiProcessingDlq.add('dlq', {
        originalJob: job.data,
        error: err.message,
        failedAt: new Date().toISOString(),
        attempts: job.attemptsMade,
      });
    }
  });

  return worker;
}
