import { Worker, type Job } from 'bullmq';
import { getRedisConnectionOptions, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, auditLogs } from '@rezervae-connect/database';
import { createLogger } from '@rezervae-connect/shared';
import { processAutomation, type CoreEventJob } from '../automation-engine.js';

const logger = createLogger('core-events-worker');

async function processCoreEvent(job: Job<CoreEventJob>): Promise<void> {
  const { eventType, eventId, tenantId, traceId, correlationId } = job.data;
  const ctx = { eventType, eventId, tenantId, traceId, correlationId, jobId: job.id };

  logger.info(ctx, 'Processing core event');

  const results = await processAutomation(job.data);

  const matched = results.filter((r) => r.matched).length;
  const sent = results.filter((r) => r.sent).length;
  const skipped = results.filter((r) => r.matched && !r.sent);

  if (skipped.length > 0) {
    logger.info({
      ...ctx,
      skipped: skipped.map((s) => ({ ruleId: s.ruleId, reason: s.reason })),
    }, 'Some rules skipped');
  }

  // Audit
  await db.insert(auditLogs).values({
    tenantId,
    actor: 'worker',
    entityType: 'automation',
    entityId: eventId,
    action: 'core_event_processed',
    newState: { eventType, rulesMatched: matched, messagesSent: sent },
    metadata: { traceId, correlationId, jobId: job.id },
  });

  logger.info({ ...ctx, rulesMatched: matched, messagesSent: sent }, 'Core event processed');
}

export function createCoreEventsWorker() {
  const worker = new Worker<CoreEventJob>(
    QUEUE_NAMES.CORE_EVENTS,
    processCoreEvent,
    {
      connection: getRedisConnectionOptions(),
      concurrency: 5,
    },
  );

  worker.on('failed', (job, err) => {
    logger.error({ jobId: job?.id, err: err.message }, 'core-events job failed');
  });

  worker.on('completed', (job) => {
    logger.info({ jobId: job.id }, 'core-events job completed');
  });

  return worker;
}
