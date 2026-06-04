import { Worker, type Job } from 'bullmq';
import { getRedisConnectionOptions, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, auditLogs } from '@rezervae-connect/database';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('webhook-delivery-worker');

export interface WebhookDeliveryJob {
  tenantId: string;
  url: string;
  event: string;
  payload: Record<string, unknown>;
  traceId: string;
  correlationId: string;
}

async function processWebhookDelivery(job: Job<WebhookDeliveryJob>): Promise<void> {
  const { tenantId, url, event, payload, traceId, correlationId } = job.data;
  const ctx = { tenantId, url, event, traceId, correlationId, jobId: job.id };

  logger.info(ctx, 'Delivering webhook');

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Connect-Event': event,
      'X-Trace-Id': traceId,
      'X-Correlation-Id': correlationId,
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(10_000),
  });

  if (!response.ok) {
    throw new Error(`Webhook delivery failed: ${response.status} ${response.statusText}`);
  }

  await db.insert(auditLogs).values({
    tenantId,
    actor: 'worker',
    entityType: 'webhook',
    action: 'delivered',
    newState: { url, event, statusCode: response.status },
    metadata: { traceId, correlationId, jobId: job.id },
  });

  logger.info({ ...ctx, statusCode: response.status }, 'Webhook delivered');
}

export function createWebhookDeliveryWorker() {
  const worker = new Worker<WebhookDeliveryJob>(
    QUEUE_NAMES.WEBHOOK_DELIVERY,
    processWebhookDelivery,
    {
      connection: getRedisConnectionOptions(),
      concurrency: 10,
    },
  );

  worker.on('failed', (job, err) => {
    logger.error({ jobId: job?.id, err: err.message }, 'webhook-delivery job failed');
  });

  return worker;
}
