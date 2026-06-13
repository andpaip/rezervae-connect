import { createHmac } from 'node:crypto';
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

/**
 * Check if the target URL is the Core API (needs HMAC authentication).
 */
function isCoreTarget(url: string): boolean {
  const coreUrl = process.env.CORE_API_URL ?? 'http://localhost:8080';
  return url.startsWith(coreUrl);
}

/**
 * Build HMAC headers for Core API targets.
 * Same algorithm as core-api-client.ts.
 */
function buildCoreHmacHeaders(
  method: string,
  url: string,
  body: string,
  tenantId: string,
  traceId: string,
  correlationId: string,
): Record<string, string> {
  const secret = process.env.INTERNAL_SECRET ?? 'dev-secret';
  const timestamp = Date.now().toString();

  // Extract path from full URL
  const urlObj = new URL(url);
  const path = urlObj.pathname;

  const hmacInput = `${timestamp}:${method}:${path}:${body}`;
  const signature = createHmac('sha256', secret)
    .update(hmacInput)
    .digest('hex');

  logger.debug({ secret: secret.substring(0, 8) + '...', secretLen: secret.length, path, bodyLen: body.length, sigPreview: signature.substring(0, 16) }, 'HMAC debug');

  return {
    'X-Connect-Signature': signature,
    'X-Timestamp': timestamp,
    'X-Tenant-Id': tenantId,
    'X-Trace-Id': traceId,
    'X-Correlation-Id': correlationId,
  };
}

async function processWebhookDelivery(job: Job<WebhookDeliveryJob>): Promise<void> {
  const { tenantId, url, event, payload, traceId, correlationId } = job.data;
  const ctx = { tenantId, url, event, traceId, correlationId, jobId: job.id };

  logger.info(ctx, 'Delivering webhook');

  const body = JSON.stringify(payload);

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'X-Connect-Event': event,
    'X-Trace-Id': traceId,
    'X-Correlation-Id': correlationId,
  };

  // Add HMAC auth for Core API targets
  if (isCoreTarget(url)) {
    const hmacHeaders = buildCoreHmacHeaders('POST', url, body, tenantId, traceId, correlationId);
    Object.assign(headers, hmacHeaders);
  }

  const response = await fetch(url, {
    method: 'POST',
    headers,
    body,
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
