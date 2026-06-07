import { Worker, type Job } from 'bullmq';
import { eq, and } from 'drizzle-orm';
import { getRedisConnectionOptions, getRedisClient, getQueues, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, auditLogs, whatsappInstances } from '@rezervae-connect/database';
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

// Maps rowId prefixes to actions and auto-reply messages
const CONFIRMATION_ACTIONS: Record<string, { action: string; reply: string }> = {
  ok: { action: 'confirmed', reply: '😍 Obrigada por confirmar! Esperamos você amanhã!' },
  ed: { action: 'reschedule', reply: '📆 Ok! Entraremos em contato para remarcar.' },
  close: { action: 'cancelled', reply: '❌ Atendimento cancelado. Quando quiser, estamos aqui!' },
};

const DEDUP_TTL = 90_000; // 25 hours in seconds

/**
 * Check if the instance has listening enabled in metadata.
 */
async function isListeningInstance(tenantId: string, sessionName: string): Promise<boolean> {
  const [instance] = await db
    .select({ metadata: whatsappInstances.metadata })
    .from(whatsappInstances)
    .where(and(
      eq(whatsappInstances.tenantId, tenantId),
      eq(whatsappInstances.sessionName, sessionName),
    ));

  if (!instance) return false;
  const meta = instance.metadata as Record<string, unknown> | null;
  // Default to true if no metadata set (backward-compatible)
  return meta?.listening !== false;
}

/**
 * Handle list_response messages (confirmation replies from customers).
 * Parses rowId format: "ok:VISIT_UUID", "ed:VISIT_UUID", "close:VISIT_UUID"
 */
async function handleListResponse(data: IncomingMessageJob, selectedRowId: string): Promise<void> {
  const { tenantId, from, sessionName, traceId, correlationId } = data;
  const ctx = { tenantId, from, selectedRowId, traceId, correlationId };

  logger.info(ctx, 'Processing list_response');

  // Check if instance is listening
  const listening = await isListeningInstance(tenantId, sessionName);
  if (!listening) {
    logger.info(ctx, 'Instance not listening, skipping list_response');
    return;
  }

  // Parse rowId: "prefix:visitUuid"
  const colonIdx = selectedRowId.indexOf(':');
  if (colonIdx === -1) {
    logger.warn(ctx, 'Unknown rowId format, ignoring');
    return;
  }

  const prefix = selectedRowId.substring(0, colonIdx);
  const visitUuid = selectedRowId.substring(colonIdx + 1);
  const actionDef = CONFIRMATION_ACTIONS[prefix];

  if (!actionDef || !visitUuid) {
    logger.warn({ ...ctx, prefix }, 'Unknown confirmation action prefix');
    return;
  }

  // Dedup check via Redis (25h TTL — same as legacy)
  const redis = getRedisClient();
  const dedupKey = `confirmation:${visitUuid}`;
  const already = await redis.set(dedupKey, data.from, 'EX', DEDUP_TTL, 'NX');

  if (!already) {
    logger.info({ ...ctx, visitUuid }, 'Duplicate confirmation response, sending ack');
    await enqueueAutoReply(tenantId, sessionName, from, 'Essa confirmação já foi respondida anteriormente.', traceId, correlationId);
    return;
  }

  // Send auto-reply to customer
  await enqueueAutoReply(tenantId, sessionName, from, actionDef.reply, traceId, correlationId);

  // Notify Core via webhook-delivery queue (reuses existing webhooks endpoint)
  const queues = getQueues();
  const coreUrl = process.env.CORE_API_URL ?? 'http://localhost:8080';
  const eventId = `conf-${visitUuid}-${Date.now()}`;
  await queues.webhookDelivery.add('confirmation-response', {
    tenantId,
    url: `${coreUrl}/api/internal/connect/webhooks`,
    event: 'whatsapp.confirmation_response',
    payload: {
      event_id: eventId,
      event_type: 'whatsapp.confirmation_response',
      payload: {
        visit_uuid: visitUuid,
        action: actionDef.action,
        responded_at: new Date().toISOString(),
        customer_phone: from,
      },
    },
    traceId,
    correlationId,
  });

  logger.info({ ...ctx, visitUuid, action: actionDef.action }, 'Confirmation response processed');
}

/**
 * Enqueue an auto-reply text message back to the customer.
 */
async function enqueueAutoReply(
  tenantId: string,
  sessionName: string,
  to: string,
  content: string,
  traceId: string,
  correlationId: string,
): Promise<void> {
  // Resolve instanceId from sessionName
  const [instance] = await db
    .select({ id: whatsappInstances.id })
    .from(whatsappInstances)
    .where(and(
      eq(whatsappInstances.tenantId, tenantId),
      eq(whatsappInstances.sessionName, sessionName),
    ));

  if (!instance) {
    logger.warn({ tenantId, sessionName }, 'Cannot send auto-reply: instance not found');
    return;
  }

  // Create message log for the auto-reply
  const { messageLogs } = await import('@rezervae-connect/database');
  const [log] = await db.insert(messageLogs).values({
    tenantId,
    instanceId: instance.id,
    direction: 'outbound',
    recipient: to,
    status: 'queued',
    payload: { content, type: 'text', source: 'auto-reply' },
    traceId,
    correlationId,
    queuedAt: new Date(),
  }).returning();

  const queues = getQueues();
  await queues.sendMessage.add('auto-reply', {
    tenantId,
    instanceId: instance.id,
    sessionName,
    messageLogId: log.id,
    to,
    content,
    type: 'text',
    traceId,
    correlationId,
  });
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

  // --- List response handler (confirmation replies) ---
  const selectedRowId = job.data.listResponse?.singleSelectReply?.selectedRowId;
  if (selectedRowId) {
    await handleListResponse(job.data, selectedRowId);
    return;
  }

  // TODO: Fase 5A — Orchestrator will handle routing:
  // 1. Find/create conversation_session
  // 2. If text → route to AI/bot/human
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
