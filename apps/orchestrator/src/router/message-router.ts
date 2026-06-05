import { eq, and } from 'drizzle-orm';
import { db, conversationSessions, conversationMessages, conversationContext, auditLogs, messageLogs } from '@rezervae-connect/database';
import { getQueues } from '@rezervae-connect/queue';
import { eventBus } from '@rezervae-connect/events';
import { createLogger, type RawIncomingMessage, createTraceContext, coreApiPost } from '@rezervae-connect/shared';

const logger = createLogger('message-router');

interface RoutingContext {
  tenantId: string;
  sessionName: string;
  instanceId: string;
  message: RawIncomingMessage;
  traceId: string;
  correlationId: string;
}

/**
 * Central message router — the "brain" for incoming messages.
 *
 * Flow:
 * 1. Filter non-real messages
 * 2. Find or create conversation session
 * 3. Store message in conversation_messages
 * 4. Route by type:
 *    - list_response → parse rowId → call Core API
 *    - text → stub for AI/bot/human routing
 */
export async function routeIncomingMessage(ctx: RoutingContext): Promise<void> {
  const { tenantId, sessionName, instanceId, message, traceId, correlationId } = ctx;
  const logCtx = { tenantId, sessionName, from: message.from, type: message.type, traceId, correlationId };

  logger.info(logCtx, 'Routing incoming message');

  // 1. Find or create conversation session
  let [session] = await db.select().from(conversationSessions).where(
    and(
      eq(conversationSessions.tenantId, tenantId),
      eq(conversationSessions.customerPhone, message.from),
      eq(conversationSessions.state, 'open'),
    ),
  );

  if (!session) {
    [session] = await db.insert(conversationSessions).values({
      tenantId,
      channel: 'whatsapp',
      customerPhone: message.from,
      customerName: message.sender.pushname ?? null,
      instanceId,
      state: 'open',
      status: 'bot',
      lastMessageAt: new Date(),
    }).returning();

    eventBus.emit({
      type: 'conversation.created',
      tenantId, traceId, correlationId,
      timestamp: new Date().toISOString(),
      version: '1.0',
      data: { sessionId: session.id, customerPhone: message.from, channel: 'whatsapp' },
    });

    logger.info({ ...logCtx, sessionId: session.id }, 'New conversation session created');
  } else {
    await db.update(conversationSessions).set({ lastMessageAt: new Date() })
      .where(eq(conversationSessions.id, session.id));
  }

  // 2. Store message
  await db.insert(conversationMessages).values({
    sessionId: session.id,
    tenantId,
    direction: 'inbound',
    sender: message.from,
    type: message.listResponse ? 'list_response' : 'text',
    content: message.body,
    metadata: {
      senderName: message.sender.pushname,
      listResponse: message.listResponse,
      providerMessageId: message.id,
    },
    status: 'delivered',
    deliveredAt: new Date(),
  });

  // 3. Route by type
  if (message.listResponse?.singleSelectReply?.selectedRowId) {
    await handleListResponse(ctx, session.id);
  } else {
    // Forward text message to Core via event (Core decides next action)
    eventBus.emit({
      type: 'message.received',
      tenantId, traceId, correlationId,
      timestamp: new Date().toISOString(),
      version: '1.0',
      data: {
        sessionName,
        from: message.from,
        body: message.body,
        messageType: 'text',
        sessionId: session.id,
        customerPhone: message.from,
        customerName: message.sender.pushname ?? null,
        channel: 'whatsapp',
        direction: 'inbound',
        providerMessageId: message.id,
      },
    });

    logger.info(logCtx, 'Text message forwarded to Core via message.received event');
  }
}

/**
 * Handle list responses (confirmation replies).
 * Parses rowId format: "ok:{comandaId}", "ed:{comandaId}", "close:{comandaId}"
 */
async function handleListResponse(ctx: RoutingContext, sessionId: string): Promise<void> {
  const { tenantId, sessionName, instanceId, message, traceId, correlationId } = ctx;
  const selectedRowId = message.listResponse!.singleSelectReply!.selectedRowId!;
  const [action, comandaId] = selectedRowId.split(':');

  const logCtx = { tenantId, sessionName, from: message.from, action, comandaId, traceId, correlationId };
  logger.info(logCtx, 'Processing list response');

  // Map action to Core API status
  const statusMap: Record<string, { status: number; replyText: string }> = {
    ok: { status: 1, replyText: '😍 Obrigada por confirmar!' },
    ed: { status: 2, replyText: '📆 Ok, um minutinho e já te chamo para remarcarmos!' },
    close: { status: 3, replyText: '❌ Atendimento cancelado.' },
  };

  const mapped = statusMap[action];
  if (!mapped) {
    logger.warn(logCtx, 'Unknown list response action');
    await enqueueReply(ctx, instanceId, '🤔 Opção inválida.');
    return;
  }

  // Call Core API to update comanda status (HMAC authenticated)
  const coreResult = await coreApiPost(
    `/api/internal/comandas/${comandaId}/confirmation-status`,
    { status: mapped.status },
    { tenantId, traceId, correlationId },
  );

  if (!coreResult.ok) {
    logger.error({ ...logCtx, statusCode: coreResult.status }, 'Core API call failed');
  }

  // Send reply message
  await enqueueReply(ctx, instanceId, mapped.replyText);

  // Audit
  await db.insert(auditLogs).values({
    tenantId,
    actor: 'system',
    entityType: 'message',
    action: 'list_response_processed',
    newState: { action, comandaId, coreStatus: mapped.status },
    metadata: { traceId, correlationId, sessionId, from: message.from },
  });
}

/**
 * Enqueue a reply message via the send-message queue.
 */
async function enqueueReply(ctx: RoutingContext, instanceId: string, content: string): Promise<void> {
  const { tenantId, sessionName, message, traceId, correlationId } = ctx;

  // Create message log
  const [log] = await db.insert(messageLogs).values({
    tenantId,
    instanceId,
    channel: 'whatsapp',
    direction: 'outbound',
    status: 'queued',
    recipient: message.from,
    payload: { content, replyTo: message.id },
    traceId,
    correlationId,
    queuedAt: new Date(),
  }).returning();

  const queues = getQueues();
  await queues.sendMessage.add('reply', {
    tenantId,
    instanceId,
    sessionName,
    messageLogId: log.id,
    to: message.from,
    content,
    type: 'text',
    traceId,
    correlationId,
  }, {
    attempts: 3,
    backoff: { type: 'exponential', delay: 15_000 },
  });
}
