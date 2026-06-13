import { Worker, type Job } from 'bullmq';
import { eq, and, or, sql } from 'drizzle-orm';
import { getRedisConnectionOptions, getRedisClient, getQueues, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, auditLogs, whatsappInstances, conversationSessions, conversationMessages, inboxThreads } from '@rezervae-connect/database';
import { eventBus } from '@rezervae-connect/events';
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
  senderProfilePicUrl?: string;
  listResponse?: {
    singleSelectReply?: { selectedRowId?: string };
  };
  timestamp: number;
  providerMessageId: string;
  traceId: string;
  correlationId: string;
  /** True when message was sent FROM the device (outbound sync) */
  fromMe?: boolean;
  /** Media data for image/audio/video/document/sticker messages */
  media?: {
    mimetype: string;
    base64: string;
    caption?: string;
    filename?: string;
    size?: number;
    duration?: number;
  };
}

// Maps rowId prefixes to actions and auto-reply messages
const CONFIRMATION_ACTIONS: Record<string, { action: string; reply: string }> = {
  ok: { action: 'confirmed', reply: '😍 Obrigada por confirmar! Esperamos você amanhã!' },
  ed: { action: 'reschedule', reply: '📆 Ok! Entraremos em contato para remarcar.' },
  close: { action: 'cancelled', reply: '❌ Atendimento cancelado. Quando quiser, estamos aqui!' },
};

const DEDUP_TTL = 90_000; // 25 hours in seconds

/**
 * Resolve the real phone number for auto-reply.
 * WPPConnect v2 sends `from` as @lid (internal WhatsApp ID), not the phone.
 * Falls back to: (1) original outbound confirmation recipient, (2) raw `from` if it looks numeric.
 */
async function resolveReplyPhone(tenantId: string, visitUuid: string, rawFrom: string): Promise<string | null> {
  // Always try message_logs first — WPPConnect v2 `from` is a LID (internal ID),
  // not the phone number, even when it looks numeric.
  try {
    const { messageLogs } = await import('@rezervae-connect/database');
    const rows = await db
      .select({ recipient: messageLogs.recipient })
      .from(messageLogs)
      .where(and(
        eq(messageLogs.tenantId, tenantId),
        eq(messageLogs.direction, 'outbound'),
        eq(messageLogs.status, 'sent'),
        sql`${messageLogs.payload}->>'id_comanda' = ${visitUuid}`,
      ))
      .orderBy(sql`${messageLogs.createdAt} DESC`)
      .limit(1);

    if (rows[0]?.recipient) {
      logger.info({ visitUuid, phone: rows[0].recipient, rawFrom }, 'Resolved phone from original confirmation');
      return rows[0].recipient;
    }
  } catch (err) {
    logger.warn({ visitUuid, err }, 'Failed to resolve phone from message_logs');
  }

  // Fallback: if rawFrom looks like a real BR phone (starts with 55, 12-13 digits)
  const stripped = rawFrom.replace(/@.*$/, '');
  if (/^55\d{10,11}$/.test(stripped)) {
    return stripped;
  }

  logger.warn({ visitUuid, rawFrom }, 'Could not resolve real phone number');
  return null;
}

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

  // Resolve the real phone number for auto-reply.
  // WPPConnect v2 sends `from` as @lid (internal WhatsApp ID), not the phone number.
  // Look up the original outbound confirmation message to get the real phone.
  const replyTo = await resolveReplyPhone(tenantId, visitUuid, from);
  if (!replyTo) {
    logger.warn({ ...ctx, visitUuid }, 'Could not resolve phone for auto-reply, skipping reply');
  }

  // Dedup check via Redis (25h TTL — same as legacy)
  const redis = getRedisClient();
  const dedupKey = `confirmation:${visitUuid}`;
  const already = await redis.set(dedupKey, data.from, 'EX', DEDUP_TTL, 'NX');

  if (!already) {
    logger.info({ ...ctx, visitUuid }, 'Duplicate confirmation response, sending ack');
    if (replyTo) {
      await enqueueAutoReply(tenantId, sessionName, replyTo, 'Essa confirmação já foi respondida anteriormente.', traceId, correlationId);
    }
    return;
  }

  // Send auto-reply to customer
  if (replyTo) {
    await enqueueAutoReply(tenantId, sessionName, replyTo, actionDef.reply, traceId, correlationId);
  }

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

/**
 * Resolve LID → real phone number using the provider.
 */
async function resolvePhone(sessionName: string, rawPhone: string): Promise<string> {
  if (/^55\d{10,11}$/.test(rawPhone)) return rawPhone;
  try {
    const { getProvider } = await import('../registry.js');
    const provider = getProvider();
    if (provider.resolvePhone) {
      const realPhone = await provider.resolvePhone(sessionName, rawPhone);
      if (realPhone) return realPhone;
    }
  } catch { /* best effort */ }
  return rawPhone;
}

/**
 * Resolve contact name from the provider (best-effort).
 */
async function resolveContactName(sessionName: string, originalFrom: string): Promise<string | null> {
  try {
    const { getProvider } = await import('../registry.js');
    const provider = getProvider();
    if (provider.getContactName) {
      return await provider.getContactName(sessionName, `${originalFrom}@lid`);
    }
  } catch { /* best effort */ }
  return null;
}

/**
 * Find or create a conversation_session, handling the unique constraint race.
 * Both inbound and device-sent message handlers use this single function.
 */
async function resolveOrCreateSession(opts: {
  tenantId: string;
  phone: string;
  originalFrom: string;
  sessionName: string;
  customerName: string | null;
  customerPhotoUrl?: string;
}): Promise<typeof conversationSessions.$inferSelect> {
  const { tenantId, phone, originalFrom, sessionName, customerName, customerPhotoUrl } = opts;

  // 1. Try to find existing open session
  // Search by real phone first, then fall back to LID (originalFrom).
  // Never OR them — a LID can collide with another contact's phone.
  let [session] = await db
    .select()
    .from(conversationSessions)
    .where(and(
      eq(conversationSessions.tenantId, tenantId),
      eq(conversationSessions.customerPhone, phone),
      eq(conversationSessions.state, 'open'),
    ))
    .limit(1);

  // Fallback: search by original LID (only if phone was resolved from LID)
  if (!session && phone !== originalFrom) {
    [session] = await db
      .select()
      .from(conversationSessions)
      .where(and(
        eq(conversationSessions.tenantId, tenantId),
        eq(conversationSessions.customerPhone, originalFrom),
        eq(conversationSessions.state, 'open'),
      ))
      .limit(1);
  }

  if (session) {
    // Update session — fill customerName if missing, fix phone if was LID
    const updates: Record<string, unknown> = { lastMessageAt: new Date(), updatedAt: new Date() };
    if (customerName && !session.customerName) updates.customerName = customerName;
    if (customerPhotoUrl) updates.customerPhotoUrl = customerPhotoUrl;
    if (session.customerPhone !== phone && /^55\d{10,11}$/.test(phone)) {
      updates.customerPhone = phone;
    }
    // Store original LID so sync-history can use it to fetch chat history
    if (phone !== originalFrom) {
      const meta = (session.metadata ?? {}) as Record<string, unknown>;
      if (!meta.originalLid || meta.originalLid !== originalFrom) {
        updates.metadata = { ...meta, originalLid: originalFrom };
      }
    }
    await db.update(conversationSessions).set(updates).where(eq(conversationSessions.id, session.id));
    return { ...session, ...updates } as typeof session;
  }

  // 2. Create new session — catch unique constraint violation (race condition guard)
  const [inst] = await db
    .select({ id: whatsappInstances.id })
    .from(whatsappInstances)
    .where(and(
      eq(whatsappInstances.tenantId, tenantId),
      eq(whatsappInstances.sessionName, sessionName),
    ));

  try {
    [session] = await db.insert(conversationSessions).values({
      tenantId,
      customerPhone: phone,
      customerName,
      customerPhotoUrl: customerPhotoUrl ?? null,
      instanceId: inst?.id ?? null,
      state: 'open',
      status: 'bot',
      lastMessageAt: new Date(),
    }).returning();
    logger.info({ tenantId, phone, sessionId: session.id }, 'Created conversation session');
    return session;
  } catch (err: unknown) {
    // Unique constraint violation (idx_session_phone_open) → retry find
    if ((err as { code?: string }).code === '23505') {
      logger.info({ tenantId, phone }, 'Session race resolved via unique constraint');
      const [existing] = await db.select().from(conversationSessions).where(and(
        eq(conversationSessions.tenantId, tenantId),
        eq(conversationSessions.customerPhone, phone),
        eq(conversationSessions.state, 'open'),
      )).limit(1);
      if (existing) return existing;
    }
    throw err;
  }
}

/**
 * Find or create an inbox_thread for a session.
 */
async function resolveOrCreateThread(opts: {
  tenantId: string;
  sessionId: string;
  phone: string;
  customerName: string | null;
  incrementUnread: boolean;
}): Promise<typeof inboxThreads.$inferSelect> {
  const { tenantId, sessionId, phone, customerName, incrementUnread } = opts;

  let [thread] = await db
    .select()
    .from(inboxThreads)
    .where(and(
      eq(inboxThreads.tenantId, tenantId),
      eq(inboxThreads.conversationSessionId, sessionId),
      sql`${inboxThreads.status} != 'closed'`,
    ))
    .limit(1);

  if (!thread) {
    [thread] = await db.insert(inboxThreads).values({
      tenantId,
      conversationSessionId: sessionId,
      channel: 'whatsapp',
      status: 'open',
      priority: 'normal',
      unreadCount: incrementUnread ? 1 : 0,
      lastMessageAt: new Date(),
      metadata: { customerPhone: phone, customerName },
    }).returning();
    logger.info({ tenantId, threadId: thread.id, sessionId }, 'Created inbox thread');
  } else {
    const threadMeta = (thread.metadata ?? {}) as Record<string, unknown>;
    const updates: Record<string, unknown> = { lastMessageAt: new Date(), updatedAt: new Date() };
    if (incrementUnread) {
      updates.unreadCount = sql`${inboxThreads.unreadCount} + 1`;
    }
    if (customerName && !threadMeta.customerName) {
      updates.metadata = { ...threadMeta, customerPhone: phone, customerName };
    }
    await db.update(inboxThreads).set(updates).where(eq(inboxThreads.id, thread.id));
    if (incrementUnread) {
      thread = { ...thread, unreadCount: (thread.unreadCount ?? 0) + 1, lastMessageAt: new Date() };
    }
  }

  return thread;
}

/**
 * Find or create a conversation session + inbox thread for an inbound message.
 * Persists the message in conversation_messages and emits inbox:message for real-time.
 */
async function upsertInboxThread(data: IncomingMessageJob): Promise<void> {
  const { tenantId, from, body, messageType, sessionName, senderName, traceId, correlationId } = data;

  const originalFrom = from.replace(/@.*$/, '');
  const phone = await resolvePhone(sessionName, originalFrom);
  if (phone !== originalFrom) {
    logger.info({ tenantId, lid: originalFrom, realPhone: phone }, 'Resolved LID to real phone');
  }

  const session = await resolveOrCreateSession({ tenantId, phone, originalFrom, sessionName, customerName: senderName ?? null, customerPhotoUrl: data.senderProfilePicUrl });

  // 2. Persist message (with media metadata if present)
  const msgMetadata: Record<string, unknown> = {};
  if (data.media) {
    msgMetadata.mimetype = data.media.mimetype;
    msgMetadata.base64 = data.media.base64;
    if (data.media.caption) msgMetadata.caption = data.media.caption;
    if (data.media.filename) msgMetadata.filename = data.media.filename;
    if (data.media.size) msgMetadata.size = data.media.size;
    if (data.media.duration) msgMetadata.duration = data.media.duration;
  }

  const [msg] = await db.insert(conversationMessages).values({
    sessionId: session.id,
    tenantId,
    direction: 'inbound',
    sender: phone,
    type: messageType === 'chat' ? 'text' : messageType,
    content: data.media?.caption || body,
    providerMessageId: data.providerMessageId || undefined,
    status: 'delivered',
    deliveredAt: new Date(),
    metadata: Object.keys(msgMetadata).length > 0 ? msgMetadata : {},
  }).returning();

  // 3. Find or create inbox_thread
  const thread = await resolveOrCreateThread({
    tenantId, sessionId: session.id, phone, customerName: senderName ?? null, incrementUnread: true,
  });

  // 4. Emit real-time events
  const now = new Date().toISOString();
  const customerName = senderName ?? session.customerName ?? '';

  // Legacy event (existing consumers)
  eventBus.emit({
    tenantId,
    traceId,
    correlationId,
    timestamp: now,
    version: '1',
    type: 'message.received' as const,
    data: { sessionName, from: phone, body, messageType, customerPhone: phone, customerName },
  });

  // Inbox-specific event (Socket.IO → FE)
  eventBus.emit({
    tenantId,
    traceId,
    correlationId,
    timestamp: now,
    version: '1',
    type: 'inbox.message' as const,
    data: {
      threadId: thread.id,
      messageId: msg.id,
      sessionName,
      from: phone,
      body: data.media?.caption || body,
      messageType,
      customerPhone: phone,
      customerName,
      unreadCount: thread.unreadCount ?? 1,
      hasMedia: !!data.media?.base64,
    },
  });

  logger.info({ tenantId, threadId: thread.id, messageId: msg.id }, 'Inbox thread updated with new message');
}

/**
 * Persist a message sent from the physical device (fromMe=true).
 * Creates/finds session+thread like upsertInboxThread but:
 * - direction = 'outbound', sender = 'device'
 * - Does NOT increment unreadCount
 * - Emits inbox.message.sent (not inbox.message)
 */
async function persistDeviceMessage(data: IncomingMessageJob): Promise<void> {
  const { tenantId, from, body, messageType, sessionName, traceId, correlationId } = data;

  // `from` for device messages = the recipient (customer), not us
  const originalFrom = from.replace(/@.*$/, '');

  // Skip system types
  const SKIP_TYPES = ['notification_template', 'e2e_notification', 'protocol', 'ciphertext', 'revoked'];
  if (SKIP_TYPES.includes(messageType)) return;
  // Skip empty text messages (but allow media messages with empty body)
  if (!body?.trim() && !data.media) return;

  const phone = await resolvePhone(sessionName, originalFrom);

  // Resolve contact name for new sessions or sessions without a name
  const contactName = await resolveContactName(sessionName, originalFrom);

  const session = await resolveOrCreateSession({ tenantId, phone, originalFrom, sessionName, customerName: contactName, customerPhotoUrl: data.senderProfilePicUrl });

  // Dedup: skip if an outbound message with same content was persisted in the last 30s
  // (handles hub-sent messages that also trigger onAnyMessage when ID tracking misses)
  const [recentDup] = await db.select({ id: conversationMessages.id })
    .from(conversationMessages)
    .where(and(
      eq(conversationMessages.sessionId, session.id),
      eq(conversationMessages.direction, 'outbound'),
      eq(conversationMessages.content, body),
      sql`${conversationMessages.createdAt} > NOW() - INTERVAL '30 seconds'`,
    ))
    .limit(1);

  if (recentDup) {
    logger.info({ tenantId, sessionId: session.id }, 'Device message skipped (duplicate of hub-sent)');
    return;
  }

  // Persist message as outbound from device (with media metadata if present)
  const devMsgMeta: Record<string, unknown> = {};
  if (data.media) {
    devMsgMeta.mimetype = data.media.mimetype;
    devMsgMeta.base64 = data.media.base64;
    if (data.media.caption) devMsgMeta.caption = data.media.caption;
    if (data.media.filename) devMsgMeta.filename = data.media.filename;
    if (data.media.size) devMsgMeta.size = data.media.size;
    if (data.media.duration) devMsgMeta.duration = data.media.duration;
  }

  const [msg] = await db.insert(conversationMessages).values({
    sessionId: session.id,
    tenantId,
    direction: 'outbound',
    sender: 'device',
    type: messageType === 'chat' ? 'text' : messageType,
    content: data.media?.caption || body,
    providerMessageId: data.providerMessageId || undefined,
    status: 'sent',
    sentAt: new Date(),
    metadata: Object.keys(devMsgMeta).length > 0 ? devMsgMeta : {},
  }).returning();

  // Find or create inbox thread (don't increment unread for device-sent)
  const thread = await resolveOrCreateThread({
    tenantId, sessionId: session.id, phone, customerName: contactName ?? session.customerName ?? null, incrementUnread: false,
  });

  // Emit real-time event
  eventBus.emit({
    tenantId,
    traceId,
    correlationId,
    timestamp: new Date().toISOString(),
    version: '1',
    type: 'inbox.message.sent' as const,
    data: {
      threadId: thread.id,
      messageId: msg.id,
      to: phone,
      content: body,
    },
  });

  logger.info({ tenantId, threadId: thread.id, messageId: msg.id }, 'Device-sent message persisted');
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

  // --- Device-sent messages (outbound sync from phone) ---
  if (job.data.fromMe) {
    await persistDeviceMessage(job.data);
    return;
  }

  // --- Skip system notifications (not real customer messages) ---
  const SKIP_TYPES = ['notification_template', 'e2e_notification', 'protocol', 'ciphertext', 'revoked'];
  if (SKIP_TYPES.includes(messageType)) {
    logger.debug(ctx, 'Skipping system notification for inbox');
    return;
  }

  // --- Find or create conversation session + inbox thread ---
  await upsertInboxThread(job.data);
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
