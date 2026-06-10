import type { FastifyPluginAsync } from 'fastify';
import { eq, and, desc, sql, ne } from 'drizzle-orm';
import {
  db,
  inboxThreads,
  conversationSessions,
  conversationMessages,
  whatsappInstances,
  messageLogs,
  auditLogs,
} from '@rezervae-connect/database';
import { getQueues } from '@rezervae-connect/queue';
import { eventBus } from '@rezervae-connect/events';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('inbox-route');

const inboxRoutes: FastifyPluginAsync = async (fastify) => {
  /**
   * GET /api/v1/inbox/threads
   * List inbox threads for the tenant (paginated, filterable).
   */
  fastify.get<{
    Querystring: {
      status?: string;
      priority?: string;
      assignedUserId?: string;
      limit?: string;
      offset?: string;
    };
  }>('/api/v1/inbox/threads', async (request) => {
    const { tenantId } = request.tenant;
    const { status, priority, assignedUserId, limit: limitStr, offset: offsetStr } = request.query ?? {};
    const limit = Math.min(Number(limitStr) || 50, 200);
    const offset = Number(offsetStr) || 0;

    const conditions = [eq(inboxThreads.tenantId, tenantId)];
    if (status) conditions.push(eq(inboxThreads.status, status));
    if (priority) conditions.push(eq(inboxThreads.priority, priority));
    if (assignedUserId) conditions.push(eq(inboxThreads.assignedUserId, assignedUserId));

    const rows = await db
      .select({
        id: inboxThreads.id,
        conversationSessionId: inboxThreads.conversationSessionId,
        channel: inboxThreads.channel,
        status: inboxThreads.status,
        priority: inboxThreads.priority,
        assignedUserId: inboxThreads.assignedUserId,
        lastMessageAt: inboxThreads.lastMessageAt,
        unreadCount: inboxThreads.unreadCount,
        metadata: inboxThreads.metadata,
        createdAt: inboxThreads.createdAt,
        // Join session fields
        customerPhone: conversationSessions.customerPhone,
        customerName: conversationSessions.customerName,
        sessionState: conversationSessions.state,
      })
      .from(inboxThreads)
      .leftJoin(conversationSessions, eq(inboxThreads.conversationSessionId, conversationSessions.id))
      .where(and(...conditions))
      .orderBy(desc(inboxThreads.lastMessageAt))
      .limit(limit)
      .offset(offset);

    // Get last message snippet for each thread
    const threadsWithSnippet = await Promise.all(
      rows.map(async (row) => {
        const [lastMsg] = await db
          .select({ content: conversationMessages.content, direction: conversationMessages.direction, createdAt: conversationMessages.createdAt })
          .from(conversationMessages)
          .where(eq(conversationMessages.sessionId, row.conversationSessionId!))
          .orderBy(desc(conversationMessages.createdAt))
          .limit(1);

        return {
          ...row,
          lastMessage: lastMsg
            ? { content: lastMsg.content?.substring(0, 100) ?? '', direction: lastMsg.direction, createdAt: lastMsg.createdAt }
            : null,
        };
      }),
    );

    return { data: threadsWithSnippet, meta: { limit, offset } };
  });

  /**
   * GET /api/v1/inbox/threads/:id/messages
   * Paginated message history for a thread.
   */
  fastify.get<{
    Params: { id: string };
    Querystring: { limit?: string; offset?: string };
  }>('/api/v1/inbox/threads/:id/messages', async (request, reply) => {
    const { tenantId } = request.tenant;
    const { id } = request.params;
    const limit = Math.min(Number(request.query.limit) || 50, 200);
    const offset = Number(request.query.offset) || 0;

    // Verify thread belongs to tenant
    const [thread] = await db
      .select({ id: inboxThreads.id, conversationSessionId: inboxThreads.conversationSessionId })
      .from(inboxThreads)
      .where(and(eq(inboxThreads.id, id), eq(inboxThreads.tenantId, tenantId)));

    if (!thread) {
      return reply.code(404).send({ error: 'Thread not found' });
    }

    const messages = await db
      .select({
        id: conversationMessages.id,
        direction: conversationMessages.direction,
        sender: conversationMessages.sender,
        type: conversationMessages.type,
        content: conversationMessages.content,
        status: conversationMessages.status,
        sentAt: conversationMessages.sentAt,
        deliveredAt: conversationMessages.deliveredAt,
        readAt: conversationMessages.readAt,
        createdAt: conversationMessages.createdAt,
      })
      .from(conversationMessages)
      .where(eq(conversationMessages.sessionId, thread.conversationSessionId!))
      .orderBy(desc(conversationMessages.createdAt))
      .limit(limit)
      .offset(offset);

    return { data: messages, meta: { limit, offset, threadId: id } };
  });

  /**
   * POST /api/v1/inbox/threads/:id/send
   * Send a message as operator through the thread.
   */
  fastify.post<{
    Params: { id: string };
    Body: { content: string; type?: 'text' | 'image'; imageUrl?: string };
  }>('/api/v1/inbox/threads/:id/send', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const { id } = request.params;
    const { content, type = 'text', imageUrl } = request.body ?? {};

    if (!content) {
      return reply.code(400).send({ error: 'content is required' });
    }

    // Load thread + session
    const [thread] = await db
      .select({
        id: inboxThreads.id,
        conversationSessionId: inboxThreads.conversationSessionId,
        status: inboxThreads.status,
      })
      .from(inboxThreads)
      .where(and(eq(inboxThreads.id, id), eq(inboxThreads.tenantId, tenantId)));

    if (!thread) {
      return reply.code(404).send({ error: 'Thread not found' });
    }

    const [session] = await db
      .select()
      .from(conversationSessions)
      .where(eq(conversationSessions.id, thread.conversationSessionId!));

    if (!session) {
      return reply.code(404).send({ error: 'Session not found' });
    }

    // Resolve instance
    const [instance] = session.instanceId
      ? await db.select().from(whatsappInstances).where(eq(whatsappInstances.id, session.instanceId))
      : await db.select().from(whatsappInstances).where(and(
          eq(whatsappInstances.tenantId, tenantId),
          eq(whatsappInstances.status, 'connected'),
        ));

    if (!instance) {
      return reply.code(503).send({ error: 'No connected instance available' });
    }

    // Persist outbound message
    const [msg] = await db.insert(conversationMessages).values({
      sessionId: session.id,
      tenantId,
      direction: 'outbound',
      sender: 'operator',
      type,
      content,
      status: 'queued',
    }).returning();

    // Create message log + enqueue
    const [log] = await db.insert(messageLogs).values({
      tenantId,
      instanceId: instance.id,
      direction: 'outbound',
      recipient: session.customerPhone,
      status: 'queued',
      payload: { content, type, source: 'inbox', threadId: id, conversationMessageId: msg.id },
      traceId,
      correlationId,
      queuedAt: new Date(),
    }).returning();

    const queues = getQueues();
    await queues.sendMessage.add('inbox-send', {
      tenantId,
      instanceId: instance.id,
      sessionName: instance.sessionName,
      messageLogId: log.id,
      to: session.customerPhone,
      content,
      type,
      imageUrl,
      traceId,
      correlationId,
    });

    // Update thread lastMessageAt
    await db.update(inboxThreads)
      .set({ lastMessageAt: new Date(), updatedAt: new Date() })
      .where(eq(inboxThreads.id, id));

    // Emit real-time event
    eventBus.emit({
      tenantId,
      traceId,
      correlationId,
      timestamp: new Date().toISOString(),
      version: '1',
      type: 'inbox.message.sent' as const,
      data: {
        threadId: id,
        messageId: msg.id,
        to: session.customerPhone,
        content,
      },
    });

    logger.info({ tenantId, threadId: id, messageId: msg.id }, 'Operator message sent via inbox');

    return reply.code(202).send({
      message: 'Message queued',
      messageId: msg.id,
      messageLogId: log.id,
    });
  });

  /**
   * POST /api/v1/inbox/threads/:id/claim
   * Claim a thread (assign to current operator).
   */
  fastify.post<{
    Params: { id: string };
    Body: { userId: string };
  }>('/api/v1/inbox/threads/:id/claim', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const { id } = request.params;
    const { userId } = request.body ?? {};

    if (!userId) {
      return reply.code(400).send({ error: 'userId is required' });
    }

    const [thread] = await db
      .select()
      .from(inboxThreads)
      .where(and(eq(inboxThreads.id, id), eq(inboxThreads.tenantId, tenantId)));

    if (!thread) {
      return reply.code(404).send({ error: 'Thread not found' });
    }

    // Check if already claimed by someone else
    if (thread.assignedUserId && thread.assignedUserId !== userId) {
      return reply.code(409).send({ error: 'Thread already claimed', assignedTo: thread.assignedUserId });
    }

    await db.update(inboxThreads)
      .set({ assignedUserId: userId, status: 'assigned', updatedAt: new Date() })
      .where(eq(inboxThreads.id, id));

    await db.insert(auditLogs).values({
      tenantId,
      actor: userId,
      entityType: 'inbox_thread',
      action: 'claimed',
      entityId: id,
      newState: { assignedUserId: userId },
      metadata: { traceId, correlationId },
    });

    eventBus.emit({
      tenantId,
      traceId,
      correlationId,
      timestamp: new Date().toISOString(),
      version: '1',
      type: 'inbox.thread.updated' as const,
      data: { threadId: id, status: 'assigned', assignedUserId: userId, action: 'claimed' as const },
    });

    logger.info({ tenantId, threadId: id, userId }, 'Thread claimed');
    return { success: true, status: 'assigned', assignedUserId: userId };
  });

  /**
   * POST /api/v1/inbox/threads/:id/release
   * Release a claimed thread.
   */
  fastify.post<{
    Params: { id: string };
    Body: { userId: string };
  }>('/api/v1/inbox/threads/:id/release', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const { id } = request.params;
    const { userId } = request.body ?? {};

    const [thread] = await db
      .select()
      .from(inboxThreads)
      .where(and(eq(inboxThreads.id, id), eq(inboxThreads.tenantId, tenantId)));

    if (!thread) {
      return reply.code(404).send({ error: 'Thread not found' });
    }

    await db.update(inboxThreads)
      .set({ assignedUserId: null, status: 'open', updatedAt: new Date() })
      .where(eq(inboxThreads.id, id));

    await db.insert(auditLogs).values({
      tenantId,
      actor: userId ?? 'system',
      entityType: 'inbox_thread',
      action: 'released',
      entityId: id,
      newState: { assignedUserId: null, status: 'open' },
      metadata: { traceId, correlationId },
    });

    eventBus.emit({
      tenantId,
      traceId,
      correlationId,
      timestamp: new Date().toISOString(),
      version: '1',
      type: 'inbox.thread.updated' as const,
      data: { threadId: id, status: 'open', assignedUserId: null, action: 'released' as const },
    });

    logger.info({ tenantId, threadId: id }, 'Thread released');
    return { success: true, status: 'open' };
  });

  /**
   * POST /api/v1/inbox/threads/:id/close
   * Close/resolve a thread.
   */
  fastify.post<{
    Params: { id: string };
    Body: { userId?: string; reason?: string };
  }>('/api/v1/inbox/threads/:id/close', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const { id } = request.params;
    const { userId, reason } = request.body ?? {};

    const [thread] = await db
      .select()
      .from(inboxThreads)
      .where(and(eq(inboxThreads.id, id), eq(inboxThreads.tenantId, tenantId)));

    if (!thread) {
      return reply.code(404).send({ error: 'Thread not found' });
    }

    await db.update(inboxThreads)
      .set({ status: 'closed', unreadCount: 0, updatedAt: new Date() })
      .where(eq(inboxThreads.id, id));

    // Also close the conversation session
    if (thread.conversationSessionId) {
      await db.update(conversationSessions)
        .set({ state: 'closed', updatedAt: new Date() })
        .where(eq(conversationSessions.id, thread.conversationSessionId));
    }

    await db.insert(auditLogs).values({
      tenantId,
      actor: userId ?? 'system',
      entityType: 'inbox_thread',
      action: 'closed',
      entityId: id,
      newState: { status: 'closed', reason },
      metadata: { traceId, correlationId },
    });

    eventBus.emit({
      tenantId,
      traceId,
      correlationId,
      timestamp: new Date().toISOString(),
      version: '1',
      type: 'inbox.thread.updated' as const,
      data: { threadId: id, status: 'closed', action: 'closed' as const },
    });

    logger.info({ tenantId, threadId: id, reason }, 'Thread closed');
    return { success: true, status: 'closed' };
  });
};

export default inboxRoutes;
