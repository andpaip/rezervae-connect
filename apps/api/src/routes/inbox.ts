import type { FastifyPluginAsync } from 'fastify';
import { eq, and, or, desc, sql, ne, inArray, isNull } from 'drizzle-orm';
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
        customerPhotoUrl: conversationSessions.customerPhotoUrl,
        sessionState: conversationSessions.state,
      })
      .from(inboxThreads)
      .leftJoin(conversationSessions, eq(inboxThreads.conversationSessionId, conversationSessions.id))
      .leftJoin(whatsappInstances, eq(conversationSessions.instanceId, whatsappInstances.id))
      .where(and(
        ...conditions,
        or(isNull(conversationSessions.instanceId), eq(whatsappInstances.status, 'connected')),
      ))
      .orderBy(desc(inboxThreads.lastMessageAt))
      .limit(limit)
      .offset(offset);

    // Batch fetch last message per session using lateral join (1 row per session, indexed)
    const sessionIds = rows.map((r) => r.conversationSessionId).filter(Boolean) as string[];
    const lastMsgMap = new Map<string, { content: string | null; type: string | null; direction: string; createdAt: Date | null }>();

    if (sessionIds.length > 0) {
      // DISTINCT ON guarantees exactly 1 row per session (the latest message)
      const idList = sql.join(sessionIds.map(id => sql`${id}`), sql`, `);
      const lastMsgs: { session_id: string; content: string | null; type: string | null; direction: string; created_at: string | null }[] =
        await db.execute(sql`
          SELECT DISTINCT ON (session_id)
            session_id, content, type, direction, created_at
          FROM conversation_messages
          WHERE session_id IN (${idList})
          ORDER BY session_id, created_at DESC
        `);

      for (const m of lastMsgs) {
        lastMsgMap.set(m.session_id, {
          content: m.content,
          type: m.type,
          direction: m.direction,
          createdAt: m.created_at ? new Date(m.created_at) : null,
        });
      }
    }

    const threadsWithSnippet = rows.map((row) => {
      const lastMsg = row.conversationSessionId ? lastMsgMap.get(row.conversationSessionId) : null;
      return {
        ...row,
        lastMessage: lastMsg
          ? { content: lastMsg.content?.substring(0, 100) ?? '', type: lastMsg.type, direction: lastMsg.direction, createdAt: lastMsg.createdAt }
          : null,
      };
    });

    return { data: threadsWithSnippet, meta: { limit, offset } };
  });

  /**
   * GET /api/v1/inbox/search?q=termo
   * Full-text search across all messages, customer names and phones.
   */
  fastify.get<{
    Querystring: { q: string; limit?: string };
  }>('/api/v1/inbox/search', async (request) => {
    const { tenantId } = request.tenant;
    const q = request.query.q?.trim();
    if (!q || q.length < 2) return { data: [] };
    const limit = Math.min(Number(request.query.limit) || 20, 50);
    const pattern = `%${q}%`;

    // 1. Search messages by content
    const msgMatches = await db
      .select({
        sessionId: conversationMessages.sessionId,
        content: conversationMessages.content,
        type: conversationMessages.type,
        direction: conversationMessages.direction,
        createdAt: conversationMessages.createdAt,
      })
      .from(conversationMessages)
      .where(and(
        eq(conversationMessages.tenantId, tenantId),
        sql`${conversationMessages.content} ILIKE ${pattern}`,
      ))
      .orderBy(desc(conversationMessages.createdAt))
      .limit(limit * 3);

    // 2. Search sessions by name or phone
    const sessionMatches = await db
      .select({ id: conversationSessions.id })
      .from(conversationSessions)
      .where(and(
        eq(conversationSessions.tenantId, tenantId),
        sql`(${conversationSessions.customerName} ILIKE ${pattern} OR ${conversationSessions.customerPhone} ILIKE ${pattern})`,
      ))
      .limit(limit);

    // Combine unique session IDs
    const sessionIdSet = new Set<string>();
    for (const m of msgMatches) if (m.sessionId) sessionIdSet.add(m.sessionId);
    for (const s of sessionMatches) sessionIdSet.add(s.id);
    const sessionIds = [...sessionIdSet];
    if (sessionIds.length === 0) return { data: [] };

    // 3. Fetch threads + session info for matched sessions
    const threads = await db
      .select({
        threadId: inboxThreads.id,
        conversationSessionId: inboxThreads.conversationSessionId,
        status: inboxThreads.status,
        unreadCount: inboxThreads.unreadCount,
        lastMessageAt: inboxThreads.lastMessageAt,
        customerPhone: conversationSessions.customerPhone,
        customerName: conversationSessions.customerName,
      })
      .from(inboxThreads)
      .leftJoin(conversationSessions, eq(inboxThreads.conversationSessionId, conversationSessions.id))
      .where(and(
        eq(inboxThreads.tenantId, tenantId),
        inArray(inboxThreads.conversationSessionId!, sessionIds),
      ));

    // Build snippet map: sessionId → best matching snippet
    const snippetMap = new Map<string, { content: string; type: string; direction: string; createdAt: Date | null }>();
    for (const m of msgMatches) {
      if (m.sessionId && !snippetMap.has(m.sessionId)) {
        snippetMap.set(m.sessionId, { content: m.content?.substring(0, 120) ?? '', type: m.type, direction: m.direction, createdAt: m.createdAt });
      }
    }

    // 4. Assemble results (dedup by threadId)
    const seen = new Set<string>();
    const results = threads
      .filter((t) => { if (seen.has(t.threadId)) return false; seen.add(t.threadId); return true; })
      .map((t) => {
        const snippet = t.conversationSessionId ? snippetMap.get(t.conversationSessionId) : null;
        return {
          threadId: t.threadId,
          customerPhone: t.customerPhone,
          customerName: t.customerName,
          status: t.status,
          unreadCount: t.unreadCount,
          lastMessageAt: t.lastMessageAt,
          matchSnippet: snippet?.content ?? '',
          matchType: snippet?.type ?? 'text',
          matchDirection: snippet?.direction ?? 'inbound',
          matchCreatedAt: snippet?.createdAt,
        };
      })
      .slice(0, limit);

    return { data: results };
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
    const limit = Math.min(Number(request.query.limit) || 50, 500);
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
        metadata: conversationMessages.metadata,
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

    if (!content && type !== 'image') {
      return reply.code(400).send({ error: 'content is required' });
    }
    if (type === 'image' && !imageUrl) {
      return reply.code(400).send({ error: 'imageUrl is required for image messages' });
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

    // Persist message + log + update thread in a single transaction
    const { msg, log } = await db.transaction(async (tx) => {
      const [msg] = await tx.insert(conversationMessages).values({
        sessionId: session.id,
        tenantId,
        direction: 'outbound',
        sender: 'operator',
        type,
        content,
        status: 'queued',
        metadata: type === 'image' && imageUrl
          ? { base64: imageUrl, mimetype: 'image/jpeg', caption: content || undefined }
          : {},
      }).returning();

      const [log] = await tx.insert(messageLogs).values({
        tenantId,
        instanceId: instance.id,
        direction: 'outbound',
        recipient: session.customerPhone,
        status: 'queued',
        payload: {
          content, type, source: 'inbox', threadId: id, conversationMessageId: msg.id,
          ...(type === 'image' && { imageUrl, caption: content || undefined }),
        },
        traceId,
        correlationId,
        queuedAt: new Date(),
      }).returning();

      await tx.update(inboxThreads)
        .set({ lastMessageAt: new Date(), updatedAt: new Date() })
        .where(eq(inboxThreads.id, id));

      return { msg, log };
    });

    // Enqueue AFTER transaction commits (avoid sending if DB rollback)
    const queues = getQueues();
    await queues.sendMessage.add('inbox-send', {
      tenantId,
      instanceId: instance.id,
      sessionName: instance.sessionName,
      messageLogId: log.id,
      to: session.customerPhone,
      content: content ?? '',
      type,
      imageUrl,
      caption: type === 'image' ? (content || '') : undefined,
      traceId,
      correlationId,
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

    await db.transaction(async (tx) => {
      await tx.update(inboxThreads)
        .set({ status: 'closed', unreadCount: 0, updatedAt: new Date() })
        .where(eq(inboxThreads.id, id));

      if (thread.conversationSessionId) {
        await tx.update(conversationSessions)
          .set({ state: 'closed', updatedAt: new Date() })
          .where(eq(conversationSessions.id, thread.conversationSessionId));
      }

      await tx.insert(auditLogs).values({
        tenantId,
        actor: userId ?? 'system',
        entityType: 'inbox_thread',
        action: 'closed',
        entityId: id,
        newState: { status: 'closed', reason },
        metadata: { traceId, correlationId },
      });
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

  /**
   * POST /api/v1/inbox/threads/:id/read
   * Mark thread as read (reset unread count, set readAt on messages).
   */
  fastify.post<{
    Params: { id: string };
  }>('/api/v1/inbox/threads/:id/read', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const { id } = request.params;

    const [thread] = await db
      .select({ id: inboxThreads.id, conversationSessionId: inboxThreads.conversationSessionId })
      .from(inboxThreads)
      .where(and(eq(inboxThreads.id, id), eq(inboxThreads.tenantId, tenantId)));

    if (!thread) {
      return reply.code(404).send({ error: 'Thread not found' });
    }

    // Reset unread count
    await db.update(inboxThreads)
      .set({ unreadCount: 0, updatedAt: new Date() })
      .where(eq(inboxThreads.id, id));

    // Mark all unread inbound messages as read
    await db.update(conversationMessages)
      .set({ readAt: new Date() })
      .where(and(
        eq(conversationMessages.sessionId, thread.conversationSessionId!),
        eq(conversationMessages.direction, 'inbound'),
        sql`${conversationMessages.readAt} IS NULL`,
      ));

    // Emit event for other clients
    eventBus.emit({
      tenantId,
      traceId,
      correlationId,
      timestamp: new Date().toISOString(),
      version: '1',
      type: 'inbox.thread.updated' as const,
      data: { threadId: id, unreadCount: 0, action: 'read' as const },
    });

    logger.info({ tenantId, threadId: id }, 'Thread marked as read');
    return { success: true };
  });

  /**
   * POST /api/v1/inbox/threads/:id/sync
   * Sync message history from WhatsApp for a thread (async via worker).
   */
  fastify.post<{
    Params: { id: string };
  }>('/api/v1/inbox/threads/:id/sync', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const { id } = request.params;

    // Verify thread belongs to tenant
    const [thread] = await db
      .select({ id: inboxThreads.id })
      .from(inboxThreads)
      .where(and(eq(inboxThreads.id, id), eq(inboxThreads.tenantId, tenantId)));

    if (!thread) {
      return reply.code(404).send({ error: 'Thread not found' });
    }

    const queues = getQueues();
    await queues.syncHistory.add('sync', {
      tenantId,
      threadId: id,
      traceId,
      correlationId,
    });

    return { queued: true };
  });
};

export default inboxRoutes;
