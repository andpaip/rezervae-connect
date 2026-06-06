import type { FastifyPluginAsync } from 'fastify';
import { eq, and } from 'drizzle-orm';
import { db, whatsappInstances, auditLogs } from '@rezervae-connect/database';
import { getQueues, QUEUE_NAMES } from '@rezervae-connect/queue';

const instanceRoutes: FastifyPluginAsync = async (fastify) => {
  // List instances
  fastify.get('/api/v1/instances', async (request) => {
    const { tenantId } = request.tenant;
    const instances = await db
      .select()
      .from(whatsappInstances)
      .where(eq(whatsappInstances.tenantId, tenantId));
    return { data: instances };
  });

  // Get instance status
  fastify.get<{ Params: { id: string } }>('/api/v1/instances/:id/status', async (request, reply) => {
    const { tenantId } = request.tenant;
    const [instance] = await db
      .select()
      .from(whatsappInstances)
      .where(
        and(
          eq(whatsappInstances.id, request.params.id),
          eq(whatsappInstances.tenantId, tenantId),
        ),
      );

    if (!instance) return reply.code(404).send({ error: 'Instance not found' });

    return {
      data: {
        id: instance.id,
        sessionName: instance.sessionName,
        instanceName: instance.instanceName,
        status: instance.status,
        phone: instance.phone,
        qrCode: instance.qrCode,
        healthScore: instance.healthScore,
        failureRate: instance.failureRate,
        lastSeenAt: instance.lastSeenAt,
        connectedAt: instance.connectedAt,
      },
    };
  });

  // Create instance
  fastify.post<{
    Body: { instanceName: string; sessionName: string; provider?: string };
  }>('/api/v1/instances', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const { instanceName, sessionName, provider } = request.body;

    const [instance] = await db.insert(whatsappInstances).values({
      tenantId,
      instanceName,
      sessionName,
      provider: provider ?? 'wppconnect',
      status: 'disconnected',
    }).returning();

    await db.insert(auditLogs).values({
      tenantId,
      actor: 'api',
      entityType: 'instance',
      entityId: instance.id,
      action: 'created',
      newState: { instanceName, sessionName, provider: provider ?? 'wppconnect' },
      metadata: { traceId, correlationId },
    });

    return reply.code(201).send({ data: instance });
  });

  // Connect instance (triggers session creation)
  fastify.post<{ Params: { id: string } }>('/api/v1/instances/:id/connect', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;

    const [instance] = await db
      .select()
      .from(whatsappInstances)
      .where(
        and(
          eq(whatsappInstances.id, request.params.id),
          eq(whatsappInstances.tenantId, tenantId),
        ),
      );

    if (!instance) return reply.code(404).send({ error: 'Instance not found' });

    if (instance.status === 'connected') {
      return reply.code(409).send({ error: 'Instance already connected' });
    }

    // Enqueue reconnect job (workers handle session creation)
    // jobId dedup prevents duplicate jobs in BullMQ
    const queues = getQueues();
    await queues.reconnect.add('connect', {
      tenantId,
      instanceId: instance.id,
      sessionName: instance.sessionName,
      attempt: 0,
      traceId,
      correlationId,
    }, { jobId: `connect-${instance.id}-${Date.now()}` });

    await db.insert(auditLogs).values({
      tenantId,
      actor: 'api',
      entityType: 'instance',
      entityId: instance.id,
      action: 'connect_requested',
      metadata: { traceId, correlationId },
    });

    return reply.code(202).send({ message: 'Connection initiated', instanceId: instance.id });
  });

  // Disconnect instance
  fastify.post<{ Params: { id: string } }>('/api/v1/instances/:id/disconnect', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;

    const [instance] = await db
      .select()
      .from(whatsappInstances)
      .where(
        and(
          eq(whatsappInstances.id, request.params.id),
          eq(whatsappInstances.tenantId, tenantId),
        ),
      );

    if (!instance) return reply.code(404).send({ error: 'Instance not found' });

    // Update status to disconnected
    await db.update(whatsappInstances).set({
      status: 'disconnected',
      disconnectedAt: new Date(),
    }).where(eq(whatsappInstances.id, instance.id));

    await db.insert(auditLogs).values({
      tenantId,
      actor: 'api',
      entityType: 'instance',
      entityId: instance.id,
      action: 'disconnect_requested',
      metadata: { traceId, correlationId },
    });

    // Enqueue session cleanup (worker will call sessionManager.disconnectSession)
    const queues = getQueues();
    await queues.reconnect.add('disconnect', {
      tenantId,
      instanceId: instance.id,
      sessionName: instance.sessionName,
      attempt: 0,
      action: 'disconnect',
      traceId,
      correlationId,
    }, { jobId: `disconnect-${instance.id}-${Date.now()}` });

    return reply.code(202).send({ message: 'Disconnection initiated', instanceId: instance.id });
  });
};

export default instanceRoutes;
