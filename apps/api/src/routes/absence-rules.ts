import type { FastifyPluginAsync } from 'fastify';
import { eq, and, asc } from 'drizzle-orm';
import { db, absenceRules, whatsappInstances } from '@rezervae-connect/database';

interface CreateBody {
  days: number;
  instanceId?: string;
  templateSlug?: string;
  messageType?: string;
  enabled?: boolean;
  sortOrder?: number;
}

const absenceRulesRoutes: FastifyPluginAsync = async (fastify) => {
  /**
   * GET /api/v1/absence-rules
   */
  fastify.get('/api/v1/absence-rules', async (request) => {
    const { tenantId } = request.tenant;

    const rules = await db
      .select()
      .from(absenceRules)
      .where(eq(absenceRules.tenantId, tenantId))
      .orderBy(asc(absenceRules.sortOrder), asc(absenceRules.days));

    return { data: rules };
  });

  /**
   * POST /api/v1/absence-rules
   */
  fastify.post<{ Body: CreateBody }>('/api/v1/absence-rules', async (request, reply) => {
    const { tenantId } = request.tenant;
    const { days, instanceId, templateSlug, messageType, enabled, sortOrder } = request.body ?? {};

    if (!days || days < 1) {
      return reply.code(400).send({ error: 'days is required and must be positive' });
    }

    // Validate instanceId belongs to tenant
    if (instanceId) {
      const [instance] = await db
        .select({ id: whatsappInstances.id })
        .from(whatsappInstances)
        .where(and(
          eq(whatsappInstances.tenantId, tenantId),
          eq(whatsappInstances.id, instanceId),
        ));
      if (!instance) {
        return reply.code(400).send({ error: 'Instance not found for this tenant' });
      }
    }

    const [rule] = await db.insert(absenceRules).values({
      tenantId,
      days,
      instanceId: instanceId ?? null,
      templateSlug: templateSlug ?? null,
      messageType: messageType ?? 'text',
      enabled: enabled ?? true,
      sortOrder: sortOrder ?? 0,
    }).returning();

    return reply.code(201).send({ data: rule });
  });

  /**
   * PUT /api/v1/absence-rules/:id
   */
  fastify.put<{ Params: { id: string }; Body: Partial<CreateBody> }>('/api/v1/absence-rules/:id', async (request, reply) => {
    const { tenantId } = request.tenant;
    const { id } = request.params;
    const body = request.body ?? {};

    // Validate instanceId if provided
    if (body.instanceId) {
      const [instance] = await db
        .select({ id: whatsappInstances.id })
        .from(whatsappInstances)
        .where(and(
          eq(whatsappInstances.tenantId, tenantId),
          eq(whatsappInstances.id, body.instanceId),
        ));
      if (!instance) {
        return reply.code(400).send({ error: 'Instance not found for this tenant' });
      }
    }

    const updates: Record<string, unknown> = {};
    if (body.days !== undefined) updates.days = body.days;
    if (body.instanceId !== undefined) updates.instanceId = body.instanceId;
    if (body.templateSlug !== undefined) updates.templateSlug = body.templateSlug;
    if (body.messageType !== undefined) updates.messageType = body.messageType;
    if (body.enabled !== undefined) updates.enabled = body.enabled;
    if (body.sortOrder !== undefined) updates.sortOrder = body.sortOrder;

    const [updated] = await db.update(absenceRules)
      .set(updates)
      .where(and(eq(absenceRules.id, id), eq(absenceRules.tenantId, tenantId)))
      .returning();

    if (!updated) {
      return reply.code(404).send({ error: 'Rule not found' });
    }

    return { data: updated };
  });

  /**
   * DELETE /api/v1/absence-rules/:id
   */
  fastify.delete<{ Params: { id: string } }>('/api/v1/absence-rules/:id', async (request, reply) => {
    const { tenantId } = request.tenant;
    const { id } = request.params;

    const [deleted] = await db.delete(absenceRules)
      .where(and(eq(absenceRules.id, id), eq(absenceRules.tenantId, tenantId)))
      .returning();

    if (!deleted) {
      return reply.code(404).send({ error: 'Rule not found' });
    }

    return { message: 'Rule deleted' };
  });
};

export default absenceRulesRoutes;
