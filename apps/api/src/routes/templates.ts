import type { FastifyPluginAsync } from 'fastify';
import { eq, and } from 'drizzle-orm';
import { db, messageTemplates, auditLogs } from '@rezervae-connect/database';

const templateRoutes: FastifyPluginAsync = async (fastify) => {
  // List templates
  fastify.get('/api/v1/templates', async (request) => {
    const { tenantId } = request.tenant;
    const templates = await db.select().from(messageTemplates)
      .where(eq(messageTemplates.tenantId, tenantId));
    return { data: templates };
  });

  // Get single template
  fastify.get<{ Params: { id: string } }>('/api/v1/templates/:id', async (request, reply) => {
    const { tenantId } = request.tenant;
    const [template] = await db.select().from(messageTemplates).where(
      and(eq(messageTemplates.id, request.params.id), eq(messageTemplates.tenantId, tenantId)),
    );
    if (!template) return reply.code(404).send({ error: 'Template not found' });
    return { data: template };
  });

  // Create template
  fastify.post<{
    Body: { slug: string; name: string; channel?: string; type?: string; content: string; variables?: Array<{ name: string; required: boolean }>; metadata?: Record<string, unknown> };
  }>('/api/v1/templates', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const { slug, name, channel, type, content, variables, metadata } = request.body;

    const [template] = await db.insert(messageTemplates).values({
      tenantId, slug, name,
      channel: channel ?? 'whatsapp',
      type: type ?? 'text',
      content, variables: variables ?? [],
      metadata: metadata ?? {},
      isActive: true,
    }).returning();

    await db.insert(auditLogs).values({
      tenantId, actor: 'api', entityType: 'template', entityId: template.id,
      action: 'created', newState: { slug, name },
      metadata: { traceId, correlationId },
    });

    return reply.code(201).send({ data: template });
  });

  // Update template
  fastify.put<{
    Params: { id: string };
    Body: { name?: string; content?: string; variables?: Record<string, unknown>; metadata?: Record<string, unknown>; isActive?: boolean };
  }>('/api/v1/templates/:id', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;

    const [existing] = await db.select().from(messageTemplates).where(
      and(eq(messageTemplates.id, request.params.id), eq(messageTemplates.tenantId, tenantId)),
    );
    if (!existing) return reply.code(404).send({ error: 'Template not found' });

    const updates: Record<string, unknown> = {};
    if (request.body.name !== undefined) updates.name = request.body.name;
    if (request.body.content !== undefined) updates.content = request.body.content;
    if (request.body.variables !== undefined) updates.variables = request.body.variables;
    if (request.body.metadata !== undefined) updates.metadata = request.body.metadata;
    if (request.body.isActive !== undefined) updates.isActive = request.body.isActive;

    const [updated] = await db.update(messageTemplates).set(updates)
      .where(eq(messageTemplates.id, request.params.id)).returning();

    await db.insert(auditLogs).values({
      tenantId, actor: 'api', entityType: 'template', entityId: updated.id,
      action: 'updated', oldState: { name: existing.name, content: existing.content },
      newState: updates,
      metadata: { traceId, correlationId },
    });

    return { data: updated };
  });

  // Delete template
  fastify.delete<{ Params: { id: string } }>('/api/v1/templates/:id', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;

    const [existing] = await db.select().from(messageTemplates).where(
      and(eq(messageTemplates.id, request.params.id), eq(messageTemplates.tenantId, tenantId)),
    );
    if (!existing) return reply.code(404).send({ error: 'Template not found' });

    await db.delete(messageTemplates).where(eq(messageTemplates.id, request.params.id));

    await db.insert(auditLogs).values({
      tenantId, actor: 'api', entityType: 'template', entityId: existing.id,
      action: 'deleted', oldState: { slug: existing.slug, name: existing.name },
      metadata: { traceId, correlationId },
    });

    return reply.code(204).send();
  });

  // Preview template
  fastify.post<{
    Params: { id: string };
    Body: { variables: Record<string, string> };
  }>('/api/v1/templates/:id/preview', async (request, reply) => {
    const { tenantId } = request.tenant;

    const [template] = await db.select().from(messageTemplates).where(
      and(eq(messageTemplates.id, request.params.id), eq(messageTemplates.tenantId, tenantId)),
    );
    if (!template) return reply.code(404).send({ error: 'Template not found' });

    // Simple variable replacement
    let rendered = template.content;
    for (const [key, value] of Object.entries(request.body.variables ?? {})) {
      rendered = rendered.replace(new RegExp(`\\{\\{${key}\\}\\}`, 'g'), value);
    }

    return { data: { original: template.content, rendered } };
  });
};

export default templateRoutes;
