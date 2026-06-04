import type { FastifyPluginAsync } from 'fastify';
import { eq, and, gte, lte, desc } from 'drizzle-orm';
import { db, auditLogs } from '@rezervae-connect/database';

const auditRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.get<{
    Querystring: {
      entity_type?: string;
      action?: string;
      from?: string;
      to?: string;
      limit?: string;
      offset?: string;
    };
  }>('/api/v1/audit', async (request) => {
    const { tenantId } = request.tenant;
    const { entity_type, action, from, to, limit, offset } = request.query;

    const conditions = [eq(auditLogs.tenantId, tenantId)];
    if (entity_type) conditions.push(eq(auditLogs.entityType, entity_type));
    if (action) conditions.push(eq(auditLogs.action, action));
    if (from) conditions.push(gte(auditLogs.createdAt, new Date(from)));
    if (to) conditions.push(lte(auditLogs.createdAt, new Date(to)));

    const pageLimit = Math.min(parseInt(limit ?? '50', 10), 200);
    const pageOffset = parseInt(offset ?? '0', 10);

    const logs = await db.select().from(auditLogs)
      .where(and(...conditions))
      .orderBy(desc(auditLogs.createdAt))
      .limit(pageLimit)
      .offset(pageOffset);

    return { data: logs, meta: { limit: pageLimit, offset: pageOffset } };
  });
};

export default auditRoutes;
