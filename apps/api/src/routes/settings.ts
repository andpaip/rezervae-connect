import type { FastifyPluginAsync } from 'fastify';
import { eq, and } from 'drizzle-orm';
import { db, tenants, whatsappInstances } from '@rezervae-connect/database';
import { getRedisClient } from '@rezervae-connect/queue';

const settingsRoutes: FastifyPluginAsync = async (fastify) => {
  /**
   * GET /api/v1/settings
   * Returns the tenant's settings.
   */
  fastify.get('/api/v1/settings', async (request) => {
    const [tenant] = await db
      .select({ settings: tenants.settings })
      .from(tenants)
      .where(eq(tenants.id, request.tenant.tenantId));

    return { data: tenant?.settings ?? {} };
  });

  /**
   * PATCH /api/v1/settings
   * Shallow-merge incoming keys into tenant settings jsonb.
   */
  fastify.patch<{ Body: Record<string, unknown> }>('/api/v1/settings', async (request, reply) => {
    const { tenantId } = request.tenant;
    const body = request.body ?? {};

    // Validate sendIntervalMs (anti-ban floor: 15s minimum)
    if (body.sendIntervalMs !== undefined) {
      const val = Number(body.sendIntervalMs);
      if (isNaN(val) || val < 15000) {
        return reply.code(400).send({ error: 'sendIntervalMs must be >= 15000 (15s minimum)' });
      }
      body.sendIntervalMs = val;
      // Invalidate worker cache so new interval takes effect
      const redis = getRedisClient();
      await redis.del(`tenant-interval:${tenantId}`);
    }

    // Validate defaultSendInstanceId belongs to this tenant
    if (body.defaultSendInstanceId) {
      const [instance] = await db
        .select({ id: whatsappInstances.id })
        .from(whatsappInstances)
        .where(
          and(
            eq(whatsappInstances.tenantId, tenantId),
            eq(whatsappInstances.id, body.defaultSendInstanceId as string),
          ),
        );
      if (!instance) {
        return reply.code(400).send({ error: 'Instance not found for this tenant' });
      }
    }

    const [tenant] = await db
      .select({ settings: tenants.settings })
      .from(tenants)
      .where(eq(tenants.id, tenantId));

    const merged = { ...(tenant?.settings ?? {}), ...body };

    await db.update(tenants)
      .set({ settings: merged, updatedAt: new Date() })
      .where(eq(tenants.id, tenantId));

    return { data: merged };
  });
};

export default settingsRoutes;
