import type { FastifyPluginAsync } from 'fastify';
import { randomBytes, createHmac, timingSafeEqual } from 'node:crypto';
import { eq } from 'drizzle-orm';
import { db, tenants } from '@rezervae-connect/database';
import { getQueues, QUEUE_NAMES } from '@rezervae-connect/queue';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('admin');

const VALID_QUEUES = Object.values(QUEUE_NAMES);

const adminRoutes: FastifyPluginAsync = async (fastify) => {
  // All admin routes require Bearer token = INTERNAL_SECRET (timing-safe)
  fastify.addHook('preHandler', async (request, reply) => {
    const auth = request.headers.authorization;
    const expected = process.env.INTERNAL_SECRET;
    if (!expected || !auth) {
      return reply.status(401).send({ error: 'Unauthorized' });
    }
    const expectedBuf = Buffer.from(`Bearer ${expected}`);
    const actualBuf = Buffer.from(auth);
    if (expectedBuf.length !== actualBuf.length || !timingSafeEqual(expectedBuf, actualBuf)) {
      return reply.status(401).send({ error: 'Unauthorized' });
    }
  });

  // POST /api/v1/admin/tenants — create or return existing tenant
  fastify.post<{
    Body: { name: string; slug: string; externalId: string };
  }>('/api/v1/admin/tenants', async (request, reply) => {
    const { name, slug, externalId } = request.body;

    if (!name || !slug) {
      return reply.status(400).send({ error: 'name and slug are required' });
    }

    // Idempotent: if slug already exists, return existing tenant (no new apiKey)
    const [existing] = await db
      .select({ id: tenants.id })
      .from(tenants)
      .where(eq(tenants.slug, slug))
      .limit(1);

    if (existing) {
      logger.info({ slug, tenantId: existing.id }, 'Tenant already exists, returning existing');
      return { data: { id: existing.id, apiKey: null, existing: true } };
    }

    // Generate API key and compute hash
    const apiKey = randomBytes(32).toString('hex');
    const secret = process.env.INTERNAL_SECRET;
    if (!secret && process.env.NODE_ENV === 'production') {
      throw new Error('INTERNAL_SECRET env var is required in production');
    }
    const apiKeyHash = createHmac('sha256', secret ?? 'dev-secret').update(apiKey).digest('hex');

    const [tenant] = await db
      .insert(tenants)
      .values({
        name,
        slug,
        apiKeyHash,
        settings: { externalId },
        status: 'active',
      })
      .returning({ id: tenants.id });

    logger.info({ slug, tenantId: tenant.id, externalId }, 'Tenant provisioned');

    return { data: { id: tenant.id, apiKey, existing: false } };
  });

  fastify.post<{ Body: { queue: string } }>('/api/v1/admin/queues/drain', async (request) => {
    const { queue: queueName } = request.body;

    if (!VALID_QUEUES.includes(queueName as typeof VALID_QUEUES[number])) {
      return { error: 'Invalid queue name', valid: VALID_QUEUES };
    }

    const queues = getQueues();
    const queueEntry = Object.entries(queues).find(
      ([, q]) => q.name === queueName,
    );

    if (!queueEntry) {
      return { error: 'Queue not found' };
    }

    const queue = queueEntry[1];
    const countsBefore = await queue.getJobCounts();
    await queue.drain();
    const cleaned = await queue.clean(0, 0, 'failed');
    const countsAfter = await queue.getJobCounts();

    logger.info({ queueName, countsBefore, countsAfter, cleanedFailed: cleaned.length }, 'Queue drained');

    return {
      ok: true,
      queue: queueName,
      before: countsBefore,
      after: countsAfter,
      cleanedFailed: cleaned.length,
    };
  });

  fastify.get('/api/v1/admin/queues/stats', async () => {
    const queues = getQueues();
    const stats: Record<string, unknown> = {};

    for (const [key, queue] of Object.entries(queues)) {
      stats[queue.name] = await queue.getJobCounts();
    }

    return { ok: true, queues: stats };
  });
};

export default adminRoutes;
