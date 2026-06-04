import type { FastifyPluginAsync } from 'fastify';
import { db } from '@rezervae-connect/database';
import { sql } from 'drizzle-orm';
import { checkRedisHealth, getQueueStats, getQueues } from '@rezervae-connect/queue';

const healthRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.get('/api/v1/health', async () => {
    return {
      status: 'healthy',
      service: 'rezervae-connect-api',
      uptime: process.uptime(),
    };
  });

  fastify.get('/api/v1/health/detailed', async () => {
    const checks: Record<string, unknown> = {
      uptime: process.uptime(),
      memoryUsage: process.memoryUsage(),
    };

    // Postgres check
    try {
      const start = Date.now();
      await db.execute(sql`SELECT 1`);
      checks.postgres = { status: 'healthy', latencyMs: Date.now() - start };
    } catch {
      checks.postgres = { status: 'unhealthy' };
    }

    // Redis check
    checks.redis = await checkRedisHealth();

    // Queue stats
    try {
      const queues = getQueues();
      checks.queues = await getQueueStats(queues);
    } catch {
      checks.queues = { error: 'unavailable' };
    }

    const pgOk = (checks.postgres as Record<string, unknown>).status === 'healthy';
    const redisOk = (checks.redis as Record<string, unknown>).status === 'healthy';

    return {
      status: pgOk && redisOk ? 'healthy' : 'degraded',
      service: 'rezervae-connect-api',
      checks,
    };
  });
};

export default healthRoutes;
