import type { FastifyPluginAsync } from 'fastify';
import { getQueues, QUEUE_NAMES } from '@rezervae-connect/queue';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('admin');

const VALID_QUEUES = Object.values(QUEUE_NAMES);

const adminRoutes: FastifyPluginAsync = async (fastify) => {
  // All admin routes require Bearer token = INTERNAL_SECRET
  fastify.addHook('preHandler', async (request, reply) => {
    const auth = request.headers.authorization;
    const expected = process.env.INTERNAL_SECRET;
    if (!expected || auth !== `Bearer ${expected}`) {
      return reply.status(401).send({ error: 'Unauthorized' });
    }
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
