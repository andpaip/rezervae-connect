import { Queue } from 'bullmq';
import { getRedisConnectionOptions } from './connection.js';

/**
 * Check Redis connectivity via a throwaway queue ping.
 */
export async function checkRedisHealth(): Promise<{ status: string; latencyMs?: number }> {
  const start = Date.now();
  const q = new Queue('__health_check__', { connection: getRedisConnectionOptions() });

  try {
    // BullMQ creates a Redis connection internally; if it connects, Redis is healthy
    await q.getJobCounts();
    const latencyMs = Date.now() - start;
    await q.close();
    return { status: 'healthy', latencyMs };
  } catch {
    try { await q.close(); } catch { /* ignore */ }
    return { status: 'unhealthy' };
  }
}

/**
 * Get job counts for all main queues.
 */
export async function getQueueStats(queues: Record<string, Queue>): Promise<Record<string, unknown>> {
  const stats: Record<string, unknown> = {};

  for (const [name, queue] of Object.entries(queues)) {
    try {
      const counts = await queue.getJobCounts();
      stats[name] = counts;
    } catch {
      stats[name] = { error: 'unavailable' };
    }
  }

  return stats;
}
