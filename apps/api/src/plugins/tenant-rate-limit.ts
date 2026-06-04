import type { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('tenant-rate-limit');

/**
 * Per-tenant rate limiting applied on top of the global rate limit.
 * Uses in-memory sliding window (for single-instance deploy).
 * For multi-instance, swap to Redis-backed counter.
 */

interface BucketEntry {
  timestamps: number[];
}

const buckets = new Map<string, BucketEntry>();
const WINDOW_MS = 60_000; // 1 minute

// Default limits per route prefix
const ROUTE_LIMITS: Record<string, number> = {
  '/api/v1/messages': 30,     // 30 message requests/min per tenant
  '/api/v1/campaigns': 5,     // 5 campaign operations/min per tenant
  '/api/v1/instances': 20,    // 20 instance operations/min per tenant
  '/api/v1/templates': 30,    // 30 template operations/min per tenant
};

function getBucketKey(tenantId: string, prefix: string): string {
  return `${tenantId}:${prefix}`;
}

function getRoutePrefix(url: string): string | null {
  for (const prefix of Object.keys(ROUTE_LIMITS)) {
    if (url.startsWith(prefix)) return prefix;
  }
  return null;
}

const tenantRateLimitPlugin: FastifyPluginAsync = async (fastify) => {
  fastify.addHook('onRequest', async (request: FastifyRequest, reply: FastifyReply) => {
    // Skip if no tenant context (health endpoints)
    if (!request.tenant) return;

    const prefix = getRoutePrefix(request.url);
    if (!prefix) return;

    const tenantId = request.tenant.tenantId;

    // Check tenant-specific override from settings
    const tenantSettings = request.tenant.settings as Record<string, unknown> | undefined;
    const overrideKey = `rateLimit_${prefix.replace(/\//g, '_')}`;
    const limit = (tenantSettings?.[overrideKey] as number) ?? ROUTE_LIMITS[prefix];

    const key = getBucketKey(tenantId, prefix);
    const now = Date.now();

    let bucket = buckets.get(key);
    if (!bucket) {
      bucket = { timestamps: [] };
      buckets.set(key, bucket);
    }

    // Slide window: remove old entries
    bucket.timestamps = bucket.timestamps.filter((t) => now - t < WINDOW_MS);

    if (bucket.timestamps.length >= limit) {
      logger.warn({ tenantId, prefix, count: bucket.timestamps.length, limit }, 'Tenant rate limit exceeded');
      return reply.code(429).send({
        error: 'Rate limit exceeded',
        retryAfter: Math.ceil((bucket.timestamps[0] + WINDOW_MS - now) / 1000),
      });
    }

    bucket.timestamps.push(now);
  });

  // Cleanup old buckets every 5 minutes
  setInterval(() => {
    const now = Date.now();
    for (const [key, bucket] of buckets) {
      bucket.timestamps = bucket.timestamps.filter((t) => now - t < WINDOW_MS);
      if (bucket.timestamps.length === 0) buckets.delete(key);
    }
  }, 5 * 60_000);
};

export default fp(tenantRateLimitPlugin, {
  name: 'tenant-rate-limit',
  dependencies: ['internal-auth'],
});
