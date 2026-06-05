import type { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { createHmac } from 'node:crypto';
import { eq } from 'drizzle-orm';
import { db, tenants } from '@rezervae-connect/database';
import { createLogger, type TenantContext, createTraceContext } from '@rezervae-connect/shared';

const logger = createLogger('internal-auth');

const TIMESTAMP_WINDOW_MS = 5 * 60 * 1000; // 5 minutes anti-replay

declare module 'fastify' {
  interface FastifyRequest {
    tenant: TenantContext;
  }
}

const internalAuthPlugin: FastifyPluginAsync = async (fastify) => {
  fastify.decorateRequest('tenant', undefined as unknown as TenantContext);

  // Use preHandler so request.body is already parsed (onRequest runs before body parsing)
  fastify.addHook('preHandler', async (request: FastifyRequest, reply: FastifyReply) => {
    // Skip health endpoints
    if (request.url.startsWith('/api/v1/health')) return;

    const token = request.headers['x-internal-token'] as string | undefined;
    const signature = request.headers['x-signature'] as string | undefined;
    const timestamp = request.headers['x-timestamp'] as string | undefined;
    const tenantId = request.headers['x-tenant-id'] as string | undefined;
    const correlationId = request.headers['x-correlation-id'] as string | undefined;

    if (!token || !signature || !timestamp || !tenantId) {
      logger.warn({ url: request.url }, 'Missing auth headers');
      return reply.code(401).send({ error: 'Missing authentication headers' });
    }

    // Anti-replay: validate timestamp window
    const reqTime = parseInt(timestamp, 10);
    const now = Date.now();
    if (isNaN(reqTime) || Math.abs(now - reqTime) > TIMESTAMP_WINDOW_MS) {
      logger.warn({ tenantId, drift: Math.abs(now - reqTime) }, 'Timestamp outside window');
      return reply.code(401).send({ error: 'Request timestamp expired' });
    }

    // Validate tenant
    const [tenant] = await db.select().from(tenants).where(eq(tenants.id, tenantId));
    if (!tenant || tenant.status !== 'active') {
      logger.warn({ tenantId }, 'Tenant not found or inactive');
      return reply.code(401).send({ error: 'Invalid tenant' });
    }

    // Validate token against stored hash
    const tokenHash = createHmac('sha256', process.env.INTERNAL_SECRET ?? 'dev-secret')
      .update(token)
      .digest('hex');

    if (tokenHash !== tenant.apiKeyHash) {
      logger.warn({ tenantId }, 'Invalid API token');
      return reply.code(401).send({ error: 'Invalid credentials' });
    }

    // Validate HMAC signature using raw body (avoids JSON re-serialization differences)
    const body = request.rawBody ?? '';
    const expectedSig = createHmac('sha256', token)
      .update(`${timestamp}:${request.method}:${request.url}:${body}`)
      .digest('hex');

    if (signature !== expectedSig) {
      logger.warn({ tenantId }, 'Invalid HMAC signature');
      return reply.code(401).send({ error: 'Invalid signature' });
    }

    // Build tenant context
    const trace = createTraceContext(correlationId);
    request.tenant = {
      tenantId: tenant.id,
      tenantSlug: tenant.slug,
      settings: (tenant.settings as Record<string, unknown>) ?? {},
      traceId: trace.traceId,
      correlationId: trace.correlationId,
    };
  });
};

export default fp(internalAuthPlugin, { name: 'internal-auth' });
