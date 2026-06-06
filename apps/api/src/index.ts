import 'dotenv/config';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import rateLimit from '@fastify/rate-limit';
import { createLogger } from '@rezervae-connect/shared';
import internalAuth from './plugins/internal-auth.js';
import tenantRateLimit from './plugins/tenant-rate-limit.js';
import safetyGuard from './plugins/safety-guard.js';
import healthRoutes from './routes/health.js';
import instanceRoutes from './routes/instances.js';
import messageRoutes from './routes/messages.js';
import campaignRoutes from './routes/campaigns.js';
import templateRoutes from './routes/templates.js';
import auditRoutes from './routes/audit.js';
import coreEventsRoutes from './routes/core-events.js';
import adminRoutes from './routes/admin.js';

const logger = createLogger('api');

const app = Fastify({ logger: true });

// Capture raw body for HMAC signature validation
// Must be registered before content-type parsers run
declare module 'fastify' {
  interface FastifyRequest {
    rawBody?: string;
  }
}
app.addHook('preParsing', async (request, _reply, payload) => {
  const chunks: Buffer[] = [];
  for await (const chunk of payload) {
    chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : chunk);
  }
  const raw = Buffer.concat(chunks).toString('utf-8');
  request.rawBody = raw;
  // Return a new readable stream with the same data for Fastify to parse
  const { Readable } = await import('node:stream');
  return Readable.from([raw]);
});

// Plugins
await app.register(cors, { origin: true });
await app.register(rateLimit, {
  max: 100,
  timeWindow: '1 minute',
});

// Auth (skips /api/v1/health)
await app.register(internalAuth);
await app.register(tenantRateLimit);
await app.register(safetyGuard);

// Routes
await app.register(healthRoutes);
await app.register(instanceRoutes);
await app.register(messageRoutes);
await app.register(campaignRoutes);
await app.register(templateRoutes);
await app.register(auditRoutes);
await app.register(coreEventsRoutes);
await app.register(adminRoutes);

const port = Number(process.env.API_PORT ?? 3100);
const host = process.env.API_HOST ?? '0.0.0.0';

await app.listen({ port, host });
logger.info({ port, host }, 'Rezervae Connect API ready');
