import 'dotenv/config';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import rateLimit from '@fastify/rate-limit';
import { createLogger } from '@rezervae-connect/shared';
import internalAuth from './plugins/internal-auth.js';
import tenantRateLimit from './plugins/tenant-rate-limit.js';
import healthRoutes from './routes/health.js';
import instanceRoutes from './routes/instances.js';
import messageRoutes from './routes/messages.js';
import campaignRoutes from './routes/campaigns.js';
import templateRoutes from './routes/templates.js';
import auditRoutes from './routes/audit.js';

const logger = createLogger('api');

const app = Fastify({ logger: true });

// Plugins
await app.register(cors, { origin: true });
await app.register(rateLimit, {
  max: 100,
  timeWindow: '1 minute',
});

// Auth (skips /api/v1/health)
await app.register(internalAuth);
await app.register(tenantRateLimit);

// Routes
await app.register(healthRoutes);
await app.register(instanceRoutes);
await app.register(messageRoutes);
await app.register(campaignRoutes);
await app.register(templateRoutes);
await app.register(auditRoutes);

const port = Number(process.env.API_PORT ?? 3100);
const host = process.env.API_HOST ?? '0.0.0.0';

await app.listen({ port, host });
logger.info({ port, host }, 'Rezervae Connect API ready');
