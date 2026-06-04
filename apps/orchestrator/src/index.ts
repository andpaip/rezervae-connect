import 'dotenv/config';
import Fastify from 'fastify';
import { createLogger } from '@rezervae-connect/shared';
import { eventBus } from '@rezervae-connect/events';
import type { MessageReceivedEvent } from '@rezervae-connect/events';
import { routeIncomingMessage } from './router/message-router.js';

const logger = createLogger('orchestrator');

const app = Fastify({ logger: true });

app.get('/health', async () => {
  return { status: 'healthy', service: 'rezervae-connect-orchestrator', uptime: process.uptime() };
});

// Subscribe to incoming messages from EventBus
eventBus.on('message.received', async (event) => {
  const e = event as MessageReceivedEvent;
  try {
    await routeIncomingMessage({
      tenantId: e.tenantId,
      sessionName: e.data.sessionName,
      instanceId: '', // resolved from session name in production
      message: {
        from: e.data.from,
        to: e.data.sessionName,
        body: e.data.body,
        type: e.data.messageType,
        isGroupMsg: false,
        sender: { pushname: undefined },
        timestamp: Math.floor(Date.now() / 1000),
        id: e.traceId,
      },
      traceId: e.traceId,
      correlationId: e.correlationId,
    });
  } catch (err) {
    logger.error({ err, tenantId: e.tenantId, traceId: e.traceId }, 'Failed to route message');
  }
});

const port = Number(process.env.ORCHESTRATOR_PORT ?? 3102);

await app.listen({ port, host: '0.0.0.0' });
logger.info({ port }, 'Rezervae Connect Orchestrator ready');
