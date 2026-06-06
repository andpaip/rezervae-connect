import type { FastifyPluginAsync } from 'fastify';
import { getQueues } from '@rezervae-connect/queue';
import { createLogger } from '@rezervae-connect/shared';
import { isDuplicateWebhook } from '../plugins/safety-guard.js';

const logger = createLogger('core-events');

/**
 * Receives business events from Rezervae Core (Laravel).
 *
 * Events are enqueued to the core-events queue for async processing
 * by the automation engine (core-events.worker.ts).
 *
 * POST /api/v1/webhooks/core-events
 * Body: { event_type, event_id, tenant_id, payload, occurred_at }
 */
const coreEventsRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.post('/api/v1/webhooks/core-events', async (request, reply) => {
    const body = request.body as {
      event_type: string;
      event_id: string;
      tenant_id: string;
      payload: Record<string, unknown>;
      occurred_at: string;
    };

    if (!body.event_type || !body.event_id) {
      return reply.code(400).send({ error: 'Missing event_type or event_id' });
    }

    // Webhook storm protection: suppress duplicate events in short window
    if (isDuplicateWebhook(body.event_type, body.event_id, body.occurred_at ?? new Date().toISOString())) {
      return reply.code(202).send({ ok: true, event_id: body.event_id, status: 'deduplicated' });
    }

    const { tenantId, traceId, correlationId } = request.tenant;

    logger.info({
      eventType: body.event_type,
      eventId: body.event_id,
      tenantId,
      traceId,
      correlationId,
    }, 'Core event received');

    const queues = getQueues();
    await queues.coreEvents.add(body.event_type, {
      eventType: body.event_type,
      eventId: body.event_id,
      tenantId,
      payload: body.payload,
      occurredAt: body.occurred_at,
      traceId,
      correlationId,
    }, {
      jobId: body.event_id, // dedup: same event_id won't be enqueued twice
      attempts: 3,
      backoff: { type: 'exponential', delay: 10_000 },
    });

    return reply.code(202).send({
      ok: true,
      event_id: body.event_id,
      status: 'queued',
    });
  });
};

export default coreEventsRoutes;
