import { eventBus, type ConnectEvent } from '@rezervae-connect/events';
import { getQueues } from '@rezervae-connect/queue';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('core-webhook-subscriber');

/**
 * Events that should be forwarded to Core as webhooks.
 * These map to what WebhookReceiverController expects.
 */
const CORE_WEBHOOK_EVENTS = [
  'message.sent',
  'message.failed',
  'message.received',
  'conversation.created',
  'campaign.finished',
] as const;

/**
 * Subscribe to relevant events and enqueue webhook-delivery jobs
 * to notify Core about messaging outcomes.
 */
export function setupCoreWebhookSubscriptions(): void {
  const coreWebhookUrl = process.env.CORE_WEBHOOK_URL
    ?? `${process.env.CORE_API_URL ?? 'http://localhost:8080'}/api/internal/connect/webhooks`;

  for (const eventType of CORE_WEBHOOK_EVENTS) {
    eventBus.on(eventType, async (event: ConnectEvent) => {
      try {
        const queues = getQueues();

        await queues.webhookDelivery.add('core-webhook', {
          tenantId: event.tenantId,
          url: coreWebhookUrl,
          event: event.type,
          payload: {
            event_id: `${event.type}:${event.timestamp}:${event.traceId}`,
            event_type: event.type,
            ...event.data,
            occurred_at: event.timestamp,
          },
          traceId: event.traceId,
          correlationId: event.correlationId,
        }, {
          attempts: 3,
          backoff: { type: 'exponential', delay: 5_000 },
        });

        logger.debug({
          eventType: event.type,
          tenantId: event.tenantId,
          traceId: event.traceId,
        }, 'Core webhook enqueued');
      } catch (err) {
        logger.error({
          eventType: event.type,
          tenantId: event.tenantId,
          err,
        }, 'Failed to enqueue Core webhook');
      }
    });
  }

  logger.info({ events: CORE_WEBHOOK_EVENTS }, 'Core webhook subscriptions active');
}
