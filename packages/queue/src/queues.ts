import { Queue } from 'bullmq';
import { getRedisConnectionOptions } from './connection.js';

export const QUEUE_NAMES = {
  SEND_MESSAGE: 'send-message',
  INCOMING_MESSAGE: 'incoming-message',
  CAMPAIGN_SEND: 'campaign-send',
  RECONNECT: 'reconnect',
  WEBHOOK_DELIVERY: 'webhook-delivery',
  AI_PROCESSING: 'ai-processing',
  CORE_EVENTS: 'core-events',
  SYNC_HISTORY: 'sync-history',
  // DLQs
  SEND_MESSAGE_DLQ: 'send-message-dlq',
  CAMPAIGN_DLQ: 'campaign-dlq',
  INCOMING_MESSAGE_DLQ: 'incoming-message-dlq',
  AI_PROCESSING_DLQ: 'ai-processing-dlq',
} as const;

function createQueue(name: string): Queue {
  return new Queue(name, {
    connection: getRedisConnectionOptions(),
    defaultJobOptions: {
      removeOnComplete: { count: 100 },
      removeOnFail: { count: 50 },
    },
  });
}

export function getQueues() {
  return {
    sendMessage: createQueue(QUEUE_NAMES.SEND_MESSAGE),
    incomingMessage: createQueue(QUEUE_NAMES.INCOMING_MESSAGE),
    campaignSend: createQueue(QUEUE_NAMES.CAMPAIGN_SEND),
    reconnect: createQueue(QUEUE_NAMES.RECONNECT),
    webhookDelivery: createQueue(QUEUE_NAMES.WEBHOOK_DELIVERY),
    aiProcessing: createQueue(QUEUE_NAMES.AI_PROCESSING),
    coreEvents: createQueue(QUEUE_NAMES.CORE_EVENTS),
    syncHistory: createQueue(QUEUE_NAMES.SYNC_HISTORY),
    // DLQs
    sendMessageDlq: createQueue(QUEUE_NAMES.SEND_MESSAGE_DLQ),
    campaignDlq: createQueue(QUEUE_NAMES.CAMPAIGN_DLQ),
    incomingMessageDlq: createQueue(QUEUE_NAMES.INCOMING_MESSAGE_DLQ),
    aiProcessingDlq: createQueue(QUEUE_NAMES.AI_PROCESSING_DLQ),
  };
}
