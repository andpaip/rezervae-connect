import 'dotenv/config';
import { createLogger } from '@rezervae-connect/shared';
import { WPPConnectProvider, SessionManager } from '@rezervae-connect/providers';
import { setProvider, setSessionManager } from './registry.js';
import { createSendMessageWorker } from './workers/send-message.worker.js';
import { createIncomingMessageWorker } from './workers/incoming-message.worker.js';
import { createCampaignSendWorker, bootstrapCampaigns } from './workers/campaign-send.worker.js';
import { createReconnectWorker } from './workers/reconnect.worker.js';
import { createWebhookDeliveryWorker } from './workers/webhook-delivery.worker.js';
import { createAIProcessingWorker } from './workers/ai-processing.worker.js';

const logger = createLogger('workers');

// Initialize provider + session manager
const provider = new WPPConnectProvider();
const sessionManager = new SessionManager(provider);
setProvider(provider);
setSessionManager(sessionManager);

// Start all workers
const workers = [
  createSendMessageWorker(),
  createIncomingMessageWorker(),
  createCampaignSendWorker(),
  createReconnectWorker(),
  createWebhookDeliveryWorker(),
  createAIProcessingWorker(),
];

// Start heartbeat
sessionManager.startHeartbeat();

logger.info({ workerCount: workers.length }, 'Rezervae Connect Workers ready');
logger.info('Waiting for jobs...');

// Bootstrap running campaigns
bootstrapCampaigns().catch((err) => {
  logger.error({ err }, 'Campaign bootstrap failed');
});

// Graceful shutdown
async function shutdown(signal: string) {
  logger.info({ signal }, 'Shutting down gracefully');

  // Close all workers
  await Promise.allSettled(workers.map((w) => w.close()));

  // Shutdown session manager (disconnects all sessions, stops heartbeat)
  await sessionManager.shutdown();

  logger.info('All workers closed');
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
