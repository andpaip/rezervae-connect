import { Worker, type Job } from 'bullmq';
import { getRedisConnectionOptions, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, auditLogs } from '@rezervae-connect/database';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('reconnect-worker');

export interface ReconnectJob {
  tenantId: string;
  instanceId: string;
  sessionName: string;
  attempt: number;
  traceId: string;
  correlationId: string;
}

async function processReconnect(job: Job<ReconnectJob>): Promise<void> {
  const { tenantId, instanceId, sessionName, attempt, traceId, correlationId } = job.data;
  const ctx = { tenantId, instanceId, sessionName, attempt, traceId, correlationId, jobId: job.id };

  logger.info(ctx, 'Processing reconnect job');

  // SessionManager handles session lifecycle.
  // For new connections (no managed session yet), use createSession.
  // For existing sessions, use reconnectSession.
  const { getSessionManager } = await import('../registry.js');
  const sessionManager = getSessionManager();

  const hasSession = sessionManager.hasSession?.(sessionName);
  if (hasSession) {
    await sessionManager.reconnectSession(sessionName);
  } else {
    await sessionManager.createSession(tenantId, instanceId, sessionName);
  }

  await db.insert(auditLogs).values({
    tenantId,
    actor: 'worker',
    entityType: 'instance',
    entityId: instanceId,
    action: 'reconnect_queued',
    newState: { attempt },
    metadata: { traceId, correlationId, jobId: job.id },
  });
}

export function createReconnectWorker() {
  const worker = new Worker<ReconnectJob>(
    QUEUE_NAMES.RECONNECT,
    processReconnect,
    {
      connection: getRedisConnectionOptions(),
      concurrency: 2,
    },
  );

  worker.on('failed', (job, err) => {
    logger.error({ jobId: job?.id, err: err.message }, 'reconnect job failed');
  });

  return worker;
}
