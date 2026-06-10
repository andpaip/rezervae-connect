import { Worker, type Job } from 'bullmq';
import { eq, and, asc, sql } from 'drizzle-orm';
import { getRedisConnectionOptions, getQueues, QUEUE_NAMES } from '@rezervae-connect/queue';
import { db, campaigns, campaignRecipients, messageLogs, whatsappInstances, messageTemplates, auditLogs } from '@rezervae-connect/database';
import { eventBus } from '@rezervae-connect/events';
import { createLogger, createTraceContext, renderTemplate } from '@rezervae-connect/shared';

const logger = createLogger('campaign-send-worker');

export interface CampaignProcessJob {
  tenantId: string;
  campaignId: string;
  traceId: string;
  correlationId: string;
}

/**
 * Count messages sent today for this campaign (daily limit check).
 */
async function countSentToday(campaignId: string): Promise<number> {
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const [result] = await db
    .select({ count: sql<number>`count(*)::int` })
    .from(campaignRecipients)
    .where(
      and(
        eq(campaignRecipients.campaignId, campaignId),
        eq(campaignRecipients.status, 'sent'),
        sql`${campaignRecipients.sentAt} >= ${today}`,
      ),
    );

  return result?.count ?? 0;
}

/**
 * Milliseconds until midnight (next day reset for daily limit).
 */
function msUntilMidnight(): number {
  const now = new Date();
  const midnight = new Date(now);
  midnight.setHours(24, 0, 0, 0);
  return midnight.getTime() - now.getTime();
}

/**
 * Count total stats for campaign progress event.
 */
async function getCampaignStats(campaignId: string) {
  const rows = await db
    .select({
      status: campaignRecipients.status,
      count: sql<number>`count(*)::int`,
    })
    .from(campaignRecipients)
    .where(eq(campaignRecipients.campaignId, campaignId))
    .groupBy(campaignRecipients.status);

  const counts: Record<string, number> = {};
  let total = 0;
  for (const r of rows) {
    counts[r.status] = r.count;
    total += r.count;
  }

  return {
    sent: counts['sent'] ?? 0,
    errors: counts['error'] ?? 0,
    ignored: counts['ignored'] ?? 0,
    total,
  };
}

async function processCampaignSend(job: Job<CampaignProcessJob>): Promise<void> {
  const { tenantId, campaignId, traceId, correlationId } = job.data;
  const ctx = { tenantId, campaignId, traceId, correlationId, jobId: job.id };

  logger.info(ctx, 'Processing campaign job');

  // Get campaign
  const [campaign] = await db.select().from(campaigns).where(eq(campaigns.id, campaignId));
  if (!campaign || campaign.status !== 'running') {
    logger.info(ctx, 'Campaign not running, skipping');
    return;
  }

  // Get next pending recipient
  const [recipient] = await db.select()
    .from(campaignRecipients)
    .where(
      and(
        eq(campaignRecipients.campaignId, campaignId),
        eq(campaignRecipients.status, 'pending'),
      ),
    )
    .orderBy(asc(campaignRecipients.sortOrder))
    .limit(1);

  if (!recipient) {
    // Campaign finished
    const stats = await getCampaignStats(campaignId);
    logger.info({ ...ctx, stats }, 'Campaign finished — no more recipients');

    await db.update(campaigns).set({
      status: 'finished',
      finishedAt: new Date(),
      stats,
    }).where(eq(campaigns.id, campaignId));

    await db.insert(auditLogs).values({
      tenantId, actor: 'worker', entityType: 'campaign', entityId: campaignId,
      action: 'finished', newState: { stats },
      metadata: { traceId, correlationId },
    });

    eventBus.emit({
      type: 'campaign.finished',
      tenantId, traceId, correlationId,
      timestamp: new Date().toISOString(), version: '1.0',
      data: { campaignId, stats },
    });

    return;
  }

  // Check daily limit
  const config = campaign.config as { dailyLimit?: number; intervalMinMs?: number; intervalMaxMs?: number } | null;
  const dailyLimit = config?.dailyLimit ?? 0;

  if (dailyLimit > 0) {
    const sentToday = await countSentToday(campaignId);
    if (sentToday >= dailyLimit) {
      const delay = msUntilMidnight() + 60_000; // midnight + 1min buffer
      logger.info({ ...ctx, sentToday, dailyLimit, delayMs: delay }, 'Daily limit reached, delaying until tomorrow');

      const queues = getQueues();
      await queues.campaignSend.add('campaign-process', job.data, { delay });

      await db.insert(auditLogs).values({
        tenantId, actor: 'worker', entityType: 'campaign', entityId: campaignId,
        action: 'daily_limit_reached',
        newState: { sentToday, dailyLimit, delayMs: delay },
        metadata: { traceId, correlationId },
      });
      return;
    }
  }

  // Validate recipient phone
  const phone = String(recipient.customerPhone).replace(/\D/g, '');
  if (!phone || phone.length < 10) {
    await db.update(campaignRecipients).set({ status: 'ignored', error: 'Invalid phone number' })
      .where(eq(campaignRecipients.id, recipient.id));

    logger.warn({ ...ctx, phone: recipient.customerPhone }, 'Invalid phone, marking ignored');

    // Re-enqueue immediately for next recipient
    const queues = getQueues();
    await queues.campaignSend.add('campaign-process', job.data);
    return;
  }

  // Resolve instance session name
  const [instance] = campaign.instanceId
    ? await db.select().from(whatsappInstances).where(eq(whatsappInstances.id, campaign.instanceId))
    : [];
  const sessionName = instance?.sessionName ?? '';

  // Resolve template content
  let content = '';
  if (campaign.templateId) {
    const [template] = await db.select().from(messageTemplates).where(eq(messageTemplates.id, campaign.templateId));
    if (template) {
      content = renderTemplate(template.content, {
        nome: recipient.customerName ?? '',
        celular: recipient.customerPhone,
        ...(recipient.metadata as Record<string, string> ?? {}),
      });
    }
  }

  // Create message log
  const [msgLog] = await db.insert(messageLogs).values({
    tenantId,
    instanceId: campaign.instanceId,
    channel: 'whatsapp',
    direction: 'outbound',
    templateId: campaign.templateId,
    status: 'queued',
    recipient: `55${phone}`,
    payload: { campaignId, recipientId: recipient.id },
    traceId, correlationId,
    queuedAt: new Date(),
  }).returning();

  // Enqueue send-message
  const queues = getQueues();
  await queues.sendMessage.add('campaign-msg', {
    tenantId,
    instanceId: campaign.instanceId ?? '',
    sessionName,
    messageLogId: msgLog.id,
    to: `55${phone}`,
    content,
    type: 'text',
    traceId,
    correlationId,
  }, {
    attempts: 3,
    backoff: { type: 'exponential', delay: 15_000 },
  });

  // Mark recipient as sent
  await db.update(campaignRecipients).set({
    status: 'sent',
    sentAt: new Date(),
    attempts: (recipient.attempts ?? 0) + 1,
  }).where(eq(campaignRecipients.id, recipient.id));

  // Update campaign stats
  const stats = await getCampaignStats(campaignId);
  await db.update(campaigns).set({ stats }).where(eq(campaigns.id, campaignId));

  // Emit progress
  eventBus.emit({
    type: 'campaign.progress',
    tenantId, traceId, correlationId,
    timestamp: new Date().toISOString(), version: '1.0',
    data: { campaignId, sent: stats.sent, total: stats.total, errors: stats.errors },
  });

  // Audit
  await db.insert(auditLogs).values({
    tenantId, actor: 'worker', entityType: 'campaign', entityId: campaignId,
    action: 'recipient_sent',
    newState: { recipientId: recipient.id, phone: recipient.customerPhone },
    metadata: { traceId, correlationId },
  });

  // Re-enqueue self with random delay (throttling)
  const minDelay = config?.intervalMinMs ?? 30_000;  // 30s min (legacy: 30s)
  const maxDelay = config?.intervalMaxMs ?? 90_000;  // 90s max (legacy: 90s)
  const delay = Math.floor(Math.random() * (maxDelay - minDelay)) + minDelay;

  await queues.campaignSend.add('campaign-process', job.data, { delay });
  logger.info({ ...ctx, delayMs: delay, progress: `${stats.sent}/${stats.total}` }, 'Re-enqueued campaign with delay');
}

/**
 * Bootstrap: on startup, find running campaigns and enqueue their process jobs.
 */
export async function bootstrapCampaigns(): Promise<void> {
  const runningCampaigns = await db.select().from(campaigns).where(eq(campaigns.status, 'running'));

  if (runningCampaigns.length === 0) {
    logger.info('No running campaigns to bootstrap');
    return;
  }

  const queues = getQueues();

  for (const campaign of runningCampaigns) {
    const trace = createTraceContext();
    await queues.campaignSend.add('campaign-process', {
      tenantId: campaign.tenantId,
      campaignId: campaign.id,
      traceId: trace.traceId,
      correlationId: trace.correlationId,
    });

    logger.info({ campaignId: campaign.id, tenantId: campaign.tenantId }, 'Bootstrapped running campaign');
  }

  logger.info({ count: runningCampaigns.length }, 'Campaign bootstrap complete');
}

export function createCampaignSendWorker() {
  const worker = new Worker<CampaignProcessJob>(
    QUEUE_NAMES.CAMPAIGN_SEND,
    processCampaignSend,
    {
      connection: getRedisConnectionOptions(),
      concurrency: 3,
    },
  );

  worker.on('failed', async (job, err) => {
    logger.error({ jobId: job?.id, err: err.message }, 'campaign-send job failed');

    if (job && (job.attemptsMade >= (job.opts.attempts ?? 3))) {
      const queues = getQueues();
      await queues.campaignDlq.add('dlq', {
        originalJob: job.data,
        error: err.message,
        failedAt: new Date().toISOString(),
        attempts: job.attemptsMade,
      });
    }
  });

  worker.on('completed', (job) => {
    logger.debug({ jobId: job.id }, 'campaign-send job completed');
  });

  return worker;
}
