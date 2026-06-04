import type { FastifyPluginAsync } from 'fastify';
import { eq, and } from 'drizzle-orm';
import { db, campaigns, campaignRecipients, auditLogs } from '@rezervae-connect/database';
import { getQueues } from '@rezervae-connect/queue';
import { createTraceContext, createLogger } from '@rezervae-connect/shared';

const logger = createLogger('campaigns-route');

interface CreateCampaignBody {
  name: string;
  templateId?: string;
  instanceId: string;
  config: {
    dailyLimit?: number;
    intervalMinMs?: number;
    intervalMaxMs?: number;
  };
  recipients: Array<{
    customerPhone: string;
    customerName?: string;
    customerExternalId?: string;
    metadata?: Record<string, unknown>;
  }>;
}

const campaignRoutes: FastifyPluginAsync = async (fastify) => {
  // Create campaign
  fastify.post<{ Body: CreateCampaignBody }>('/api/v1/campaigns', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const { name, templateId, instanceId, config, recipients } = request.body;

    if (!name || !instanceId || !recipients?.length) {
      return reply.code(400).send({ error: 'name, instanceId and recipients are required' });
    }

    const [campaign] = await db.insert(campaigns).values({
      tenantId,
      name,
      templateId: templateId ?? null,
      instanceId,
      status: 'draft',
      config,
      stats: { total: recipients.length, sent: 0, errors: 0, ignored: 0 },
    }).returning();

    // Insert recipients
    const recipientValues = recipients.map((r, i) => ({
      campaignId: campaign.id,
      tenantId,
      customerPhone: r.customerPhone,
      customerName: r.customerName ?? null,
      customerExternalId: r.customerExternalId ?? null,
      metadata: r.metadata ?? {},
      sortOrder: i,
      status: 'pending',
    }));

    // Batch insert in chunks of 500
    for (let i = 0; i < recipientValues.length; i += 500) {
      await db.insert(campaignRecipients).values(recipientValues.slice(i, i + 500));
    }

    await db.insert(auditLogs).values({
      tenantId, actor: 'api', entityType: 'campaign', entityId: campaign.id,
      action: 'created',
      newState: { name, recipientCount: recipients.length, config },
      metadata: { traceId, correlationId },
    });

    return reply.code(201).send({ data: campaign });
  });

  // Start campaign (draft → running)
  fastify.post<{ Params: { id: string } }>('/api/v1/campaigns/:id/start', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const [campaign] = await db.select().from(campaigns).where(
      and(eq(campaigns.id, request.params.id), eq(campaigns.tenantId, tenantId)),
    );

    if (!campaign) return reply.code(404).send({ error: 'Campaign not found' });
    if (campaign.status !== 'draft' && campaign.status !== 'paused') {
      return reply.code(409).send({ error: `Cannot start campaign with status: ${campaign.status}` });
    }

    await db.update(campaigns).set({ status: 'running', startedAt: new Date() })
      .where(eq(campaigns.id, campaign.id));

    // Enqueue first campaign-process job
    const queues = getQueues();
    await queues.campaignSend.add('campaign-process', {
      tenantId, campaignId: campaign.id, traceId, correlationId,
    });

    await db.insert(auditLogs).values({
      tenantId, actor: 'api', entityType: 'campaign', entityId: campaign.id,
      action: 'started', metadata: { traceId, correlationId },
    });

    return reply.code(202).send({ message: 'Campaign started', campaignId: campaign.id });
  });

  // Pause campaign
  fastify.post<{ Params: { id: string } }>('/api/v1/campaigns/:id/pause', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const [campaign] = await db.select().from(campaigns).where(
      and(eq(campaigns.id, request.params.id), eq(campaigns.tenantId, tenantId)),
    );

    if (!campaign) return reply.code(404).send({ error: 'Campaign not found' });
    if (campaign.status !== 'running') {
      return reply.code(409).send({ error: 'Campaign is not running' });
    }

    await db.update(campaigns).set({ status: 'paused', pausedAt: new Date() })
      .where(eq(campaigns.id, campaign.id));

    await db.insert(auditLogs).values({
      tenantId, actor: 'api', entityType: 'campaign', entityId: campaign.id,
      action: 'paused', metadata: { traceId, correlationId },
    });

    return reply.code(200).send({ message: 'Campaign paused' });
  });

  // Resume campaign
  fastify.post<{ Params: { id: string } }>('/api/v1/campaigns/:id/resume', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const [campaign] = await db.select().from(campaigns).where(
      and(eq(campaigns.id, request.params.id), eq(campaigns.tenantId, tenantId)),
    );

    if (!campaign) return reply.code(404).send({ error: 'Campaign not found' });
    if (campaign.status !== 'paused') {
      return reply.code(409).send({ error: 'Campaign is not paused' });
    }

    await db.update(campaigns).set({ status: 'running' })
      .where(eq(campaigns.id, campaign.id));

    const queues = getQueues();
    await queues.campaignSend.add('campaign-process', {
      tenantId, campaignId: campaign.id, traceId, correlationId,
    });

    await db.insert(auditLogs).values({
      tenantId, actor: 'api', entityType: 'campaign', entityId: campaign.id,
      action: 'resumed', metadata: { traceId, correlationId },
    });

    return reply.code(202).send({ message: 'Campaign resumed' });
  });

  // Stop campaign
  fastify.post<{ Params: { id: string } }>('/api/v1/campaigns/:id/stop', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const [campaign] = await db.select().from(campaigns).where(
      and(eq(campaigns.id, request.params.id), eq(campaigns.tenantId, tenantId)),
    );

    if (!campaign) return reply.code(404).send({ error: 'Campaign not found' });

    await db.update(campaigns).set({ status: 'canceled' })
      .where(eq(campaigns.id, campaign.id));

    await db.insert(auditLogs).values({
      tenantId, actor: 'api', entityType: 'campaign', entityId: campaign.id,
      action: 'canceled', metadata: { traceId, correlationId },
    });

    return reply.code(200).send({ message: 'Campaign stopped' });
  });

  // Get campaign status
  fastify.get<{ Params: { id: string } }>('/api/v1/campaigns/:id/status', async (request, reply) => {
    const { tenantId } = request.tenant;
    const [campaign] = await db.select().from(campaigns).where(
      and(eq(campaigns.id, request.params.id), eq(campaigns.tenantId, tenantId)),
    );

    if (!campaign) return reply.code(404).send({ error: 'Campaign not found' });

    return { data: campaign };
  });

  // Get campaign recipients
  fastify.get<{ Params: { id: string }; Querystring: { status?: string } }>('/api/v1/campaigns/:id/recipients', async (request, reply) => {
    const { tenantId } = request.tenant;
    const statusFilter = request.query.status;

    let query = db.select().from(campaignRecipients).where(
      and(
        eq(campaignRecipients.campaignId, request.params.id),
        eq(campaignRecipients.tenantId, tenantId),
        ...(statusFilter ? [eq(campaignRecipients.status, statusFilter)] : []),
      ),
    );

    const recipients = await query;
    return { data: recipients };
  });
};

export default campaignRoutes;
