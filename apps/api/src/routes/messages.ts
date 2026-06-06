import type { FastifyPluginAsync } from 'fastify';
import { eq, and } from 'drizzle-orm';
import { db, messageLogs, whatsappInstances, auditLogs } from '@rezervae-connect/database';
import { getQueues } from '@rezervae-connect/queue';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('messages-route');

interface ConfirmationItem {
  id_cliente: string;
  nome: string;
  data_atendimento: string;
  horario: string;
  descricao: string;
  cel: string;
  id_comanda: string;
  faltas?: number;
  pacotes?: number;
  vouchers?: number;
  promocao?: boolean;
}

interface AusenciaItem {
  cel: string;
  nome: string;
  nivel: number;
}

interface SendConfirmationBody {
  dados: Array<{
    confirmacao?: ConfirmationItem[];
    ausencias?: { dados: AusenciaItem[]; msgs: Record<string, string> };
    natendidos?: { dados: AusenciaItem[]; msgs: Array<{ msg: string }> };
  }>;
  session?: string;
}

interface SingleMessageBody {
  dados: Record<string, unknown>;
  session?: string;
}

function cleanPhone(cel: string): string {
  return String(cel).replace(/\D/g, '');
}

/**
 * Helper: resolve the operational "send" instance for a tenant.
 * Priority: tenant settings defaultSendInstanceId → fallback instanceName='send'.
 */
async function resolveSendInstance(tenantId: string, settings?: Record<string, unknown>) {
  const defaultId = settings?.defaultSendInstanceId as string | undefined;

  if (defaultId) {
    const [instance] = await db
      .select()
      .from(whatsappInstances)
      .where(
        and(
          eq(whatsappInstances.tenantId, tenantId),
          eq(whatsappInstances.id, defaultId),
        ),
      );
    if (instance) return instance;
  }

  // Fallback: legacy convention instanceName='send'
  const [instance] = await db
    .select()
    .from(whatsappInstances)
    .where(
      and(
        eq(whatsappInstances.tenantId, tenantId),
        eq(whatsappInstances.instanceName, 'send'),
      ),
    );
  return instance;
}

/**
 * Helper: create a message log and enqueue a send-message job.
 */
async function enqueueMessage(opts: {
  tenantId: string;
  instanceId: string;
  sessionName: string;
  to: string;
  content: string;
  type: 'text' | 'image' | 'list';
  traceId: string;
  correlationId: string;
  imageUrl?: string;
  caption?: string;
  buttonText?: string;
  sections?: Array<{
    title: string;
    rows: Array<{ rowId: string; title: string; description?: string }>;
  }>;
  templateSlug?: string;
  payload?: Record<string, unknown>;
}) {
  const [log] = await db.insert(messageLogs).values({
    tenantId: opts.tenantId,
    instanceId: opts.instanceId,
    channel: 'whatsapp',
    direction: 'outbound',
    status: 'queued',
    recipient: opts.to,
    payload: opts.payload ?? {},
    traceId: opts.traceId,
    correlationId: opts.correlationId,
    queuedAt: new Date(),
  }).returning();

  const queues = getQueues();
  await queues.sendMessage.add('send', {
    tenantId: opts.tenantId,
    instanceId: opts.instanceId,
    sessionName: opts.sessionName,
    messageLogId: log.id,
    to: opts.to,
    content: opts.content,
    type: opts.type,
    imageUrl: opts.imageUrl,
    caption: opts.caption,
    buttonText: opts.buttonText,
    sections: opts.sections,
    traceId: opts.traceId,
    correlationId: opts.correlationId,
  }, {
    attempts: 3,
    backoff: { type: 'exponential', delay: 15_000 },
  });

  return log.id;
}

const messageRoutes: FastifyPluginAsync = async (fastify) => {
  /**
   * POST /api/v1/messages/send-confirmation
   * Batch: confirmações + ausências + não atendidas.
   * Returns 202 immediately — jobs handle actual sending.
   */
  fastify.post<{ Body: SendConfirmationBody }>('/api/v1/messages/send-confirmation', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const bodyRoot = Array.isArray(request.body?.dados) ? request.body.dados[0] : {};
    const confirmacao = Array.isArray(bodyRoot?.confirmacao) ? bodyRoot.confirmacao : [];
    const ausencias = bodyRoot?.ausencias;
    const naoAtendidas = bodyRoot?.natendidos;

    const hasConfirmacao = confirmacao.length > 0;
    const hasAusencias = Array.isArray(ausencias?.dados) && ausencias.dados.length > 0;
    const hasNaoAtendidas = naoAtendidas && Array.isArray(naoAtendidas?.dados) && naoAtendidas.dados.length > 0;

    if (!hasConfirmacao && !hasAusencias && !hasNaoAtendidas) {
      return reply.code(400).send({ error: 'Corpo da requisição inválido ou vazio' });
    }

    const instance = await resolveSendInstance(tenantId, request.tenant.settings);
    if (!instance) {
      return reply.code(503).send({ error: 'No send instance available' });
    }

    const queued = { confirmacao: 0, ausencias: 0, naoAtendidas: 0 };

    // Confirmations → list messages
    for (const item of confirmacao) {
      if (!item.cel || !item.nome || !item.data_atendimento || !item.horario || !item.descricao || !item.id_comanda) continue;
      const phone = cleanPhone(item.cel);
      await enqueueMessage({
        tenantId, instanceId: instance.id, sessionName: instance.sessionName,
        to: `55${phone}`, content: '', type: 'list',
        traceId, correlationId, templateSlug: 'confirmation',
        buttonText: 'Confirmar Presença',
        sections: [{
          title: 'Opções',
          rows: [
            { rowId: `ok:${item.id_comanda}`, title: '✅ Confirmar presença' },
            { rowId: `ed:${item.id_comanda}`, title: '📅 Reagendar' },
            { rowId: `close:${item.id_comanda}`, title: '❌ Cancelar' },
          ],
        }],
        payload: { ...item, templateSlug: 'confirmation' },
      });
      queued.confirmacao++;
    }

    // Ausências → text messages
    if (hasAusencias) {
      for (const item of ausencias!.dados) {
        if (!item.cel || !item.nome) continue;
        const phone = cleanPhone(item.cel);
        await enqueueMessage({
          tenantId, instanceId: instance.id, sessionName: instance.sessionName,
          to: `55${phone}`, content: '', type: 'text',
          traceId, correlationId, templateSlug: 'ausencia',
          payload: { ...item, msgs: ausencias!.msgs, templateSlug: 'ausencia' },
        });
        queued.ausencias++;
      }
    }

    // Não atendidas → text messages
    if (hasNaoAtendidas) {
      for (const item of naoAtendidas!.dados) {
        if (!item.cel || !item.nome) continue;
        const phone = cleanPhone(item.cel);
        await enqueueMessage({
          tenantId, instanceId: instance.id, sessionName: instance.sessionName,
          to: `55${phone}`, content: '', type: 'text',
          traceId, correlationId, templateSlug: 'nao-atendida',
          payload: { ...item, msgs: naoAtendidas!.msgs, templateSlug: 'nao-atendida' },
        });
        queued.naoAtendidas++;
      }
    }

    await db.insert(auditLogs).values({
      tenantId, actor: 'api', entityType: 'message', action: 'batch_queued',
      newState: { queued },
      metadata: { traceId, correlationId },
    });

    return reply.code(202).send({ message: 'Messages queued', queued });
  });

  /**
   * POST /api/v1/messages/send-schedule
   */
  fastify.post<{ Body: SingleMessageBody }>('/api/v1/messages/send-schedule', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const dados = request.body?.dados ?? {};
    const cliente = (dados as Record<string, unknown>).cliente as Record<string, unknown> | undefined;

    if (!cliente?.celular || !cliente?.nome || !cliente?.data) {
      return reply.code(400).send({ error: 'Dados do cliente incompletos' });
    }

    const instance = await resolveSendInstance(tenantId, request.tenant.settings);
    if (!instance) return reply.code(503).send({ error: 'No send instance available' });

    const phone = cleanPhone(cliente.celular as string);
    const logId = await enqueueMessage({
      tenantId, instanceId: instance.id, sessionName: instance.sessionName,
      to: `55${phone}`, content: '', type: 'image',
      imageUrl: 'schedule.png', caption: '',
      traceId, correlationId, templateSlug: 'schedule',
      payload: { ...dados, templateSlug: 'schedule' },
    });

    return reply.code(202).send({ message: 'Message queued', messageLogId: logId });
  });

  /**
   * POST /api/v1/messages/send-pesquisa
   */
  fastify.post<{ Body: SingleMessageBody }>('/api/v1/messages/send-pesquisa', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const dados = request.body?.dados ?? {};
    const { nome, celular } = dados as { nome?: string; celular?: string };

    if (!nome || !celular) {
      return reply.code(400).send({ error: 'Dados do cliente incompletos' });
    }

    const instance = await resolveSendInstance(tenantId, request.tenant.settings);
    if (!instance) return reply.code(503).send({ error: 'No send instance available' });

    const phone = cleanPhone(celular);
    const logId = await enqueueMessage({
      tenantId, instanceId: instance.id, sessionName: instance.sessionName,
      to: `55${phone}`, content: '', type: 'image',
      imageUrl: 'rating.png', caption: '',
      traceId, correlationId, templateSlug: 'pesquisa',
      payload: { nome, celular, templateSlug: 'pesquisa' },
    });

    return reply.code(202).send({ message: 'Message queued', messageLogId: logId });
  });

  /**
   * POST /api/v1/messages/send-app-confirmation
   */
  fastify.post<{ Body: SingleMessageBody }>('/api/v1/messages/send-app-confirmation', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const dados = request.body?.dados ?? {};
    const { nome, itens, data, celular } = dados as { nome?: string; itens?: unknown; data?: string; celular?: string };

    if (!nome || !itens || !data) {
      return reply.code(400).send({ error: 'Dados do cliente incompletos' });
    }

    const instance = await resolveSendInstance(tenantId, request.tenant.settings);
    if (!instance) return reply.code(503).send({ error: 'No send instance available' });

    const phone = cleanPhone(celular ?? '');
    const logId = await enqueueMessage({
      tenantId, instanceId: instance.id, sessionName: instance.sessionName,
      to: `55${phone}`, content: '', type: 'image',
      imageUrl: 'app.png', caption: '',
      traceId, correlationId, templateSlug: 'app-confirmation',
      payload: { ...dados, templateSlug: 'app-confirmation' },
    });

    return reply.code(202).send({ message: 'Message queued', messageLogId: logId });
  });

  /**
   * POST /api/v1/messages/send-voucher-confirmation
   */
  fastify.post<{ Body: SingleMessageBody }>('/api/v1/messages/send-voucher-confirmation', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const dados = request.body?.dados ?? {};
    const { profissional, validade, aplicacao, nome, celular, id_servico, descricao } = dados as Record<string, string>;

    if (!profissional || !validade || !aplicacao || !nome || !celular || !id_servico || !descricao) {
      return reply.code(400).send({ error: 'Dados do cliente incompletos' });
    }

    const instance = await resolveSendInstance(tenantId, request.tenant.settings);
    if (!instance) return reply.code(503).send({ error: 'No send instance available' });

    const phone = cleanPhone(celular);
    const logId = await enqueueMessage({
      tenantId, instanceId: instance.id, sessionName: instance.sessionName,
      to: `55${phone}`, content: '', type: 'image',
      imageUrl: 'voucher.png', caption: '',
      traceId, correlationId, templateSlug: 'voucher-confirmation',
      payload: { ...dados, templateSlug: 'voucher-confirmation' },
    });

    return reply.code(202).send({ message: 'Message queued', messageLogId: logId });
  });

  /**
   * POST /api/v1/messages/send-reset-senha
   */
  fastify.post<{ Body: SingleMessageBody }>('/api/v1/messages/send-reset-senha', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const dados = request.body?.dados ?? {};
    const { nome, celular, token, cliente_id } = dados as Record<string, string>;

    if (!token || !cliente_id || !nome || !celular) {
      return reply.code(400).send({ error: 'Dados do cliente incompletos' });
    }

    const instance = await resolveSendInstance(tenantId, request.tenant.settings);
    if (!instance) return reply.code(503).send({ error: 'No send instance available' });

    const phone = cleanPhone(celular);
    const logId = await enqueueMessage({
      tenantId, instanceId: instance.id, sessionName: instance.sessionName,
      to: `55${phone}`, content: '', type: 'image',
      imageUrl: 'schedule.png', caption: '',
      traceId, correlationId, templateSlug: 'reset-senha',
      payload: { nome, celular, token, cliente_id, templateSlug: 'reset-senha' },
    });

    return reply.code(202).send({ message: 'Message queued', messageLogId: logId });
  });

  /**
   * POST /api/v1/messages/send-local
   */
  fastify.post<{ Body: SingleMessageBody }>('/api/v1/messages/send-local', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const dados = request.body?.dados ?? {};
    const { cliente, data, nome, celular } = dados as Record<string, string>;

    if (!cliente || !nome || !data || !celular) {
      return reply.code(400).send({ error: 'Dados do cliente incompletos' });
    }

    const instance = await resolveSendInstance(tenantId, request.tenant.settings);
    if (!instance) return reply.code(503).send({ error: 'No send instance available' });

    const phone = cleanPhone(celular);
    const logId = await enqueueMessage({
      tenantId, instanceId: instance.id, sessionName: instance.sessionName,
      to: `55${phone}`, content: '', type: 'image',
      imageUrl: 'local.png', caption: '',
      traceId, correlationId, templateSlug: 'local',
      payload: { ...dados, templateSlug: 'local' },
    });

    return reply.code(202).send({ message: 'Message queued', messageLogId: logId });
  });

  /**
   * POST /api/v1/messages/send-test
   * Generic test message: text, image, or list.
   */
  fastify.post<{
    Body: {
      to: string;
      content: string;
      type?: 'text' | 'image' | 'list';
      instanceId?: string;
      imageUrl?: string;
      caption?: string;
      buttonText?: string;
      sections?: Array<{ title: string; rows: Array<{ rowId: string; title: string; description?: string }> }>;
    };
  }>('/api/v1/messages/send-test', async (request, reply) => {
    const { tenantId, traceId, correlationId } = request.tenant;
    const { to, content, type = 'text', instanceId, imageUrl, caption, buttonText, sections } = request.body ?? {};

    if (!to || !content) {
      return reply.code(400).send({ error: 'to and content are required' });
    }

    const phone = cleanPhone(to);
    if (phone.length < 10) {
      return reply.code(400).send({ error: 'Número inválido' });
    }

    // Resolve instance: explicit selection → fallback to first connected
    let instance: typeof whatsappInstances.$inferSelect | undefined;

    if (instanceId) {
      [instance] = await db
        .select()
        .from(whatsappInstances)
        .where(
          and(
            eq(whatsappInstances.tenantId, tenantId),
            eq(whatsappInstances.id, instanceId),
          ),
        );
    } else {
      [instance] = await db
        .select()
        .from(whatsappInstances)
        .where(
          and(
            eq(whatsappInstances.tenantId, tenantId),
            eq(whatsappInstances.status, 'connected'),
          ),
        );
    }

    if (!instance) {
      return reply.code(503).send({ error: 'Nenhuma instância conectada' });
    }

    const fullPhone = phone.startsWith('55') ? phone : `55${phone}`;

    const logId = await enqueueMessage({
      tenantId,
      instanceId: instance.id,
      sessionName: instance.sessionName,
      to: fullPhone,
      content,
      type,
      imageUrl,
      caption,
      buttonText,
      sections,
      traceId,
      correlationId,
      templateSlug: 'test',
      payload: { templateSlug: 'test', testMessage: true, type },
    });

    return reply.code(202).send({ message: 'Test message queued', messageLogId: logId });
  });
};

export default messageRoutes;
