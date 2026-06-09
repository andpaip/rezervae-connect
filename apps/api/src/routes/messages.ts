import type { FastifyPluginAsync } from 'fastify';
import { eq, and, desc, sql, gte, lte } from 'drizzle-orm';
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
  const digits = String(cel).replace(/\D/g, '');
  return digits.startsWith('55') ? digits : `55${digits}`;
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
 * Helper: resolve a connected instance by role (from metadata.roles array).
 * Returns the first connected instance matching the role, or undefined.
 */
async function resolveInstanceByRole(tenantId: string, role: string) {
  const instances = await db
    .select()
    .from(whatsappInstances)
    .where(
      and(
        eq(whatsappInstances.tenantId, tenantId),
        eq(whatsappInstances.status, 'connected'),
      ),
    );

  return instances.find(i => {
    const roles = (i.metadata as Record<string, unknown> | null)?.roles;
    return Array.isArray(roles) && roles.includes(role);
  });
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
  delayMs?: number;
  scheduledFor?: Date;
  sourceType?: string;
  priority?: number;
}) {
  const isScheduled = opts.delayMs != null && opts.delayMs > 0;

  const [log] = await db.insert(messageLogs).values({
    tenantId: opts.tenantId,
    instanceId: opts.instanceId,
    channel: 'whatsapp',
    direction: 'outbound',
    status: isScheduled ? 'scheduled' : 'queued',
    recipient: opts.to,
    payload: opts.payload ?? {},
    traceId: opts.traceId,
    correlationId: opts.correlationId,
    sourceType: opts.sourceType ?? null,
    scheduledFor: opts.scheduledFor ?? null,
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
    ...(isScheduled ? { delay: opts.delayMs } : {}),
    ...(opts.priority ? { priority: opts.priority } : {}),
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

    // Resolve instance: role-based → settings-based → legacy name-based
    const instance =
      await resolveInstanceByRole(tenantId, 'confirmacao') ??
      await resolveSendInstance(tenantId, request.tenant.settings);
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
        to: phone, content: '', type: 'list',
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
          to: phone, content: '', type: 'text',
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
          to: phone, content: '', type: 'text',
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
      to: phone, content: '', type: 'image',
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
      to: phone, content: '', type: 'image',
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
      to: phone, content: '', type: 'image',
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
      to: phone, content: '', type: 'image',
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
      to: phone, content: '', type: 'image',
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
      to: phone, content: '', type: 'image',
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

    const logId = await enqueueMessage({
      tenantId,
      instanceId: instance.id,
      sessionName: instance.sessionName,
      to: phone,
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

  /**
   * GET /api/v1/messages
   * List message logs for the tenant (Envios panel).
   */
  fastify.get<{
    Querystring: {
      status?: string;
      direction?: string;
      source?: string;
      from?: string;
      to?: string;
      limit?: string;
      offset?: string;
    };
  }>('/api/v1/messages', async (request) => {
    const { tenantId } = request.tenant;
    const { status, direction, source, from, to, limit: limitStr, offset: offsetStr } = request.query ?? {};
    const limit = Math.min(Number(limitStr) || 50, 200);
    const offset = Number(offsetStr) || 0;

    const conditions = [eq(messageLogs.tenantId, tenantId)];

    if (status) conditions.push(eq(messageLogs.status, status));
    if (direction) conditions.push(eq(messageLogs.direction, direction));
    if (source) conditions.push(sql`${messageLogs.payload}->>'templateSlug' = ${source}`);
    if (from) conditions.push(gte(messageLogs.createdAt, new Date(from)));
    if (to) conditions.push(lte(messageLogs.createdAt, new Date(to)));

    const rows = await db.select({
      id: messageLogs.id,
      recipient: messageLogs.recipient,
      direction: messageLogs.direction,
      status: messageLogs.status,
      sourceType: messageLogs.sourceType,
      scheduledFor: messageLogs.scheduledFor,
      error: messageLogs.error,
      payload: messageLogs.payload,
      queuedAt: messageLogs.queuedAt,
      sentAt: messageLogs.sentAt,
      createdAt: messageLogs.createdAt,
    }).from(messageLogs)
      .where(and(...conditions))
      .orderBy(desc(messageLogs.createdAt))
      .limit(limit)
      .offset(offset);

    return { data: rows, meta: { limit, offset } };
  });

  // ─── Scheduling helpers ───────────────────────────────────────────

  function addJitterMinutes(date: Date, maxMinutes = 10): Date {
    const jitter = Math.floor(Math.random() * maxMinutes * 60 * 1000);
    return new Date(date.getTime() + jitter);
  }

  function clampToSendWindow(date: Date, settings?: Record<string, unknown>): Date {
    const startHour = parseWindowHour(settings?.sendWindowStart as string | undefined, 8);
    const endHour = parseWindowHour(settings?.sendWindowEnd as string | undefined, 20);
    const clamped = new Date(date);
    if (clamped.getHours() < startHour) clamped.setHours(startHour, 0, 0, 0);
    if (clamped.getHours() >= endHour) clamped.setHours(endHour, 0, 0, 0);
    return clamped;
  }

  function parseWindowHour(val: string | undefined, fallback: number): number {
    if (!val) return fallback;
    const h = parseInt(val.split(':')[0], 10);
    return isNaN(h) ? fallback : h;
  }

  // ─── POST /api/v1/messages/schedule-confirmation ──────────────────

  interface ScheduleConfirmationItem {
    cel: string;
    nome: string;
    data_atendimento: string;
    horario: string;
    descricao: string;
    id_comanda: string;
    starts_at_iso: string;
  }

  fastify.post<{ Body: { items: ScheduleConfirmationItem[] } }>('/api/v1/messages/schedule-confirmation', async (request, reply) => {
    const { tenantId, traceId, correlationId, settings } = request.tenant;
    const items = request.body?.items;
    if (!Array.isArray(items) || items.length === 0) {
      return reply.code(400).send({ error: 'items array required' });
    }

    const instance =
      await resolveInstanceByRole(tenantId, 'confirmacao') ??
      await resolveSendInstance(tenantId, settings);
    if (!instance) {
      return reply.code(503).send({ error: 'No send instance available' });
    }

    let totalQueued = 0;

    for (const item of items) {
      if (!item.cel || !item.nome || !item.starts_at_iso || !item.id_comanda) continue;
      const phone = cleanPhone(item.cel);
      const startsAt = new Date(item.starts_at_iso);
      const hoursUntil = (startsAt.getTime() - Date.now()) / 3_600_000;

      // Threshold: < 2h → no jobs (client just interacted)
      if (hoursUntil < 2) continue;

      // Threshold: >= 24h → confirmation 24h before
      if (hoursUntil >= 24) {
        const confirm24h = clampToSendWindow(
          addJitterMinutes(new Date(startsAt.getTime() - 24 * 3_600_000)),
          settings,
        );
        await enqueueMessage({
          tenantId, instanceId: instance.id, sessionName: instance.sessionName,
          to: phone, content: '', type: 'list',
          traceId, correlationId: item.id_comanda,
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
          delayMs: Math.max(0, confirm24h.getTime() - Date.now()),
          scheduledFor: confirm24h,
          sourceType: 'confirmation',
          priority: 1,
        });
        totalQueued++;
      }

      // Reminder 2h (always created when hoursUntil >= 2)
      const reminder2h = clampToSendWindow(
        addJitterMinutes(new Date(startsAt.getTime() - 2 * 3_600_000)),
        settings,
      );
      const firstName = item.nome.split(' ')[0];
      await enqueueMessage({
        tenantId, instanceId: instance.id, sessionName: instance.sessionName,
        to: phone,
        content: `Oi ${firstName}! 😊 Seu horário é daqui a 2h, às ${item.horario}. Te esperamos! 💕`,
        type: 'text',
        traceId, correlationId: item.id_comanda,
        payload: { ...item, templateSlug: 'reminder-2h' },
        delayMs: Math.max(0, reminder2h.getTime() - Date.now()),
        scheduledFor: reminder2h,
        sourceType: 'reminder-2h',
        priority: 2,
      });
      totalQueued++;
    }

    logger.info({ tenantId, totalQueued, items: items.length }, 'Scheduled confirmations');
    return reply.code(202).send({ message: 'Confirmations scheduled', queued: totalQueued });
  });

  // ─── POST /api/v1/messages/schedule-absence ───────────────────────

  interface ScheduleAbsenceItem {
    customer_id: string;
    nome: string;
    cel: string;
    last_visit_time: string;
    days: number;
    templateSlug?: string;
    instanceId?: string;
  }

  fastify.post<{ Body: { items: ScheduleAbsenceItem[] } }>('/api/v1/messages/schedule-absence', async (request, reply) => {
    const { tenantId, traceId, correlationId, settings } = request.tenant;
    const items = request.body?.items;
    if (!Array.isArray(items) || items.length === 0) {
      return reply.code(400).send({ error: 'items array required' });
    }

    // Default instance (can be overridden per item)
    const defaultInstance =
      await resolveInstanceByRole(tenantId, 'ausencia') ??
      await resolveSendInstance(tenantId, settings);

    let totalQueued = 0;

    for (const item of items) {
      if (!item.cel || !item.nome || !item.last_visit_time) continue;
      const phone = cleanPhone(item.cel);

      // Parse last visit time and clamp to send window
      const [h, m] = item.last_visit_time.split(':').map(Number);
      const base = new Date();
      base.setHours(h || 10, m || 0, 0, 0);
      const scheduledFor = clampToSendWindow(addJitterMinutes(base), settings);
      const delayMs = Math.max(0, scheduledFor.getTime() - Date.now());

      // Resolve instance (per-item override or default)
      let instance = defaultInstance;
      if (item.instanceId) {
        const [specific] = await db.select().from(whatsappInstances).where(
          and(eq(whatsappInstances.tenantId, tenantId), eq(whatsappInstances.id, item.instanceId)),
        );
        if (specific) instance = specific;
      }
      if (!instance) continue;

      const firstName = item.nome.split(' ')[0];
      const days = item.days || 30;
      const content = renderAbsenceTemplate(item.templateSlug ?? 'ausencia', firstName, days);

      await enqueueMessage({
        tenantId, instanceId: instance.id, sessionName: instance.sessionName,
        to: phone, content, type: 'text',
        traceId, correlationId: item.customer_id,
        payload: { customerId: item.customer_id, days, templateSlug: item.templateSlug ?? 'ausencia' },
        delayMs,
        scheduledFor,
        sourceType: 'absence',
        priority: 3,
      });
      totalQueued++;
    }

    logger.info({ tenantId, totalQueued, items: items.length }, 'Scheduled absence reminders');
    return reply.code(202).send({ message: 'Absences scheduled', queued: totalQueued });
  });

  // ─── POST /api/v1/messages/cancel-scheduled ───────────────────────

  fastify.post<{ Body: { correlationId: string } }>('/api/v1/messages/cancel-scheduled', async (request, reply) => {
    const { tenantId } = request.tenant;
    const { correlationId: corrId } = request.body ?? {};
    if (!corrId) {
      return reply.code(400).send({ error: 'correlationId required' });
    }

    const updated = await db.update(messageLogs)
      .set({ status: 'cancelled' })
      .where(and(
        eq(messageLogs.correlationId, corrId),
        eq(messageLogs.status, 'scheduled'),
        eq(messageLogs.tenantId, tenantId),
      ))
      .returning({ id: messageLogs.id });

    logger.info({ tenantId, correlationId: corrId, cancelled: updated.length }, 'Cancelled scheduled messages');
    return { cancelled: updated.length };
  });
};

function renderAbsenceTemplate(slug: string, firstName: string, days: number): string {
  switch (slug) {
    case 'ausencia-saudade':
      return `Oi ${firstName}! 🥰 Faz ${days} dias que não nos vemos... Estamos com saudade! Que tal agendar um horário? Responda essa mensagem e marcamos pra você! 💕`;
    case 'ausencia-promo':
      return `Oi ${firstName}! Faz tempo que você não aparece por aqui... Temos novidades e promoções especiais esperando por você! 🎉 Responda para agendar.`;
    default:
      return `Oi ${firstName}! Faz ${days} dias que não nos vemos. Estamos com saudade! Quando quiser, estamos aqui para te atender. 💕`;
  }
}

export default messageRoutes;
