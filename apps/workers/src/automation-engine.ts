import { eq, and } from 'drizzle-orm';
import { db, messageTemplates, whatsappInstances, messageLogs } from '@rezervae-connect/database';
import { getQueues } from '@rezervae-connect/queue';
import { renderTemplate, createLogger, createTraceContext, DEFAULT_RULES, type AutomationRule, type CoreEventPayload } from '@rezervae-connect/shared';

const logger = createLogger('automation-engine');

export interface CoreEventJob {
  eventType: string;
  eventId: string;
  tenantId: string;
  payload: CoreEventPayload;
  occurredAt: string;
  traceId: string;
  correlationId: string;
}

export interface AutomationResult {
  ruleId: string;
  matched: boolean;
  sent: boolean;
  reason?: string;
}

/**
 * Process a Core business event against automation rules.
 *
 * For each matching rule:
 * 1. Resolve the template by slug for this tenant
 * 2. Find an active WhatsApp instance for the tenant
 * 3. Render the template with event payload variables
 * 4. Create a message_log entry
 * 5. Enqueue to the send-message queue
 */
export async function processAutomation(event: CoreEventJob): Promise<AutomationResult[]> {
  const matchingRules = DEFAULT_RULES.filter((rule) => rule.eventType === event.eventType);

  if (matchingRules.length === 0) {
    logger.debug({ eventType: event.eventType, tenantId: event.tenantId }, 'No automation rules match');
    return [];
  }

  const results: AutomationResult[] = [];

  for (const rule of matchingRules) {
    const result = await executeRule(rule, event);
    results.push(result);
  }

  return results;
}

async function executeRule(rule: AutomationRule, event: CoreEventJob): Promise<AutomationResult> {
  const ctx = { ruleId: rule.id, eventType: event.eventType, tenantId: event.tenantId };

  // Check condition
  if (rule.condition && !rule.condition(event.payload)) {
    logger.debug(ctx, 'Rule condition not met, skipping');
    return { ruleId: rule.id, matched: true, sent: false, reason: 'condition_not_met' };
  }

  // Extract recipient
  const recipient = rule.extractRecipient(event.payload);
  if (!recipient) {
    logger.warn(ctx, 'No recipient phone found in payload');
    return { ruleId: rule.id, matched: true, sent: false, reason: 'no_recipient' };
  }

  // Resolve template
  const template = await db.select()
    .from(messageTemplates)
    .where(and(
      eq(messageTemplates.tenantId, event.tenantId),
      eq(messageTemplates.slug, rule.templateSlug),
      eq(messageTemplates.isActive, true),
    ))
    .limit(1)
    .then((rows) => rows[0]);

  if (!template) {
    logger.warn({ ...ctx, templateSlug: rule.templateSlug }, 'Template not found or inactive');
    return { ruleId: rule.id, matched: true, sent: false, reason: 'template_not_found' };
  }

  // Resolve instance
  const instance = await db.select()
    .from(whatsappInstances)
    .where(and(
      eq(whatsappInstances.tenantId, event.tenantId),
      eq(whatsappInstances.status, 'connected'),
    ))
    .limit(1)
    .then((rows) => rows[0]);

  if (!instance) {
    logger.warn(ctx, 'No connected WhatsApp instance for tenant');
    return { ruleId: rule.id, matched: true, sent: false, reason: 'no_instance' };
  }

  // Render template
  const variables = rule.extractVariables(event.payload);
  const renderedContent = renderTemplate(template.content, variables);

  // Create trace context
  const trace = createTraceContext(event.correlationId);

  // Normalize phone to 55XXXXXXXXXXX format
  const normalizedPhone = normalizePhone(recipient);

  // Create message log
  const [msgLog] = await db.insert(messageLogs).values({
    tenantId: event.tenantId,
    instanceId: instance.id,
    templateId: template.id,
    direction: 'outbound',
    channel: 'whatsapp',
    recipient: normalizedPhone,
    status: 'queued',
    payload: { content: renderedContent, type: rule.messageType },
    traceId: event.traceId || trace.traceId,
    correlationId: event.correlationId || trace.correlationId,
    queuedAt: new Date(),
  }).returning();

  // Enqueue send-message
  const queues = getQueues();

  const jobData: Record<string, unknown> = {
    tenantId: event.tenantId,
    instanceId: instance.id,
    sessionName: instance.sessionName,
    messageLogId: msgLog.id,
    to: normalizedPhone,
    content: renderedContent,
    type: rule.messageType,
    traceId: event.traceId || trace.traceId,
    correlationId: event.correlationId || trace.correlationId,
  };

  // Add list-specific fields if template is list type
  if (rule.messageType === 'list' && template.metadata) {
    const meta = template.metadata as Record<string, unknown>;
    if (meta.buttonText) jobData.buttonText = meta.buttonText;
    if (meta.sections) jobData.sections = meta.sections;
  }

  const delay = rule.delayMs > 0 ? rule.delayMs : undefined;

  await queues.sendMessage.add('automation', jobData, {
    delay,
    attempts: 3,
    backoff: { type: 'exponential', delay: 10_000 },
  });

  logger.info({
    ...ctx,
    templateSlug: rule.templateSlug,
    recipient: normalizedPhone,
    messageLogId: msgLog.id,
    delay: delay ?? 0,
  }, 'Automation message enqueued');

  return { ruleId: rule.id, matched: true, sent: true };
}

function normalizePhone(phone: string): string {
  // Remove non-digits
  const digits = phone.replace(/\D/g, '');

  // Add country code 55 if not present
  if (digits.length <= 11) {
    return `55${digits}`;
  }

  return digits;
}
