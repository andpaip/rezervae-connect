import { pgTable, uuid, varchar, text, jsonb, timestamp, index } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';
import { whatsappInstances } from './whatsapp-instances.js';
import { conversationSessions } from './conversations.js';
import { messageTemplates } from './message-templates.js';

export const messageLogs = pgTable('message_logs', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  instanceId: uuid('instance_id').references(() => whatsappInstances.id),
  sessionId: uuid('session_id').references(() => conversationSessions.id),
  channel: varchar('channel', { length: 30 }).default('whatsapp').notNull(),
  direction: varchar('direction', { length: 10 }).notNull(),
  templateId: uuid('template_id').references(() => messageTemplates.id),
  status: varchar('status', { length: 20 }).notNull(),
  providerMessageId: varchar('provider_message_id', { length: 255 }),
  recipient: varchar('recipient', { length: 100 }),
  error: text('error'),
  payload: jsonb('payload').default({}).$type<Record<string, unknown>>(),
  traceId: varchar('trace_id', { length: 100 }),
  correlationId: varchar('correlation_id', { length: 100 }),
  sourceType: varchar('source_type', { length: 20 }),
  scheduledFor: timestamp('scheduled_for', { withTimezone: true }),
  queuedAt: timestamp('queued_at', { withTimezone: true }),
  sentAt: timestamp('sent_at', { withTimezone: true }),
  deliveredAt: timestamp('delivered_at', { withTimezone: true }),
  readAt: timestamp('read_at', { withTimezone: true }),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  index('idx_msglog_tenant_status').on(table.tenantId, table.status),
  index('idx_msglog_tenant_created').on(table.tenantId, table.createdAt),
  index('idx_msglog_status_scheduled').on(table.status, table.scheduledFor),
]);
