import { pgTable, uuid, varchar, text, jsonb, timestamp, index, uniqueIndex } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';
import { whatsappInstances } from './whatsapp-instances.js';

export const conversationSessions = pgTable('conversation_sessions', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  channel: varchar('channel', { length: 30 }).default('whatsapp').notNull(),
  customerPhone: varchar('customer_phone', { length: 20 }).notNull(),
  customerName: varchar('customer_name', { length: 255 }),
  customerExternalId: varchar('customer_external_id', { length: 100 }),
  instanceId: uuid('instance_id').references(() => whatsappInstances.id),
  assignedUserId: varchar('assigned_user_id', { length: 100 }),
  state: varchar('state', { length: 50 }).default('open').notNull(),
  status: varchar('status', { length: 50 }).default('bot').notNull(),
  lastMessageAt: timestamp('last_message_at', { withTimezone: true }),
  metadata: jsonb('metadata').default({}).$type<Record<string, unknown>>(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  index('idx_convsess_tenant_phone').on(table.tenantId, table.customerPhone),
  index('idx_convsess_tenant_state').on(table.tenantId, table.state),
  index('idx_convsess_instance').on(table.instanceId),
  index('idx_convsess_tenant_phone_state').on(table.tenantId, table.customerPhone, table.state),
]);

export const conversationMessages = pgTable('conversation_messages', {
  id: uuid('id').defaultRandom().primaryKey(),
  sessionId: uuid('session_id').notNull().references(() => conversationSessions.id, { onDelete: 'cascade' }),
  tenantId: uuid('tenant_id').notNull(),
  direction: varchar('direction', { length: 10 }).notNull(),
  sender: varchar('sender', { length: 100 }),
  type: varchar('type', { length: 30 }).default('text').notNull(),
  content: text('content'),
  metadata: jsonb('metadata').default({}).$type<Record<string, unknown>>(),
  providerMessageId: varchar('provider_message_id', { length: 255 }),
  status: varchar('status', { length: 20 }).default('queued').notNull(),
  error: text('error'),
  sentAt: timestamp('sent_at', { withTimezone: true }),
  deliveredAt: timestamp('delivered_at', { withTimezone: true }),
  readAt: timestamp('read_at', { withTimezone: true }),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  index('idx_convmsg_session_created').on(table.sessionId, table.createdAt),
  index('idx_convmsg_tenant_created').on(table.tenantId, table.createdAt),
  index('idx_convmsg_session_dir_created').on(table.sessionId, table.direction, table.createdAt),
]);

export const conversationContext = pgTable('conversation_context', {
  id: uuid('id').defaultRandom().primaryKey(),
  sessionId: uuid('session_id').notNull().references(() => conversationSessions.id, { onDelete: 'cascade' }).unique(),
  currentFlow: varchar('current_flow', { length: 100 }),
  currentStep: varchar('current_step', { length: 100 }),
  collectedData: jsonb('collected_data').default({}).$type<Record<string, unknown>>(),
  aiContext: jsonb('ai_context').default({}).$type<Record<string, unknown>>(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});
