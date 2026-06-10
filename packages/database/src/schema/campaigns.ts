import { pgTable, uuid, varchar, text, jsonb, integer, timestamp, index } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';
import { messageTemplates } from './message-templates.js';
import { whatsappInstances } from './whatsapp-instances.js';

export const campaigns = pgTable('campaigns', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  name: varchar('name', { length: 255 }).notNull(),
  templateId: uuid('template_id').references(() => messageTemplates.id),
  status: varchar('status', { length: 30 }).default('draft').notNull(),
  config: jsonb('config').notNull().$type<{
    dailyLimit?: number;
    intervalMinMs?: number;
    intervalMaxMs?: number;
    segmentation?: Record<string, unknown>;
  }>(),
  stats: jsonb('stats').default({}).$type<Record<string, number>>(),
  instanceId: uuid('instance_id').references(() => whatsappInstances.id),
  startedAt: timestamp('started_at', { withTimezone: true }),
  pausedAt: timestamp('paused_at', { withTimezone: true }),
  finishedAt: timestamp('finished_at', { withTimezone: true }),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const campaignRecipients = pgTable('campaign_recipients', {
  id: uuid('id').defaultRandom().primaryKey(),
  campaignId: uuid('campaign_id').notNull().references(() => campaigns.id, { onDelete: 'cascade' }),
  tenantId: uuid('tenant_id').notNull(),
  customerPhone: varchar('customer_phone', { length: 20 }).notNull(),
  customerName: varchar('customer_name', { length: 255 }),
  customerExternalId: varchar('customer_external_id', { length: 100 }),
  status: varchar('status', { length: 20 }).default('pending').notNull(),
  error: text('error'),
  attempts: integer('attempts').default(0),
  sentAt: timestamp('sent_at', { withTimezone: true }),
  metadata: jsonb('metadata').default({}).$type<Record<string, unknown>>(),
  sortOrder: integer('sort_order').notNull(),
}, (table) => [
  index('idx_campaignrec_campaign_status_order').on(table.campaignId, table.status, table.sortOrder),
]);
