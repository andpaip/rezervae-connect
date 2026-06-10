import { pgTable, uuid, varchar, text, jsonb, boolean, timestamp, uniqueIndex } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';

export const messageTemplates = pgTable('message_templates', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  slug: varchar('slug', { length: 100 }).notNull(),
  name: varchar('name', { length: 255 }).notNull(),
  channel: varchar('channel', { length: 30 }).default('whatsapp').notNull(),
  type: varchar('type', { length: 30 }).default('text').notNull(),
  content: text('content').notNull(),
  variables: jsonb('variables').default([]).$type<Array<{ name: string; required: boolean }>>(),
  metadata: jsonb('metadata').default({}).$type<Record<string, unknown>>(),
  isActive: boolean('is_active').default(true).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  uniqueIndex('uq_template_tenant_slug_channel').on(table.tenantId, table.slug, table.channel),
]);
