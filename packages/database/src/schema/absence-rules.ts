import { pgTable, uuid, varchar, integer, boolean, timestamp, uniqueIndex } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';
import { whatsappInstances } from './whatsapp-instances.js';

export const absenceRules = pgTable('absence_rules', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  days: integer('days').notNull(),
  instanceId: uuid('instance_id').references(() => whatsappInstances.id),
  templateSlug: varchar('template_slug', { length: 100 }),
  messageType: varchar('message_type', { length: 20 }).default('text').notNull(),
  enabled: boolean('enabled').default(true).notNull(),
  sortOrder: integer('sort_order').default(0).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  uniqueIndex('uq_tenant_days').on(table.tenantId, table.days),
]);
