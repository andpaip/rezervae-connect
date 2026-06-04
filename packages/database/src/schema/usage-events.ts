import { pgTable, uuid, varchar, integer, numeric, jsonb, timestamp, index } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';

export const usageEvents = pgTable('usage_events', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  feature: varchar('feature', { length: 100 }).notNull(),
  quantity: integer('quantity').notNull(),
  cost: numeric('cost', { precision: 10, scale: 4 }),
  referenceId: varchar('reference_id', { length: 255 }),
  metadata: jsonb('metadata').default({}).$type<Record<string, unknown>>(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
}, (table) => [
  index('idx_usage_tenant_feature_created').on(table.tenantId, table.feature, table.createdAt),
]);
