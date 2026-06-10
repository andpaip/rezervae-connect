import { pgTable, uuid, varchar, boolean, jsonb, timestamp, uniqueIndex } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';

export const tenantFeatures = pgTable('tenant_features', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  feature: varchar('feature', { length: 100 }).notNull(),
  enabled: boolean('enabled').default(false).notNull(),
  config: jsonb('config').default({}).$type<Record<string, unknown>>(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  uniqueIndex('uq_tenant_feature').on(table.tenantId, table.feature),
]);
