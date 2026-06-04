import { pgTable, uuid, varchar, jsonb, timestamp, index } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';

export const auditLogs = pgTable('audit_logs', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  actor: varchar('actor', { length: 100 }).notNull(),
  entityType: varchar('entity_type', { length: 50 }).notNull(),
  entityId: uuid('entity_id'),
  action: varchar('action', { length: 50 }).notNull(),
  oldState: jsonb('old_state').$type<Record<string, unknown> | null>(),
  newState: jsonb('new_state').$type<Record<string, unknown> | null>(),
  metadata: jsonb('metadata').default({}).$type<Record<string, unknown>>(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
}, (table) => [
  index('idx_audit_tenant_entity_created').on(table.tenantId, table.entityType, table.createdAt),
  index('idx_audit_tenant_action_created').on(table.tenantId, table.action, table.createdAt),
  index('idx_audit_entity').on(table.entityType, table.entityId),
]);
