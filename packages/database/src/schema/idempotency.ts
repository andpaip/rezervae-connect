import { pgTable, uuid, varchar, jsonb, timestamp, index, uniqueIndex } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';

export const idempotencyKeys = pgTable('idempotency_keys', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  key: varchar('key', { length: 255 }).notNull(),
  operation: varchar('operation', { length: 100 }).notNull(),
  payloadHash: varchar('payload_hash', { length: 255 }),
  requestId: varchar('request_id', { length: 100 }),
  providerEventId: varchar('provider_event_id', { length: 255 }),
  correlationId: varchar('correlation_id', { length: 100 }),
  result: jsonb('result').$type<Record<string, unknown> | null>(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  expiresAt: timestamp('expires_at', { withTimezone: true }).notNull(),
}, (table) => [
  uniqueIndex('uq_idemp_tenant_key').on(table.tenantId, table.key),
  index('idx_idemp_expires').on(table.expiresAt),
]);
