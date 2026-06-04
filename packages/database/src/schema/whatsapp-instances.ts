import { pgTable, uuid, varchar, text, jsonb, timestamp, integer, numeric, uniqueIndex } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';

export const whatsappInstances = pgTable('whatsapp_instances', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  provider: varchar('provider', { length: 50 }).default('wppconnect').notNull(),
  instanceName: varchar('instance_name', { length: 100 }).notNull(),
  sessionName: varchar('session_name', { length: 100 }).notNull(),
  phone: varchar('phone', { length: 20 }),
  status: varchar('status', { length: 30 }).default('disconnected').notNull(),
  qrCode: text('qr_code'),
  lastSeenAt: timestamp('last_seen_at'),
  connectedAt: timestamp('connected_at'),
  disconnectedAt: timestamp('disconnected_at'),
  metadata: jsonb('metadata').default({}).$type<Record<string, unknown>>(),
  healthScore: integer('health_score').default(100),
  failureRate: numeric('failure_rate', { precision: 5, scale: 2 }).default('0'),
  reconnectCount: integer('reconnect_count').default(0),
  lastHeartbeatAt: timestamp('last_heartbeat_at'),
  createdAt: timestamp('created_at').defaultNow().notNull(),
}, (table) => [
  uniqueIndex('uq_tenant_session').on(table.tenantId, table.sessionName),
]);
