import { pgTable, uuid, varchar, bigint, jsonb, timestamp } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';

export const mediaAssets = pgTable('media_assets', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  type: varchar('type', { length: 30 }).notNull(),
  mimeType: varchar('mime_type', { length: 100 }).notNull(),
  storageProvider: varchar('storage_provider', { length: 30 }).default('local').notNull(),
  storageKey: varchar('storage_key', { length: 500 }).notNull(),
  size: bigint('size', { mode: 'number' }),
  metadata: jsonb('metadata').default({}).$type<Record<string, unknown>>(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

export const mediaJobs = pgTable('media_jobs', {
  id: uuid('id').defaultRandom().primaryKey(),
  assetId: uuid('asset_id').notNull().references(() => mediaAssets.id),
  operation: varchar('operation', { length: 50 }).notNull(),
  status: varchar('status', { length: 30 }).default('pending').notNull(),
  result: jsonb('result').$type<Record<string, unknown> | null>(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});
