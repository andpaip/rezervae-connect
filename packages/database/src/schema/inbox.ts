import { pgTable, uuid, varchar, text, integer, jsonb, timestamp, index } from 'drizzle-orm/pg-core';
import { tenants } from './tenants.js';
import { conversationSessions } from './conversations.js';

export const inboxThreads = pgTable('inbox_threads', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  conversationSessionId: uuid('conversation_session_id').references(() => conversationSessions.id, { onDelete: 'cascade' }),
  channel: varchar('channel', { length: 30 }).default('whatsapp').notNull(),
  status: varchar('status', { length: 30 }).default('open').notNull(),
  priority: varchar('priority', { length: 20 }).default('normal').notNull(),
  assignedUserId: varchar('assigned_user_id', { length: 100 }),
  teamId: varchar('team_id', { length: 100 }),
  lastMessageAt: timestamp('last_message_at', { withTimezone: true }),
  unreadCount: integer('unread_count').default(0),
  metadata: jsonb('metadata').default({}).$type<Record<string, unknown>>(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  index('idx_inbox_tenant_status').on(table.tenantId, table.status),
  index('idx_inbox_convsession').on(table.conversationSessionId),
]);

export const inboxParticipants = pgTable('inbox_participants', {
  id: uuid('id').defaultRandom().primaryKey(),
  threadId: uuid('thread_id').notNull().references(() => inboxThreads.id, { onDelete: 'cascade' }),
  userId: varchar('user_id', { length: 100 }).notNull(),
  role: varchar('role', { length: 30 }).default('agent').notNull(),
  joinedAt: timestamp('joined_at', { withTimezone: true }).defaultNow().notNull(),
  leftAt: timestamp('left_at', { withTimezone: true }),
});

export const inboxAssignments = pgTable('inbox_assignments', {
  id: uuid('id').defaultRandom().primaryKey(),
  threadId: uuid('thread_id').notNull().references(() => inboxThreads.id, { onDelete: 'cascade' }),
  fromUserId: varchar('from_user_id', { length: 100 }),
  toUserId: varchar('to_user_id', { length: 100 }).notNull(),
  reason: varchar('reason', { length: 255 }),
  assignedAt: timestamp('assigned_at', { withTimezone: true }).defaultNow().notNull(),
});

export const inboxTags = pgTable('inbox_tags', {
  id: uuid('id').defaultRandom().primaryKey(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id),
  threadId: uuid('thread_id').notNull().references(() => inboxThreads.id, { onDelete: 'cascade' }),
  tag: varchar('tag', { length: 100 }).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
});

export const inboxNotes = pgTable('inbox_notes', {
  id: uuid('id').defaultRandom().primaryKey(),
  threadId: uuid('thread_id').notNull().references(() => inboxThreads.id, { onDelete: 'cascade' }),
  userId: varchar('user_id', { length: 100 }).notNull(),
  content: text('content').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
});
