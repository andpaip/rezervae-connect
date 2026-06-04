import { createLogger } from './logger.js';

const logger = createLogger('audit');

export interface AuditEntry {
  tenantId: string;
  actor: string;
  entityType: string;
  entityId?: string;
  action: string;
  oldState?: Record<string, unknown> | null;
  newState?: Record<string, unknown> | null;
  metadata?: Record<string, unknown>;
}

/**
 * Centralized AuditService.
 * Accepts a DB insert function so it remains decoupled from the database package.
 */
export class AuditService {
  private insertFn: ((entry: AuditEntry) => Promise<void>) | null = null;

  /**
   * Register the DB insert function.
   * Called once at app startup after database is initialized.
   */
  register(insertFn: (entry: AuditEntry) => Promise<void>): void {
    this.insertFn = insertFn;
  }

  /**
   * Log an audit entry. Never throws — failures are logged but swallowed.
   */
  async log(entry: AuditEntry): Promise<void> {
    if (!this.insertFn) {
      logger.warn({ entry }, 'AuditService not registered, dropping audit entry');
      return;
    }

    try {
      await this.insertFn(entry);
    } catch (err) {
      logger.error({ err, entry }, 'Failed to write audit log');
    }
  }
}

// Singleton
export const auditService = new AuditService();
