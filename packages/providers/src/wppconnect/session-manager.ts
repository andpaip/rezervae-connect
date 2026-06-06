import { eq, and } from 'drizzle-orm';
import { createHash } from 'node:crypto';
import { db } from '@rezervae-connect/database';
import { whatsappInstances, auditLogs } from '@rezervae-connect/database';
import { eventBus } from '@rezervae-connect/events';
import {
  createLogger,
  createTraceContext,
  type InstanceStatus,
  type ConnectionConfig,
} from '@rezervae-connect/shared';
import { WPPConnectProvider } from './wppconnect-provider.js';

const logger = createLogger('session-manager');

const HEARTBEAT_INTERVAL_MS = 60_000;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_BASE_DELAY_MS = 5_000;

interface ManagedSession {
  tenantId: string;
  instanceId: string;
  sessionName: string;
  reconnectAttempts: number;
  lastQrHash?: string;
}

export class SessionManager {
  private provider: WPPConnectProvider;
  private managedSessions = new Map<string, ManagedSession>();
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;

  constructor(provider: WPPConnectProvider) {
    this.provider = provider;
    this.registerProviderCallbacks();
  }

  // --- Public API ---

  hasSession(sessionName: string): boolean {
    return this.managedSessions.has(sessionName);
  }

  async createSession(tenantId: string, instanceId: string, sessionName: string): Promise<void> {
    const trace = createTraceContext();

    logger.info({ tenantId, instanceId, sessionName, ...trace }, 'Creating session');

    this.managedSessions.set(sessionName, {
      tenantId,
      instanceId,
      sessionName,
      reconnectAttempts: 0,
    });

    await this.updateInstanceStatus(instanceId, 'connecting', trace);
    await this.audit(tenantId, instanceId, 'connecting', null, { status: 'connecting' }, trace);

    const config: ConnectionConfig = { sessionName, tenantId, instanceId };

    try {
      await this.provider.connect(config);
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      logger.error({ sessionName, error, ...trace }, 'Failed to create session');
      await this.updateInstanceStatus(instanceId, 'error', trace);
      await this.audit(tenantId, instanceId, 'error', { status: 'connecting' }, { status: 'error', error }, trace);
      throw err;
    }
  }

  async disconnectSession(sessionName: string): Promise<void> {
    const session = this.managedSessions.get(sessionName);
    if (!session) {
      logger.warn({ sessionName }, 'No managed session found to disconnect');
      return;
    }

    const trace = createTraceContext();
    const { tenantId, instanceId } = session;

    logger.info({ sessionName, ...trace }, 'Disconnecting session');

    await this.provider.disconnect(sessionName);
    await this.updateInstanceStatus(instanceId, 'disconnected', trace, {
      disconnectedAt: new Date(),
    });
    await this.audit(tenantId, instanceId, 'disconnected', { status: 'connected' }, { status: 'disconnected' }, trace);

    this.managedSessions.delete(sessionName);
  }

  async reconnectSession(sessionName: string): Promise<void> {
    const session = this.managedSessions.get(sessionName);
    if (!session) {
      logger.warn({ sessionName }, 'No managed session found to reconnect');
      return;
    }

    if (session.reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
      const trace = createTraceContext();
      logger.error({ sessionName, attempts: session.reconnectAttempts, ...trace }, 'Max reconnect attempts reached');
      await this.updateInstanceStatus(session.instanceId, 'error', trace);
      await this.audit(
        session.tenantId, session.instanceId, 'reconnect_failed',
        null, { attempts: session.reconnectAttempts }, trace,
      );
      return;
    }

    session.reconnectAttempts++;
    const delay = RECONNECT_BASE_DELAY_MS * Math.pow(2, session.reconnectAttempts - 1);
    const trace = createTraceContext();

    logger.info({ sessionName, attempt: session.reconnectAttempts, delayMs: delay, ...trace }, 'Scheduling reconnect');

    await this.updateInstanceStatus(session.instanceId, 'connecting', trace, {
      reconnectCount: session.reconnectAttempts,
    });

    await this.audit(
      session.tenantId, session.instanceId, 'reconnecting',
      null, { attempt: session.reconnectAttempts, delayMs: delay }, trace,
    );

    setTimeout(async () => {
      try {
        await this.provider.disconnect(sessionName);
        await this.provider.connect({
          sessionName,
          tenantId: session.tenantId,
          instanceId: session.instanceId,
        });
        session.reconnectAttempts = 0;
      } catch (err) {
        logger.error({ sessionName, err }, 'Reconnect attempt failed');
        await this.reconnectSession(sessionName);
      }
    }, delay);
  }

  async restoreAllSessions(tenantId: string): Promise<void> {
    const trace = createTraceContext();
    logger.info({ tenantId, ...trace }, 'Restoring all sessions for tenant');

    const instances = await db
      .select()
      .from(whatsappInstances)
      .where(
        and(
          eq(whatsappInstances.tenantId, tenantId),
          eq(whatsappInstances.status, 'connected'),
        ),
      );

    logger.info({ tenantId, count: instances.length, ...trace }, 'Found instances to restore');

    for (const instance of instances) {
      try {
        await this.createSession(tenantId, instance.id, instance.sessionName);
      } catch (err) {
        logger.error({ sessionName: instance.sessionName, err }, 'Failed to restore session');
      }
    }
  }

  startHeartbeat(): void {
    if (this.heartbeatTimer) return;

    this.heartbeatTimer = setInterval(async () => {
      const now = new Date();
      for (const [sessionName, session] of this.managedSessions) {
        const status = this.provider.getStatus(sessionName);
        if (status === 'connected') {
          await db
            .update(whatsappInstances)
            .set({ lastHeartbeatAt: now, lastSeenAt: now })
            .where(eq(whatsappInstances.id, session.instanceId));
        }
      }
    }, HEARTBEAT_INTERVAL_MS);

    logger.info('Heartbeat started');
  }

  stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
      logger.info('Heartbeat stopped');
    }
  }

  async shutdown(): Promise<void> {
    this.stopHeartbeat();
    const sessions = [...this.managedSessions.keys()];
    for (const sessionName of sessions) {
      await this.disconnectSession(sessionName);
    }
    logger.info('All sessions shut down');
  }

  // --- Provider Callbacks ---

  private registerProviderCallbacks(): void {
    this.provider.onQRCode(async (sessionName, qr) => {
      const session = this.managedSessions.get(sessionName);
      if (!session) return;

      // Dedup: skip if QR hasn't changed
      const qrHash = createHash('md5').update(qr).digest('hex');
      if (session.lastQrHash === qrHash) return;
      session.lastQrHash = qrHash;

      const trace = createTraceContext();

      await this.updateInstanceStatus(session.instanceId, 'qr_ready', trace, {
        qrCode: qr,
      });

      await this.audit(session.tenantId, session.instanceId, 'qr_generated', null, { sessionName }, trace);

      eventBus.emit({
        type: 'qr.generated',
        tenantId: session.tenantId,
        traceId: trace.traceId,
        correlationId: trace.correlationId,
        timestamp: new Date().toISOString(),
        version: '1.0',
        data: { instanceId: session.instanceId, sessionName, qrCode: qr },
      });
    });

    this.provider.onStatusChange(async (sessionName, status) => {
      const session = this.managedSessions.get(sessionName);
      if (!session) return;
      const trace = createTraceContext();

      logger.info({ sessionName, status, ...trace }, 'Provider status change');

      const updates: Record<string, unknown> = { status };
      if (status === 'connected') {
        updates.connectedAt = new Date();
        session.reconnectAttempts = 0;
      }
      if (status === 'disconnected') {
        updates.disconnectedAt = new Date();
      }

      await this.updateInstanceStatus(session.instanceId, status, trace, updates);
      await this.audit(session.tenantId, session.instanceId, status, null, { status }, trace);

      if (status === 'connected') {
        eventBus.emit({
          type: 'instance.connected',
          tenantId: session.tenantId,
          traceId: trace.traceId,
          correlationId: trace.correlationId,
          timestamp: new Date().toISOString(),
          version: '1.0',
          data: { instanceId: session.instanceId, sessionName },
        });
      }

      if (status === 'disconnected') {
        eventBus.emit({
          type: 'instance.disconnected',
          tenantId: session.tenantId,
          traceId: trace.traceId,
          correlationId: trace.correlationId,
          timestamp: new Date().toISOString(),
          version: '1.0',
          data: { instanceId: session.instanceId, sessionName },
        });

        // Auto-reconnect
        await this.reconnectSession(sessionName);
      }
    });

    this.provider.onMessage(async (sessionName, message) => {
      const session = this.managedSessions.get(sessionName);
      if (!session) return;
      const trace = createTraceContext();

      logger.info({ sessionName, from: message.from, type: message.type, ...trace }, 'Incoming message');

      eventBus.emit({
        type: 'message.received',
        tenantId: session.tenantId,
        traceId: trace.traceId,
        correlationId: trace.correlationId,
        timestamp: new Date().toISOString(),
        version: '1.0',
        data: {
          sessionName,
          from: message.from,
          body: message.body,
          messageType: message.type,
        },
      });
    });
  }

  // --- DB helpers ---

  private async updateInstanceStatus(
    instanceId: string,
    status: InstanceStatus,
    trace: { traceId: string; correlationId: string },
    extra?: Record<string, unknown>,
  ): Promise<void> {
    try {
      await db
        .update(whatsappInstances)
        .set({ status, ...extra })
        .where(eq(whatsappInstances.id, instanceId));
    } catch (err) {
      logger.error({ instanceId, status, err, ...trace }, 'Failed to update instance status');
    }
  }

  private async audit(
    tenantId: string,
    instanceId: string,
    action: string,
    oldState: Record<string, unknown> | null,
    newState: Record<string, unknown> | null,
    trace: { traceId: string; correlationId: string },
  ): Promise<void> {
    try {
      await db.insert(auditLogs).values({
        tenantId,
        actor: 'system',
        entityType: 'instance',
        entityId: instanceId,
        action,
        oldState,
        newState,
        metadata: { traceId: trace.traceId, correlationId: trace.correlationId },
      });
    } catch (err) {
      logger.error({ tenantId, instanceId, action, err }, 'Failed to write audit log');
    }
  }
}
