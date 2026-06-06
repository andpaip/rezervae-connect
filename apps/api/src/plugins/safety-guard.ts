import type { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { createHash } from 'node:crypto';
import { eq } from 'drizzle-orm';
import { db, whatsappInstances, auditLogs } from '@rezervae-connect/database';
import { createLogger } from '@rezervae-connect/shared';

const logger = createLogger('safety-guard');

// --- Circuit Breaker ---
const CIRCUIT_ERROR_THRESHOLD = 10;   // errors to trip
const CIRCUIT_WINDOW_MS = 5 * 60_000; // 5 min window
const CIRCUIT_COOLDOWN_MS = 2 * 60_000; // 2 min auto-reset

// --- Instance Auto-Disable ---
const AUTO_DISABLE_ERRORS = 50;
const AUTO_DISABLE_WINDOW_MS = 60 * 60_000; // 1 hour
const AUTO_DISABLE_CB_OPENS = 5;
const AUTO_DISABLE_CB_WINDOW_MS = 24 * 60 * 60_000; // 24 hours

// --- Webhook Dedup ---
const WEBHOOK_DEDUP_WINDOW_MS = 10_000; // 10 seconds

interface CircuitState {
  errors: number[];       // timestamps of errors
  isOpen: boolean;
  openedAt: number | null;
  cbOpens: number[];      // timestamps of circuit breaker opens
  totalErrors: number[];  // timestamps for auto-disable (1h window)
}

interface WebhookEntry {
  hash: string;
  timestamp: number;
}

const circuits = new Map<string, CircuitState>();
const webhookDedup = new Map<string, WebhookEntry[]>();

function getCircuit(instanceId: string): CircuitState {
  let circuit = circuits.get(instanceId);
  if (!circuit) {
    circuit = { errors: [], isOpen: false, openedAt: null, cbOpens: [], totalErrors: [] };
    circuits.set(instanceId, circuit);
  }
  return circuit;
}

function slideWindow(timestamps: number[], windowMs: number): number[] {
  const cutoff = Date.now() - windowMs;
  return timestamps.filter((t) => t > cutoff);
}

// --- Public API for workers to report errors ---
export function reportInstanceError(instanceId: string): void {
  const circuit = getCircuit(instanceId);
  const now = Date.now();

  circuit.errors.push(now);
  circuit.totalErrors.push(now);

  // Slide windows
  circuit.errors = slideWindow(circuit.errors, CIRCUIT_WINDOW_MS);
  circuit.totalErrors = slideWindow(circuit.totalErrors, AUTO_DISABLE_WINDOW_MS);

  // Check circuit breaker
  if (!circuit.isOpen && circuit.errors.length >= CIRCUIT_ERROR_THRESHOLD) {
    circuit.isOpen = true;
    circuit.openedAt = now;
    circuit.cbOpens.push(now);
    circuit.cbOpens = slideWindow(circuit.cbOpens, AUTO_DISABLE_CB_WINDOW_MS);

    logger.warn({ instanceId, errors: circuit.errors.length }, 'Circuit breaker OPENED');

    // Audit log (fire and forget)
    auditCircuitBreaker(instanceId, 'circuit_breaker_opened', circuit.errors.length).catch(() => {});

    // Check auto-disable
    if (circuit.totalErrors.length >= AUTO_DISABLE_ERRORS || circuit.cbOpens.length >= AUTO_DISABLE_CB_OPENS) {
      autoDisableInstance(instanceId, circuit).catch(() => {});
    }
  }
}

export function isCircuitOpen(instanceId: string): boolean {
  const circuit = circuits.get(instanceId);
  if (!circuit || !circuit.isOpen) return false;

  // Auto-reset after cooldown
  if (circuit.openedAt && Date.now() - circuit.openedAt > CIRCUIT_COOLDOWN_MS) {
    circuit.isOpen = false;
    circuit.openedAt = null;
    circuit.errors = [];
    logger.info({ instanceId }, 'Circuit breaker auto-reset');
    return false;
  }

  return true;
}

// --- Webhook Dedup ---
export function isDuplicateWebhook(event: string, entityId: string, timestamp: string): boolean {
  const raw = `${event}:${entityId}:${Math.floor(new Date(timestamp).getTime() / WEBHOOK_DEDUP_WINDOW_MS)}`;
  const hash = createHash('md5').update(raw).digest('hex');
  const key = `${event}:${entityId}`;
  const now = Date.now();

  let entries = webhookDedup.get(key);
  if (!entries) {
    entries = [];
    webhookDedup.set(key, entries);
  }

  // Cleanup old
  entries = entries.filter((e) => now - e.timestamp < WEBHOOK_DEDUP_WINDOW_MS * 3);
  webhookDedup.set(key, entries);

  // Check duplicate
  if (entries.some((e) => e.hash === hash)) {
    logger.debug({ event, entityId }, 'Duplicate webhook suppressed');
    return true;
  }

  entries.push({ hash, timestamp: now });
  return false;
}

// --- Internal helpers ---

async function auditCircuitBreaker(instanceId: string, action: string, errorCount: number): Promise<void> {
  try {
    const [instance] = await db.select().from(whatsappInstances).where(eq(whatsappInstances.id, instanceId));
    if (!instance) return;

    await db.insert(auditLogs).values({
      tenantId: instance.tenantId,
      actor: 'system',
      entityType: 'instance',
      entityId: instanceId,
      action,
      newState: { errorCount, timestamp: new Date().toISOString() },
      metadata: {},
    });
  } catch (err) {
    logger.error({ instanceId, action, err }, 'Failed to audit circuit breaker');
  }
}

async function autoDisableInstance(instanceId: string, circuit: CircuitState): Promise<void> {
  try {
    const reason = circuit.totalErrors.length >= AUTO_DISABLE_ERRORS
      ? `${circuit.totalErrors.length} errors in 1h`
      : `${circuit.cbOpens.length} circuit breaker opens in 24h`;

    logger.error({ instanceId, reason }, 'AUTO-DISABLING instance');

    await db.update(whatsappInstances)
      .set({ status: 'error' })
      .where(eq(whatsappInstances.id, instanceId));

    const [instance] = await db.select().from(whatsappInstances).where(eq(whatsappInstances.id, instanceId));
    if (instance) {
      await db.insert(auditLogs).values({
        tenantId: instance.tenantId,
        actor: 'system',
        entityType: 'instance',
        entityId: instanceId,
        action: 'instance_auto_disabled',
        newState: { reason, status: 'error' },
        metadata: {},
      });
    }
  } catch (err) {
    logger.error({ instanceId, err }, 'Failed to auto-disable instance');
  }
}

// --- Fastify Plugin (request-level circuit breaker check) ---

const safetyGuardPlugin: FastifyPluginAsync = async (fastify) => {
  fastify.addHook('preHandler', async (request: FastifyRequest, reply: FastifyReply) => {
    // Only check instance-specific routes
    const match = request.url.match(/\/api\/v1\/instances\/([^/]+)/);
    if (!match) return;

    const instanceId = match[1];

    if (isCircuitOpen(instanceId)) {
      logger.warn({ instanceId, url: request.url }, 'Request blocked by circuit breaker');
      return reply.code(503).send({
        error: 'Instance temporarily unavailable',
        reason: 'circuit_breaker_open',
        retryAfter: Math.ceil(CIRCUIT_COOLDOWN_MS / 1000),
      });
    }
  });

  // Cleanup old entries every 5 minutes
  setInterval(() => {
    const now = Date.now();
    for (const [key, circuit] of circuits) {
      circuit.errors = slideWindow(circuit.errors, CIRCUIT_WINDOW_MS);
      circuit.totalErrors = slideWindow(circuit.totalErrors, AUTO_DISABLE_WINDOW_MS);
      circuit.cbOpens = slideWindow(circuit.cbOpens, AUTO_DISABLE_CB_WINDOW_MS);
      if (circuit.errors.length === 0 && circuit.totalErrors.length === 0 && !circuit.isOpen) {
        circuits.delete(key);
      }
    }

    for (const [key, entries] of webhookDedup) {
      const filtered = entries.filter((e) => now - e.timestamp < WEBHOOK_DEDUP_WINDOW_MS * 3);
      if (filtered.length === 0) webhookDedup.delete(key);
      else webhookDedup.set(key, filtered);
    }
  }, 5 * 60_000);
};

export default fp(safetyGuardPlugin, {
  name: 'safety-guard',
  dependencies: ['internal-auth'],
});
