import { createHmac } from 'node:crypto';
import { createLogger } from './logger.js';
import { createTraceContext } from './trace.js';

const logger = createLogger('core-api-client');

/**
 * HTTP client for Connect -> Core API calls.
 *
 * Generates HMAC-SHA256 signature that the Laravel ConnectAuth middleware validates:
 *   signature = HMAC-SHA256(secret, "{timestamp}:{method}:{url}:{body}")
 *
 * Headers sent:
 *   X-Connect-Signature, X-Timestamp, X-Tenant-Id, X-Trace-Id, X-Correlation-Id
 */
export interface CoreApiConfig {
  baseUrl: string;
  secret: string;
  timeoutMs?: number;
}

export interface CoreApiRequestOptions {
  tenantId: string;
  traceId?: string;
  correlationId?: string;
}

function getConfig(): CoreApiConfig {
  return {
    baseUrl: process.env.CORE_API_URL ?? 'http://localhost:8080',
    secret: process.env.CORE_SECRET ?? 'dev-secret',
    timeoutMs: parseInt(process.env.CORE_API_TIMEOUT ?? '10000', 10),
  };
}

function buildHeaders(
  method: string,
  path: string,
  body: string,
  options: CoreApiRequestOptions,
): Record<string, string> {
  const config = getConfig();
  const timestamp = Date.now().toString();
  const trace = createTraceContext(options.correlationId);

  const signature = createHmac('sha256', config.secret)
    .update(`${timestamp}:${method}:${path}:${body}`)
    .digest('hex');

  return {
    'Content-Type': 'application/json',
    'X-Connect-Signature': signature,
    'X-Timestamp': timestamp,
    'X-Tenant-Id': options.tenantId,
    'X-Trace-Id': options.traceId ?? trace.traceId,
    'X-Correlation-Id': trace.correlationId,
  };
}

export async function coreApiPost(
  path: string,
  data: Record<string, unknown>,
  options: CoreApiRequestOptions,
): Promise<{ ok: boolean; status: number; data?: unknown }> {
  const config = getConfig();
  const body = JSON.stringify(data);
  const headers = buildHeaders('POST', path, body, options);
  const url = `${config.baseUrl}${path}`;

  const startMs = Date.now();

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers,
      body,
      signal: AbortSignal.timeout(config.timeoutMs ?? 10_000),
    });

    const durationMs = Date.now() - startMs;
    const responseData = response.headers.get('content-type')?.includes('json')
      ? await response.json()
      : null;

    logger.info({
      method: 'POST', path,
      status: response.status, durationMs,
      tenantId: options.tenantId,
      traceId: headers['X-Trace-Id'],
    }, 'Core API response');

    return { ok: response.ok, status: response.status, data: responseData };
  } catch (err) {
    const durationMs = Date.now() - startMs;
    logger.error({
      method: 'POST', path, durationMs,
      tenantId: options.tenantId,
      err,
    }, 'Core API request failed');

    return { ok: false, status: 0, data: undefined };
  }
}

export async function coreApiGet(
  path: string,
  options: CoreApiRequestOptions,
): Promise<{ ok: boolean; status: number; data?: unknown }> {
  const config = getConfig();
  const headers = buildHeaders('GET', path, '', options);
  const url = `${config.baseUrl}${path}`;

  const startMs = Date.now();

  try {
    const response = await fetch(url, {
      method: 'GET',
      headers,
      signal: AbortSignal.timeout(config.timeoutMs ?? 10_000),
    });

    const durationMs = Date.now() - startMs;
    const responseData = response.headers.get('content-type')?.includes('json')
      ? await response.json()
      : null;

    logger.info({
      method: 'GET', path,
      status: response.status, durationMs,
      tenantId: options.tenantId,
    }, 'Core API response');

    return { ok: response.ok, status: response.status, data: responseData };
  } catch (err) {
    const durationMs = Date.now() - startMs;
    logger.error({
      method: 'GET', path, durationMs,
      tenantId: options.tenantId,
      err,
    }, 'Core API request failed');

    return { ok: false, status: 0, data: undefined };
  }
}
