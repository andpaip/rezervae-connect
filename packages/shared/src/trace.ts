import { randomUUID } from 'node:crypto';

export function generateTraceId(): string {
  return randomUUID();
}

export function generateCorrelationId(): string {
  return randomUUID();
}

export interface TraceContext {
  traceId: string;
  correlationId: string;
  requestId?: string;
}

export function createTraceContext(correlationId?: string): TraceContext {
  return {
    traceId: generateTraceId(),
    correlationId: correlationId ?? generateCorrelationId(),
  };
}
