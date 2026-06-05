import type { ConnectionOptions } from 'bullmq';
import { Redis } from 'ioredis';

export function getRedisConnectionOptions(): ConnectionOptions {
  const url = process.env.REDIS_URL ?? 'redis://localhost:6379';
  const parsed = new URL(url);
  return {
    host: parsed.hostname,
    port: Number(parsed.port) || 6379,
    maxRetriesPerRequest: null,
  };
}

let _redisClient: Redis | null = null;

/**
 * Shared ioredis client for rate limiting, locks, and counters.
 * Reuses a single connection across the process.
 */
export function getRedisClient(): Redis {
  if (!_redisClient) {
    const url = process.env.REDIS_URL ?? 'redis://localhost:6379';
    const parsed = new URL(url);
    _redisClient = new Redis({
      host: parsed.hostname,
      port: Number(parsed.port) || 6379,
      maxRetriesPerRequest: null,
      lazyConnect: false,
    });
  }
  return _redisClient;
}
