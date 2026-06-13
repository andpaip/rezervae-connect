import { EventEmitter } from 'node:events';
import { Redis } from 'ioredis';
import type { ConnectEvent, ConnectEventType } from './event-types.js';

const CHANNEL = 'connect:events';

type EventHandler = (event: ConnectEvent) => void | Promise<void>;

export class EventBus {
  private emitter = new EventEmitter();
  private pub: Redis | null = null;
  private sub: Redis | null = null;

  /** Call once per process to enable cross-process events via Redis pub/sub */
  async connectRedis(redisUrl?: string): Promise<void> {
    const url = redisUrl ?? process.env.REDIS_URL ?? 'redis://localhost:6379';
    this.pub = new Redis(url, { maxRetriesPerRequest: 3, lazyConnect: true });
    this.sub = new Redis(url, { maxRetriesPerRequest: 3, lazyConnect: true });

    await this.pub.connect();
    await this.sub.connect();
    await this.sub.subscribe(CHANNEL);

    this.sub.on('message', (_ch: string, raw: string) => {
      try {
        const event = JSON.parse(raw) as ConnectEvent;
        this.emitter.emit(event.type, event);
      } catch { /* ignore malformed */ }
    });
  }

  on(eventType: ConnectEventType, handler: EventHandler): void {
    this.emitter.on(eventType, handler);
  }

  off(eventType: ConnectEventType, handler: EventHandler): void {
    this.emitter.off(eventType, handler);
  }

  emit(event: ConnectEvent): void {
    if (this.pub) {
      this.pub.publish(CHANNEL, JSON.stringify(event));
    } else {
      // Fallback: local-only (no Redis connected)
      this.emitter.emit(event.type, event);
    }
  }

  async disconnect(): Promise<void> {
    await this.sub?.unsubscribe(CHANNEL);
    this.sub?.disconnect();
    this.pub?.disconnect();
    this.pub = null;
    this.sub = null;
  }
}

// Singleton for cross-process use (call connectRedis() at startup)
export const eventBus = new EventBus();
