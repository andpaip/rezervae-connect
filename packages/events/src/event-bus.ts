import { EventEmitter } from 'node:events';
import type { ConnectEvent, ConnectEventType } from './event-types.js';

type EventHandler = (event: ConnectEvent) => void | Promise<void>;

export class EventBus {
  private emitter = new EventEmitter();

  on(eventType: ConnectEventType, handler: EventHandler): void {
    this.emitter.on(eventType, handler);
  }

  off(eventType: ConnectEventType, handler: EventHandler): void {
    this.emitter.off(eventType, handler);
  }

  emit(event: ConnectEvent): void {
    this.emitter.emit(event.type, event);
  }
}

// Singleton for in-process use
export const eventBus = new EventBus();
