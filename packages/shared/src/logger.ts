import pino from 'pino';

export function createLogger(name: string) {
  return pino({
    name,
    level: process.env.LOG_LEVEL ?? 'debug',
    transport: process.env.NODE_ENV !== 'production'
      ? { target: 'pino-pretty', options: { colorize: true } }
      : undefined,
    redact: ['api_key', 'password', 'qr_code', 'token'],
  });
}

export type Logger = pino.Logger;
