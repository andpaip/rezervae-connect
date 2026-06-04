import type { ChannelProvider } from '@rezervae-connect/shared';
import type { SessionManager } from '@rezervae-connect/providers';

let _provider: ChannelProvider | null = null;
let _sessionManager: SessionManager | null = null;

export function setProvider(provider: ChannelProvider): void {
  _provider = provider;
}

export function getProvider(): ChannelProvider {
  if (!_provider) throw new Error('Provider not registered. Call setProvider() at startup.');
  return _provider;
}

export function setSessionManager(sm: SessionManager): void {
  _sessionManager = sm;
}

export function getSessionManager(): SessionManager {
  if (!_sessionManager) throw new Error('SessionManager not registered. Call setSessionManager() at startup.');
  return _sessionManager;
}
