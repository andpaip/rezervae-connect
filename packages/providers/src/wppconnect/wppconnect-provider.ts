import wppconnect from '@wppconnect-team/wppconnect';
import { createLogger } from '@rezervae-connect/shared';
import type {
  ChannelProvider,
  ConnectionConfig,
  SendParams,
  SendImageParams,
  SendListParams,
  SendResult,
  InstanceStatus,
  RawIncomingMessage,
} from '@rezervae-connect/shared';

type WPPClient = Awaited<ReturnType<typeof wppconnect.create>>;

type QRCallback = (session: string, qr: string) => void;
type StatusCallback = (session: string, status: InstanceStatus) => void;
type MessageCallback = (session: string, message: RawIncomingMessage) => void;

const logger = createLogger('wppconnect-provider');

/**
 * WPPConnect adapter implementing ChannelProvider.
 * Zero business logic — only connect/disconnect/send/receive/QR/status.
 */
export class WPPConnectProvider implements ChannelProvider {
  private sessions = new Map<string, WPPClient>();
  private statuses = new Map<string, InstanceStatus>();
  private phones = new Map<string, string>();

  private qrCallbacks: QRCallback[] = [];
  private statusCallbacks: StatusCallback[] = [];
  private messageCallbacks: MessageCallback[] = [];

  async connect(config: ConnectionConfig): Promise<void> {
    const { sessionName, headless = true } = config;

    if (this.sessions.has(sessionName)) {
      logger.warn({ sessionName }, 'Cleaning up existing session before reconnect');
      await this.disconnect(sessionName);
    }

    this.setStatus(sessionName, 'connecting');

    const client = await wppconnect.create({
      session: sessionName,
      headless,
      useChrome: true,
      disableWelcome: true,
      logQR: false,
      folderNameToken: 'tokens',
      tokenStore: 'tokens',
      autoClose: 0,

      puppeteerOptions: {
        headless,
        userDataDir: `./tokens/${sessionName}-profile`,
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-accelerated-2d-canvas',
          '--disable-gpu',
        ],
      },

      catchQR: (base64Qr: string) => {
        this.setStatus(sessionName, 'qr_ready');
        for (const cb of this.qrCallbacks) {
          cb(sessionName, base64Qr);
        }
      },

      statusFind: (statusSession) => {
        if (statusSession === 'isLogged') {
          this.setStatus(sessionName, 'connected');
        }
      },
    });

    this.sessions.set(sessionName, client);

    // Extract phone number from connected client
    try {
      const hostDevice = await client.getHostDevice();
      const wid = (hostDevice as Record<string, unknown>)?.id?.toString()
        ?? (hostDevice as Record<string, unknown>)?.wid?.toString()
        ?? '';
      const phone = wid.replace('@c.us', '').replace(/\D/g, '');
      if (phone) {
        this.phones.set(sessionName, phone);
        logger.info({ sessionName, phone }, 'Phone number captured');
      }
    } catch (err) {
      logger.warn({ sessionName, err }, 'Could not extract phone number');
    }

    this.setStatus(sessionName, 'connected');

    // Register incoming message handler
    client.onMessage((msg: unknown) => {
      const raw = this.mapToRawMessage(sessionName, msg);
      if (!raw) return;
      for (const cb of this.messageCallbacks) {
        cb(sessionName, raw);
      }
    });

    // Handle disconnection
    client.onStateChange((state) => {
      logger.info({ sessionName, state }, 'WPP state change');
      if (state === 'CONFLICT' || state === 'UNPAIRED' || state === 'UNLAUNCHED') {
        this.sessions.delete(sessionName);
        this.setStatus(sessionName, 'disconnected');
      }
    });

    logger.info({ sessionName }, 'WPPConnect session connected');
  }

  async disconnect(sessionName: string): Promise<void> {
    const client = this.sessions.get(sessionName);
    if (!client) {
      logger.warn({ sessionName }, 'No session to disconnect');
      return;
    }

    try {
      await client.close();
    } catch (err) {
      logger.error({ sessionName, err }, 'Error closing WPP session');
    }

    this.sessions.delete(sessionName);
    this.setStatus(sessionName, 'disconnected');
    logger.info({ sessionName }, 'WPPConnect session disconnected');
  }

  async sendMessage(params: SendParams): Promise<SendResult> {
    const client = this.getClientOrThrow(params.sessionName);
    try {
      const result = await client.sendText(`${params.to}@c.us`, params.content);
      return { success: true, providerMessageId: (result as { id?: string }).id };
    } catch (err) {
      return this.handleSendError(params.sessionName, params.to, err, 'sendMessage');
    }
  }

  async sendImage(params: SendImageParams): Promise<SendResult> {
    const client = this.getClientOrThrow(params.sessionName);
    try {
      const result = await client.sendImage(
        `${params.to}@c.us`,
        params.imageUrl,
        'image',
        params.caption,
      );
      return { success: true, providerMessageId: (result as { id?: string }).id };
    } catch (err) {
      return this.handleSendError(params.sessionName, params.to, err, 'sendImage');
    }
  }

  async sendListMessage(params: SendListParams): Promise<SendResult> {
    const client = this.getClientOrThrow(params.sessionName);
    try {
      const result = await client.sendListMessage(`${params.to}@c.us`, {
        buttonText: params.buttonText,
        description: params.content,
        sections: params.sections,
      });
      return { success: true, providerMessageId: (result as { id?: string }).id };
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      // WPPConnect v2 sends the list successfully but crashes on post-send @lid tracking.
      // The message IS delivered — do NOT fallback to sendMessage (would cause duplicate).
      if (error.includes('not found') && error.includes('@lid')) {
        logger.warn({ sessionName: params.sessionName, to: params.to, error }, 'sendListMessage: message delivered but post-send tracking failed (WPP @lid bug), treating as success');
        return { success: true, providerMessageId: undefined };
      }
      // For non-@lid errors, fallback to plain text
      logger.warn({ sessionName: params.sessionName, to: params.to, error }, 'sendListMessage failed, falling back to sendMessage');
      return this.sendMessage({ sessionName: params.sessionName, to: params.to, content: params.content });
    }
  }

  async logout(sessionName: string): Promise<void> {
    const client = this.sessions.get(sessionName);
    if (client) {
      try {
        await client.logout();
      } catch (err) {
        logger.warn({ sessionName, err }, 'Error during WPP logout (may already be logged out)');
      }
      try {
        await client.close();
      } catch (err) {
        logger.warn({ sessionName, err }, 'Error closing WPP session after logout');
      }
    }

    this.sessions.delete(sessionName);
    this.setStatus(sessionName, 'disconnected');

    // Delete persisted tokens so next connect requires a new QR
    const fs = await import('node:fs/promises');
    const tokenPaths = [`./tokens/${sessionName}`, `./tokens/${sessionName}-profile`];
    for (const p of tokenPaths) {
      try {
        await fs.rm(p, { recursive: true, force: true });
        logger.info({ sessionName, path: p }, 'Deleted token directory');
      } catch {
        // Ignore — may not exist
      }
    }

    logger.info({ sessionName }, 'WPPConnect session logged out (tokens deleted)');
  }

  getStatus(sessionName: string): InstanceStatus {
    return this.statuses.get(sessionName) ?? 'disconnected';
  }

  getPhone(sessionName: string): string | null {
    return this.phones.get(sessionName) ?? null;
  }

  onQRCode(callback: QRCallback): void {
    this.qrCallbacks.push(callback);
  }

  onStatusChange(callback: StatusCallback): void {
    this.statusCallbacks.push(callback);
  }

  onMessage(callback: MessageCallback): void {
    this.messageCallbacks.push(callback);
  }

  // --- Internal helpers ---

  /**
   * WPPConnect v2.x throws "Message true_...@lid_..._out not found" after
   * successfully delivering a message. The message IS sent but internal
   * tracking of the new @lid ID format fails. Treat as success.
   */
  private handleSendError(sessionName: string, to: string, err: unknown, method: string): SendResult {
    const error = err instanceof Error ? err.message : String(err);
    if (error.includes('not found') && error.includes('@lid')) {
      logger.warn({ sessionName, to, error }, `${method}: message delivered but post-send tracking failed (WPP @lid bug), treating as success`);
      return { success: true, providerMessageId: undefined };
    }
    logger.error({ sessionName, to, error }, `${method} failed`);
    return { success: false, error };
  }

  private setStatus(sessionName: string, status: InstanceStatus): void {
    const prev = this.statuses.get(sessionName);
    if (prev === status) return;
    this.statuses.set(sessionName, status);
    for (const cb of this.statusCallbacks) {
      cb(sessionName, status);
    }
  }

  private getClientOrThrow(sessionName: string): WPPClient {
    const client = this.sessions.get(sessionName);
    if (!client) {
      throw new Error(`No active session: ${sessionName}`);
    }
    return client;
  }

  private mapToRawMessage(sessionName: string, msg: unknown): RawIncomingMessage | null {
    const m = msg as Record<string, unknown>;
    // Skip non-real messages (status broadcasts, groups, self-sent)
    if (!m.from || (m.from as string).includes('status@broadcast')) return null;
    if (m.isGroupMsg) return null;
    if (m.fromMe) return null;

    return {
      from: (m.from as string).replace('@c.us', ''),
      to: (m.to as string)?.replace('@c.us', '') ?? sessionName,
      body: (m.body as string) ?? '',
      type: (m.type as string) ?? 'chat',
      isGroupMsg: false,
      sender: { pushname: (m.sender as Record<string, unknown>)?.pushname as string | undefined },
      listResponse: m.listResponse as RawIncomingMessage['listResponse'],
      timestamp: (m.timestamp as number) ?? Math.floor(Date.now() / 1000),
      id: (m.id as string) ?? '',
    };
  }
}
