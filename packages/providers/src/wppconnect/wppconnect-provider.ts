import wppconnect from '@wppconnect-team/wppconnect';
import { execSync } from 'node:child_process';
import { appendFileSync } from 'node:fs';
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
  private deviceMessageCallbacks: MessageCallback[] = [];
  /** IDs of messages sent by the hub — used to skip them in onAnyMessage */
  private hubSentIds = new Set<string>();

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
      // WPPConnect v2 + WA-JS: getHostDevice().id is just "1", not the phone.
      // Use getWid() which returns the WhatsApp ID (phone@c.us format).
      const wid = await client.getWid();
      const widStr = typeof wid === 'string' ? wid
        : (wid as Record<string, unknown>)?._serialized?.toString()
        ?? (wid as Record<string, unknown>)?.user?.toString()
        ?? '';
      const phone = widStr.replace('@c.us', '').replace(/\D/g, '');
      logger.info({ sessionName, rawWid: JSON.stringify(wid).slice(0, 200), phone }, 'WID extraction');
      if (phone && phone.length > 3) {
        this.phones.set(sessionName, phone);
        logger.info({ sessionName, phone }, 'Phone number captured');
      }
    } catch (err) {
      logger.warn({ sessionName, err }, 'Could not extract phone number from getWid');
      // Fallback: try to get from page context
      try {
        const number = await (client as unknown as Record<string, CallableFunction>).page?.evaluate(
          () => (window as unknown as Record<string, unknown>).Store?.Conn?.wid?.user,
        );
        if (number) {
          this.phones.set(sessionName, String(number));
          logger.info({ sessionName, phone: number }, 'Phone captured via Store.Conn.wid');
        }
      } catch {
        logger.warn({ sessionName }, 'All phone extraction methods failed');
      }
    }

    this.setStatus(sessionName, 'connected');

    // Register incoming message handler (customer → us)
    client.onMessage((msg: unknown) => {
      const raw = this.mapToRawMessage(sessionName, msg);
      if (!raw) return;
      for (const cb of this.messageCallbacks) {
        cb(sessionName, raw);
      }
    });

    // Capture device-sent messages (us → customer, sent from phone)
    (client as unknown as { onAnyMessage: (cb: (msg: unknown) => void) => void }).onAnyMessage((msg: unknown) => {
      const m = msg as Record<string, unknown>;
      if (!m.fromMe) return; // inbound already handled by onMessage
      if (!m.to || (m.to as string).includes('status@broadcast')) return;
      if (m.isGroupMsg) return;

      // Skip messages sent by the hub (already persisted by the send endpoint)
      const msgId = (m.id as string) ?? '';
      if (this.hubSentIds.delete(msgId)) return;

      const raw: RawIncomingMessage = {
        from: (m.to as string)?.replace(/@(c\.us|lid)$/, '') ?? '',
        to: (m.from as string)?.replace(/@(c\.us|lid)$/, '') ?? '',
        body: (m.body as string) ?? '',
        type: (m.type as string) ?? 'chat',
        isGroupMsg: false,
        sender: { pushname: undefined },
        timestamp: (m.timestamp as number) ?? Math.floor(Date.now() / 1000),
        id: (m.id as string) ?? '',
        fromMe: true,
      };

      for (const cb of this.deviceMessageCallbacks) {
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

  async resolvePhone(sessionName: string, lidOrPhone: string): Promise<string | null> {
    const client = this.getClientOrThrow(sessionName);
    const chatId = /^55\d{10,11}$/.test(lidOrPhone)
      ? `${lidOrPhone}@c.us`
      : `${lidOrPhone}@lid`;

    const debugFile = '/tmp/resolve-phone-debug.log';

    // Try getPnLidEntry first (LID↔phone mapping)
    try {
      const entry = await (client as unknown as {
        getPnLidEntry: (id: string) => Promise<unknown>;
      }).getPnLidEntry(chatId);
      const entryJson = JSON.stringify(entry, null, 2);
      logger.info({ sessionName, lid: lidOrPhone }, 'getPnLidEntry raw result');
      appendFileSync(debugFile, `\n[${new Date().toISOString()}] getPnLidEntry(${chatId}):\n${entryJson}\n`);
      if (entry && typeof entry === 'object') {
        const e = entry as Record<string, unknown>;
        const pn = e.pnNumber as Record<string, unknown> | undefined;
        const phone = (pn?.user as string) ?? null;
        if (phone && /^55\d{10,11}$/.test(phone)) {
          return phone;
        }
      }
    } catch (err) {
      logger.warn({ sessionName, lidOrPhone, err }, 'getPnLidEntry failed');
      appendFileSync(debugFile, `\n[${new Date().toISOString()}] getPnLidEntry(${chatId}) ERROR: ${err}\n`);
    }

    // Try getContact as fallback
    try {
      const contact = await client.getContact(chatId);
      const contactJson = JSON.stringify(contact, null, 2);
      logger.info({ sessionName, lid: lidOrPhone }, 'getContact raw result');
      appendFileSync(debugFile, `\n[${new Date().toISOString()}] getContact(${chatId}):\n${contactJson}\n`);
      const c = contact as Record<string, unknown>;
      // Check common fields where phone might be
      const idStr = (c.id as string) ?? '';
      const match = idStr.match(/^(\d+)@c\.us$/);
      if (match && /^55\d{10,11}$/.test(match[1])) {
        return match[1];
      }
    } catch (err) {
      logger.warn({ sessionName, lidOrPhone, err }, 'getContact failed');
      appendFileSync(debugFile, `\n[${new Date().toISOString()}] getContact(${chatId}) ERROR: ${err}\n`);
    }

    return null;
  }

  async getMessages(sessionName: string, chatId: string, count = 50): Promise<RawIncomingMessage[]> {
    const client = this.getClientOrThrow(sessionName);
    try {
      const msgs = await client.getMessages(chatId, { count });
      return (msgs as Array<Record<string, unknown>>)
        .filter((m) => {
          const from = m.from as string | undefined;
          return from && !from.includes('status@broadcast') && !m.isGroupMsg;
        })
        .map((m) => ({
          from: ((m.fromMe ? m.to : m.from) as string)?.replace(/@(c\.us|lid)$/, '') ?? '',
          to: ((m.fromMe ? m.from : m.to) as string)?.replace(/@(c\.us|lid)$/, '') ?? '',
          body: (m.body as string) ?? '',
          type: (m.type as string) ?? 'chat',
          isGroupMsg: false,
          sender: { pushname: ((m.sender as Record<string, unknown>)?.pushname as string) ?? undefined },
          timestamp: (m.timestamp as number) ?? Math.floor(Date.now() / 1000),
          id: (m.id as string) ?? '',
          fromMe: !!m.fromMe,
        }));
    } catch (err) {
      logger.error({ sessionName, chatId, err }, 'Failed to fetch message history');
      return [];
    }
  }

  async sendMessage(params: SendParams): Promise<SendResult> {
    const client = this.getClientOrThrow(params.sessionName);
    try {
      const result = await client.sendText(this.formatRecipient(params.to), params.content);
      const id = (result as { id?: string }).id;
      if (id) this.trackHubSent(id);
      return { success: true, providerMessageId: id };
    } catch (err) {
      return this.handleSendError(params.sessionName, params.to, err, 'sendMessage');
    }
  }

  async sendImage(params: SendImageParams): Promise<SendResult> {
    const client = this.getClientOrThrow(params.sessionName);
    try {
      const result = await client.sendImage(
        this.formatRecipient(params.to),
        params.imageUrl,
        'image',
        params.caption,
      );
      const id = (result as { id?: string }).id;
      if (id) this.trackHubSent(id);
      return { success: true, providerMessageId: id };
    } catch (err) {
      return this.handleSendError(params.sessionName, params.to, err, 'sendImage');
    }
  }

  async sendListMessage(params: SendListParams): Promise<SendResult> {
    const client = this.getClientOrThrow(params.sessionName);
    try {
      const result = await client.sendListMessage(this.formatRecipient(params.to), {
        buttonText: params.buttonText,
        description: params.content,
        sections: params.sections,
      });
      const id = (result as { id?: string }).id;
      if (id) this.trackHubSent(id);
      return { success: true, providerMessageId: id };
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
      // Grab browser PID before logout destroys the execution context
      let browserPid: number | undefined;
      try {
        const browser = (client as unknown as { page: { browser(): { process(): { pid: number } | null } } }).page.browser();
        browserPid = browser.process()?.pid;
      } catch {
        // Ignore — page may already be closed
      }

      try {
        await client.logout();
      } catch (err) {
        logger.warn({ sessionName, err }, 'Error during WPP logout (may already be logged out)');
      }

      // Kill browser process FIRST — close() hangs when logout
      // destroys the execution context mid-navigation
      if (browserPid) {
        this.killBrowserTree(sessionName, browserPid);
      }

      // Attempt close with timeout — may hang, so don't block on it
      try {
        await Promise.race([
          client.close(),
          new Promise((_, reject) => setTimeout(() => reject(new Error('close timeout')), 5000)),
        ]);
      } catch {
        // Expected — browser already killed or close timed out
      }
    } else {
      // Client not in memory — try to kill orphan Chrome by userDataDir
      logger.warn({ sessionName }, 'No client in memory, attempting orphan browser cleanup');
      await this.killOrphanBrowser(sessionName);
    }

    this.sessions.delete(sessionName);
    this.setStatus(sessionName, 'disconnected');

    // Wait for browser process tree to fully terminate before deleting tokens
    await new Promise((r) => setTimeout(r, 2000));

    // Always cleanup tokens/profile regardless of client state
    await this.cleanupTokenDirs(sessionName);

    logger.info({ sessionName }, 'WPPConnect session logged out (tokens deleted)');
  }

  private async cleanupTokenDirs(sessionName: string): Promise<void> {
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
  }

  private async killOrphanBrowser(sessionName: string): Promise<void> {
    const profileDir = `${sessionName}-profile`;
    const isWindows = process.platform === 'win32';
    try {
      if (isWindows) {
        // Find Chrome processes with matching userDataDir via WMIC
        const output = execSync(
          `wmic process where "name='chrome.exe' and commandline like '%${profileDir}%'" get processid /format:list`,
          { timeout: 5000, encoding: 'utf-8' },
        );
        const pids = output.match(/ProcessId=(\d+)/g)?.map((m) => m.split('=')[1]) ?? [];
        for (const pid of pids) {
          try {
            execSync(`taskkill /F /T /PID ${pid}`, { timeout: 5000 });
            logger.info({ sessionName, pid }, 'Killed orphan Chrome (Windows)');
          } catch {
            // Already dead
          }
        }
      } else {
        // Linux: find Chrome with matching userDataDir
        const output = execSync(
          `ps aux | grep "[c]hrome.*${profileDir}" | awk '{print $2}'`,
          { timeout: 5000, encoding: 'utf-8' },
        );
        const pids = output.trim().split('\n').filter(Boolean);
        for (const pid of pids) {
          try {
            execSync(`kill -9 ${pid}`, { timeout: 3000 });
            logger.info({ sessionName, pid }, 'Killed orphan Chrome (Linux)');
          } catch {
            // Already dead
          }
        }
      }
    } catch (err) {
      logger.warn({ sessionName, err }, 'Orphan browser cleanup failed (may be none running)');
    }
  }

  private killBrowserTree(sessionName: string, pid: number): void {
    const isWindows = process.platform === 'win32';
    try {
      if (isWindows) {
        execSync(`taskkill /F /T /PID ${pid}`, { timeout: 5000 });
      } else {
        execSync(`kill -9 -${pid} 2>/dev/null; kill -9 ${pid} 2>/dev/null`, { timeout: 3000 });
      }
      logger.info({ sessionName, pid }, 'Killed browser process tree');
    } catch {
      // Fallback: try Node's process.kill
      try {
        process.kill(pid, 'SIGKILL');
        logger.info({ sessionName, pid }, 'Killed browser process (fallback)');
      } catch {
        // Already dead
      }
    }
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

  onDeviceMessage(callback: MessageCallback): void {
    this.deviceMessageCallbacks.push(callback);
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

  /**
   * Format recipient for WPPConnect: BR phone → @c.us, LID → @lid.
   */
  private formatRecipient(to: string): string {
    return /^55\d{10,11}$/.test(to) ? `${to}@c.us` : `${to}@lid`;
  }

  /** Track a hub-sent message ID so onAnyMessage skips it (auto-expires after 30s) */
  private trackHubSent(id: string): void {
    this.hubSentIds.add(id);
    setTimeout(() => this.hubSentIds.delete(id), 30_000);
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
    // Skip non-real messages (status broadcasts, groups)
    if (!m.from || (m.from as string).includes('status@broadcast')) return null;
    if (m.isGroupMsg) return null;
    if (m.fromMe) return null;

    return {
      from: (m.from as string).replace(/@(c\.us|lid)$/, ''),
      to: (m.to as string)?.replace(/@(c\.us|lid)$/, '') ?? sessionName,
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
