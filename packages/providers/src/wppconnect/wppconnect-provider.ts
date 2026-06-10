import wppconnect from '@wppconnect-team/wppconnect';
import { execSync } from 'node:child_process';
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
  RawMediaData,
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
  /** LID → real phone cache (null = unresolvable). Evicts oldest when over MAX_CACHE. */
  private phoneCache = new Map<string, string | null>();
  /** contactId → display name cache (null = unknown). Evicts oldest when over MAX_CACHE. */
  private contactNameCache = new Map<string, string | null>();
  private static readonly MAX_CACHE = 5000;

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
        const number = await (client as unknown as Record<string, any>).page?.evaluate(
          () => (globalThis as any).Store?.Conn?.wid?.user,
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
    client.onMessage(async (msg: unknown) => {
      const raw = await this.mapToRawMessage(sessionName, msg, client);
      if (!raw) return;
      for (const cb of this.messageCallbacks) {
        cb(sessionName, raw);
      }
    });

    // Capture device-sent messages (us → customer, sent from phone)
    (client as unknown as { onAnyMessage: (cb: (msg: unknown) => void) => void }).onAnyMessage(async (msg: unknown) => {
      const m = msg as Record<string, unknown>;
      if (!m.fromMe) return; // inbound already handled by onMessage
      if (!m.to || (m.to as string).includes('status@broadcast')) return;
      if (m.isGroupMsg) return;

      // Skip messages sent by the hub (already persisted by the send endpoint)
      const msgId = (m.id as string) ?? '';
      if (this.hubSentIds.delete(msgId)) return;

      const msgType = (m.type as string) ?? 'chat';

      // Download media for media messages
      const media = await this.downloadMediaSafe(client, m, msgType, sessionName);

      const raw: RawIncomingMessage = {
        from: (m.to as string)?.replace(/@(c\.us|lid)$/, '') ?? '',
        to: (m.from as string)?.replace(/@(c\.us|lid)$/, '') ?? '',
        body: (m.body as string) ?? '',
        type: msgType,
        isGroupMsg: false,
        sender: { pushname: undefined, contactName: undefined },
        timestamp: (m.timestamp as number) ?? Math.floor(Date.now() / 1000),
        id: (m.id as string) ?? '',
        fromMe: true,
        media,
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
    if (/^55\d{10,11}$/.test(lidOrPhone)) return lidOrPhone;

    const cached = this.phoneCache.get(lidOrPhone);
    if (cached !== undefined) return cached;

    const client = this.getClientOrThrow(sessionName);
    const chatId = `${lidOrPhone}@lid`;

    // getPnLidEntry returns { phoneNumber: { id: "5511...", server: "c.us" }, ... }
    try {
      const entry = await (client as unknown as {
        getPnLidEntry: (id: string) => Promise<unknown>;
      }).getPnLidEntry(chatId);
      if (entry && typeof entry === 'object') {
        const e = entry as Record<string, unknown>;
        const pn = e.phoneNumber as Record<string, unknown> | undefined;
        const phone = (pn?.id as string) ?? null;
        if (phone && /^55\d{10,11}$/.test(phone)) {
          logger.info({ sessionName, lid: lidOrPhone, phone }, 'Resolved phone via getPnLidEntry');
          this.phoneCache.set(lidOrPhone, phone);
          this.evictIfNeeded(this.phoneCache);
          return phone;
        }
      }
    } catch (err) {
      logger.warn({ sessionName, lidOrPhone, err }, 'getPnLidEntry failed');
    }

    this.phoneCache.set(lidOrPhone, null);
    this.evictIfNeeded(this.phoneCache);
    return null;
  }

  async getMessages(sessionName: string, chatId: string, count = 50): Promise<RawIncomingMessage[]> {
    const client = this.getClientOrThrow(sessionName);
    try {
      const msgs = await client.getMessages(chatId, { count });
      return (msgs as unknown as Array<Record<string, unknown>>)
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
          sender: {
            pushname: ((m.sender as Record<string, unknown>)?.pushname as string) ?? undefined,
            contactName: (m.sender as Record<string, unknown>)?.name as string | undefined
              ?? (m.sender as Record<string, unknown>)?.formattedName as string | undefined
              ?? (m.sender as Record<string, unknown>)?.verifiedName as string | undefined
              ?? (m as Record<string, unknown>).notifyName as string | undefined,
          },
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

  async getContactName(sessionName: string, contactId: string): Promise<string | null> {
    const cached = this.contactNameCache.get(contactId);
    if (cached !== undefined) return cached;

    const client = this.getClientOrThrow(sessionName);
    try {
      const contact = await client.getContact(contactId);
      const name = contact.name || contact.pushname || contact.verifiedName || null;
      this.contactNameCache.set(contactId, name);
      this.evictIfNeeded(this.contactNameCache);
      logger.info({ sessionName, contactId, name }, 'Resolved contact name via getContact');
      return name;
    } catch (err) {
      logger.warn({ sessionName, contactId, err }, 'getContact failed');
      this.contactNameCache.set(contactId, null);
      this.evictIfNeeded(this.contactNameCache);
      return null;
    }
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

  private static readonly MEDIA_TYPES = new Set(['image', 'audio', 'ptt', 'video', 'document', 'sticker']);
  private static readonly MAX_MEDIA_SIZE = 5 * 1024 * 1024; // 5 MB

  private async downloadMediaSafe(
    client: WPPClient,
    m: Record<string, unknown>,
    msgType: string,
    sessionName: string,
  ): Promise<RawMediaData | undefined> {
    if (!WPPConnectProvider.MEDIA_TYPES.has(msgType)) return undefined;

    // Skip oversized media
    const size = m.size as number | undefined;
    if (size && size > WPPConnectProvider.MAX_MEDIA_SIZE) {
      logger.info({ sessionName, msgId: m.id, size, type: msgType }, 'Skipping oversized media');
      return { mimetype: (m.mimetype as string) ?? '', base64: '', caption: (m.caption as string) ?? undefined, filename: (m.filename as string) ?? undefined, size };
    }

    try {
      const base64 = await client.downloadMedia(m.id as string);
      const mimetype = (m.mimetype as string) ?? '';
      const mediaData = m.mediaData as Record<string, unknown> | undefined;
      return {
        mimetype,
        base64: base64.startsWith('data:') ? base64 : `data:${mimetype};base64,${base64}`,
        caption: (m.caption as string) ?? undefined,
        filename: (m.filename as string) ?? undefined,
        size,
        duration: (mediaData?.duration as number) ?? undefined,
      };
    } catch (err) {
      logger.warn({ sessionName, msgId: m.id, type: msgType, err }, 'Failed to download media');
      return { mimetype: (m.mimetype as string) ?? '', base64: '', caption: (m.caption as string) ?? undefined, filename: (m.filename as string) ?? undefined, size };
    }
  }

  private async mapToRawMessage(sessionName: string, msg: unknown, client: WPPClient): Promise<RawIncomingMessage | null> {
    const m = msg as Record<string, unknown>;
    // Skip non-real messages (status broadcasts, groups)
    if (!m.from || (m.from as string).includes('status@broadcast')) return null;
    if (m.isGroupMsg) return null;
    if (m.fromMe) return null;

    const s = m.sender as Record<string, unknown> | undefined;
    const msgType = (m.type as string) ?? 'chat';

    // DEBUG: log sender fields to diagnose missing contact name for unknown contacts
    logger.info({
      sessionName,
      from: (m.from as string),
      senderPushname: s?.pushname,
      senderName: s?.name,
      senderFormattedName: s?.formattedName,
      senderVerifiedName: s?.verifiedName,
      msgNotifyName: (m as Record<string, unknown>).notifyName,
      msgPushname: (m as Record<string, unknown>).pushname,
      type: msgType,
    }, 'DEBUG: inbound message sender info');

    // Download media for media messages
    const media = await this.downloadMediaSafe(client, m, msgType, sessionName);

    return {
      from: (m.from as string).replace(/@(c\.us|lid)$/, ''),
      to: (m.to as string)?.replace(/@(c\.us|lid)$/, '') ?? sessionName,
      body: (m.body as string) ?? '',
      type: msgType,
      isGroupMsg: false,
      sender: {
        pushname: s?.pushname as string | undefined,
        contactName: s?.name as string | undefined
          ?? s?.formattedName as string | undefined
          ?? s?.verifiedName as string | undefined
          ?? (m as Record<string, unknown>).notifyName as string | undefined,
      },
      listResponse: m.listResponse as RawIncomingMessage['listResponse'],
      timestamp: (m.timestamp as number) ?? Math.floor(Date.now() / 1000),
      id: (m.id as string) ?? '',
      media,
    };
  }

  /** Evict oldest entries from a Map when it exceeds MAX_CACHE */
  private evictIfNeeded<V>(cache: Map<string, V>): void {
    if (cache.size <= WPPConnectProvider.MAX_CACHE) return;
    const excess = cache.size - WPPConnectProvider.MAX_CACHE;
    const iter = cache.keys();
    for (let i = 0; i < excess; i++) {
      const key = iter.next().value;
      if (key !== undefined) cache.delete(key);
    }
  }
}
