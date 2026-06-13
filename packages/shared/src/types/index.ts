// === Enums ===

export const InstanceStatus = {
  DISCONNECTED: 'disconnected',
  CONNECTING: 'connecting',
  QR_READY: 'qr_ready',
  CONNECTED: 'connected',
  ERROR: 'error',
  DEGRADED: 'degraded',
} as const;
export type InstanceStatus = (typeof InstanceStatus)[keyof typeof InstanceStatus];

export const MessageDirection = {
  INBOUND: 'inbound',
  OUTBOUND: 'outbound',
} as const;
export type MessageDirection = (typeof MessageDirection)[keyof typeof MessageDirection];

export const MessageStatus = {
  QUEUED: 'queued',
  SENT: 'sent',
  DELIVERED: 'delivered',
  READ: 'read',
  FAILED: 'failed',
} as const;
export type MessageStatus = (typeof MessageStatus)[keyof typeof MessageStatus];

export const CampaignStatus = {
  DRAFT: 'draft',
  RUNNING: 'running',
  PAUSED: 'paused',
  FINISHED: 'finished',
  CANCELED: 'canceled',
} as const;
export type CampaignStatus = (typeof CampaignStatus)[keyof typeof CampaignStatus];

export const ConversationState = {
  OPEN: 'open',
  CLOSED: 'closed',
  ARCHIVED: 'archived',
} as const;
export type ConversationState = (typeof ConversationState)[keyof typeof ConversationState];

export const ConversationStatus = {
  BOT: 'bot',
  AI: 'ai',
  HUMAN: 'human',
  AUTOMATION: 'automation',
} as const;
export type ConversationStatus = (typeof ConversationStatus)[keyof typeof ConversationStatus];

// === Provider Interface ===

export interface ConnectionConfig {
  sessionName: string;
  tenantId: string;
  instanceId: string;
  headless?: boolean;
}

export interface SendParams {
  sessionName: string;
  to: string;
  content: string;
}

export interface SendImageParams extends SendParams {
  imageUrl: string;
  caption: string;
}

export interface SendListParams extends SendParams {
  buttonText: string;
  sections: Array<{
    title: string;
    rows: Array<{
      rowId: string;
      title: string;
      description?: string;
    }>;
  }>;
}

export interface SendResult {
  success: boolean;
  providerMessageId?: string;
  error?: string;
}

export interface RawMediaData {
  mimetype: string;
  base64: string;
  caption?: string;
  filename?: string;
  size?: number;
  duration?: number;
}

export interface RawIncomingMessage {
  from: string;
  to: string;
  body: string;
  type: string;
  isGroupMsg: boolean;
  sender: { pushname?: string; contactName?: string; profilePicUrl?: string };
  listResponse?: {
    singleSelectReply?: {
      selectedRowId?: string;
    };
  };
  timestamp: number;
  id: string;
  /** True when message was sent FROM the device (outbound captured via onAnyMessage) */
  fromMe?: boolean;
  /** Media data for image/audio/video/document/sticker messages */
  media?: RawMediaData;
}

export interface ChannelProvider {
  connect(config: ConnectionConfig): Promise<void>;
  disconnect(sessionName: string): Promise<void>;
  sendMessage(params: SendParams): Promise<SendResult>;
  sendImage(params: SendImageParams): Promise<SendResult>;
  sendListMessage(params: SendListParams): Promise<SendResult>;
  getStatus(sessionName: string): InstanceStatus;
  onQRCode(callback: (session: string, qr: string) => void): void;
  onStatusChange(callback: (session: string, status: InstanceStatus) => void): void;
  onMessage(callback: (session: string, message: RawIncomingMessage) => void): void;
  onDeviceMessage(callback: (session: string, message: RawIncomingMessage) => void): void;
  /** Fetch recent messages from a chat (used for history sync). */
  getMessages?(sessionName: string, chatId: string, count?: number): Promise<RawIncomingMessage[]>;
  /** Resolve real phone number from a LID (WhatsApp internal ID). */
  resolvePhone?(sessionName: string, lidOrPhone: string): Promise<string | null>;
  /** Get contact display name (from phone agenda, pushname, or business name). Cached. */
  getContactName?(sessionName: string, contactId: string): Promise<string | null>;
}

// === Internal Auth ===

export interface InternalAuthHeaders {
  'x-internal-token': string;
  'x-signature': string;
  'x-timestamp': string;
  'x-tenant-id': string;
  'x-correlation-id': string;
}

// === Tenant Context ===

export interface TenantContext {
  tenantId: string;
  tenantSlug: string;
  settings: Record<string, unknown>;
  traceId: string;
  correlationId: string;
}
