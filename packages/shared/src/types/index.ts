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

export interface RawIncomingMessage {
  from: string;
  to: string;
  body: string;
  type: string;
  isGroupMsg: boolean;
  sender: { pushname?: string };
  listResponse?: {
    singleSelectReply?: {
      selectedRowId?: string;
    };
  };
  timestamp: number;
  id: string;
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
