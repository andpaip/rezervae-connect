export interface BaseEvent {
  tenantId: string;
  traceId: string;
  correlationId: string;
  timestamp: string;
  version: string;
}

export interface MessageReceivedEvent extends BaseEvent {
  type: 'message.received';
  data: {
    sessionName: string;
    from: string;
    body: string;
    messageType: string;
    customerPhone?: string;
    customerName?: string | null;
    sessionId?: string;
    channel?: string;
    direction?: string;
    providerMessageId?: string;
  };
}

export interface MessageSentEvent extends BaseEvent {
  type: 'message.sent';
  data: { messageLogId: string; to: string; status: string };
}

export interface MessageFailedEvent extends BaseEvent {
  type: 'message.failed';
  data: { messageLogId: string; to: string; error: string };
}

export interface QRGeneratedEvent extends BaseEvent {
  type: 'qr.generated';
  data: { instanceId: string; sessionName: string; qrCode: string };
}

export interface InstanceConnectedEvent extends BaseEvent {
  type: 'instance.connected';
  data: { instanceId: string; sessionName: string };
}

export interface InstanceDisconnectedEvent extends BaseEvent {
  type: 'instance.disconnected';
  data: { instanceId: string; sessionName: string; reason?: string };
}

export interface InstanceDegradedEvent extends BaseEvent {
  type: 'instance.degraded';
  data: { instanceId: string; sessionName: string; healthScore: number };
}

export interface CampaignStartedEvent extends BaseEvent {
  type: 'campaign.started';
  data: { campaignId: string; totalRecipients: number };
}

export interface CampaignProgressEvent extends BaseEvent {
  type: 'campaign.progress';
  data: { campaignId: string; sent: number; total: number; errors: number };
}

export interface CampaignFinishedEvent extends BaseEvent {
  type: 'campaign.finished';
  data: { campaignId: string; stats: Record<string, number> };
}

export interface ConversationCreatedEvent extends BaseEvent {
  type: 'conversation.created';
  data: { sessionId: string; customerPhone: string; channel: string };
}

export interface ConversationAssignedEvent extends BaseEvent {
  type: 'conversation.assigned';
  data: { sessionId: string; fromUserId?: string; toUserId: string; reason: string };
}

// --- Inbox events ---

export interface InboxMessageEvent extends BaseEvent {
  type: 'inbox.message';
  data: {
    threadId: string;
    messageId: string;
    sessionName: string;
    from: string;
    body: string;
    messageType: string;
    customerPhone: string;
    customerName: string;
    unreadCount: number;
  };
}

export interface InboxMessageSentEvent extends BaseEvent {
  type: 'inbox.message.sent';
  data: {
    threadId: string;
    messageId: string;
    to: string;
    content: string;
  };
}

export interface InboxThreadUpdatedEvent extends BaseEvent {
  type: 'inbox.thread.updated';
  data: {
    threadId: string;
    status?: string;
    assignedUserId?: string | null;
    unreadCount?: number;
    action: 'claimed' | 'released' | 'closed' | 'reopened' | 'read' | 'sync';
  };
}

export type ConnectEvent =
  | MessageReceivedEvent
  | MessageSentEvent
  | MessageFailedEvent
  | QRGeneratedEvent
  | InstanceConnectedEvent
  | InstanceDisconnectedEvent
  | InstanceDegradedEvent
  | CampaignStartedEvent
  | CampaignProgressEvent
  | CampaignFinishedEvent
  | ConversationCreatedEvent
  | ConversationAssignedEvent
  | InboxMessageEvent
  | InboxMessageSentEvent
  | InboxThreadUpdatedEvent;

export type ConnectEventType = ConnectEvent['type'];
