/**
 * Automation rules: map business events from Core to messaging actions.
 *
 * Each rule defines:
 * - eventType: which Core event triggers this rule
 * - templateSlug: which message template to use
 * - delayMs: delay before sending (0 = immediate)
 * - channel: messaging channel (default whatsapp)
 * - extractVariables: function to build template variables from the event payload
 * - extractRecipient: function to extract the recipient phone from the payload
 * - condition: optional function to evaluate whether the rule should fire
 */

export interface CoreEventPayload {
  customer_name?: string;
  customer_phone?: string;
  professional_name?: string;
  service_name?: string;
  services?: string;
  starts_at?: string;
  date?: string;
  time?: string;
  total_price?: string;
  appointment_uuid?: string;
  appointment_id?: number;
  payment_method?: string;
  amount?: string;
  tenant_name?: string;
  tenant_phone?: string;
  [key: string]: unknown;
}

export interface AutomationRule {
  id: string;
  eventType: string;
  templateSlug: string;
  delayMs: number;
  channel: 'whatsapp';
  messageType: 'text' | 'list';
  extractVariables: (payload: CoreEventPayload) => Record<string, string>;
  extractRecipient: (payload: CoreEventPayload) => string | null;
  condition?: (payload: CoreEventPayload) => boolean;
}

function defaultRecipient(payload: CoreEventPayload): string | null {
  return payload.customer_phone ?? null;
}

function appointmentVariables(payload: CoreEventPayload): Record<string, string> {
  return {
    nome: payload.customer_name ?? '',
    profissional: payload.professional_name ?? '',
    servico: payload.service_name ?? payload.services ?? '',
    data: payload.date ?? payload.starts_at ?? '',
    horario: payload.time ?? '',
    valor: payload.total_price ?? '',
    empresa: payload.tenant_name ?? '',
    telefone_empresa: payload.tenant_phone ?? '',
  };
}

export const DEFAULT_RULES: AutomationRule[] = [
  {
    id: 'appointment-created-confirmation',
    eventType: 'appointment.created',
    templateSlug: 'confirmacao-agendamento',
    delayMs: 10_000,
    channel: 'whatsapp',
    messageType: 'list',
    extractVariables: appointmentVariables,
    extractRecipient: defaultRecipient,
  },
  {
    id: 'appointment-confirmed-thanks',
    eventType: 'appointment.confirmed',
    templateSlug: 'confirmado-obrigado',
    delayMs: 10_000,
    channel: 'whatsapp',
    messageType: 'text',
    extractVariables: appointmentVariables,
    extractRecipient: defaultRecipient,
  },
  {
    id: 'appointment-cancelled-notice',
    eventType: 'appointment.cancelled',
    templateSlug: 'cancelamento-aviso',
    delayMs: 10_000,
    channel: 'whatsapp',
    messageType: 'text',
    extractVariables: appointmentVariables,
    extractRecipient: defaultRecipient,
  },
  {
    id: 'appointment-rescheduled-notice',
    eventType: 'appointment.rescheduled',
    templateSlug: 'reagendamento-aviso',
    delayMs: 10_000,
    channel: 'whatsapp',
    messageType: 'text',
    extractVariables: appointmentVariables,
    extractRecipient: defaultRecipient,
  },
  {
    id: 'payment-received-receipt',
    eventType: 'payment.received',
    templateSlug: 'recibo-pagamento',
    delayMs: 10_000,
    channel: 'whatsapp',
    messageType: 'text',
    extractVariables: (payload) => ({
      ...appointmentVariables(payload),
      metodo_pagamento: payload.payment_method ?? '',
      valor_pago: payload.amount ?? payload.total_price ?? '',
    }),
    extractRecipient: defaultRecipient,
  },
  {
    id: 'appointment-noshow-notice',
    eventType: 'appointment.no_show',
    templateSlug: 'ausencia-aviso',
    delayMs: 10_000,
    channel: 'whatsapp',
    messageType: 'text',
    extractVariables: appointmentVariables,
    extractRecipient: defaultRecipient,
  },
];
