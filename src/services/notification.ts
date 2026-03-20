import { query } from '../db/index';
import { logger } from '../utils/logger';

export interface NotificationEvent {
  event_id: string;
  type: 'email' | 'sms' | 'push';
  recipient: string;
  payload: any;
  timestamp: string;
}

/** Custom error to differentiate transient from permanent failures */
export class TransientError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TransientError';
  }
}

export class PermanentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentError';
  }
}

/**
 * Simulates dispatching a notification via an external service.
 * - If payload contains `force_fail: true`, always throws a TransientError (for testing).
 * - If payload contains `permanent_fail: true`, always throws a PermanentError (for testing).
 * - Otherwise, simulates a 500ms delay and a 10% random transient failure rate.
 */
export async function dispatchNotification(event: NotificationEvent): Promise<void> {
  logger.info('Simulating notification dispatch', {
    eventId: event.event_id,
    type: event.type,
    recipient: event.recipient,
  });

  // Check for forced failure flags in payload (for controlled testing)
  if (event.payload?.force_fail === true) {
    throw new TransientError('Forced transient failure for testing');
  }

  if (event.payload?.permanent_fail === true) {
    throw new PermanentError('Permanent failure: invalid notification configuration');
  }

  // Simulate external API call latency
  await new Promise((resolve) => setTimeout(resolve, 500));

  // Random transient failure simulation (10% chance)
  if (Math.random() < 0.1) {
    throw new TransientError('Transient external service failure');
  }

  logger.info('Notification dispatched successfully', {
    eventId: event.event_id,
    type: event.type,
  });
}

/**
 * Logs a notification record to the notification_logs table.
 */
export async function logNotification(event: NotificationEvent, status: string): Promise<void> {
  try {
    await query(
      `INSERT INTO notification_logs (event_id, recipient, type, message_payload, status)
       VALUES ($1, $2, $3, $4, $5)`,
      [event.event_id, event.recipient, event.type, JSON.stringify(event.payload), status],
    );
    logger.info('Notification logged to database', {
      eventId: event.event_id,
      status,
    });
  } catch (error) {
    logger.error('Error logging notification to database', {
      eventId: event.event_id,
      error: String(error),
    });
    throw error;
  }
}
