import { Channel, ConsumeMessage } from 'amqplib';
import { z } from 'zod';
import { logger } from '../utils/logger';
import { checkAndMarkProcessing, updateEventStatus, EventStatus } from '../services/idempotency';
import {
  dispatchNotification,
  logNotification,
  NotificationEvent,
  PermanentError,
} from '../services/notification';
import { publishToDLQ } from '../services/mq';
import { config } from '../config/index';

const NotificationEventSchema = z.object({
  event_id: z.string().uuid(),
  type: z.enum(['email', 'sms', 'push']),
  recipient: z.string().trim().min(1),
  payload: z.record(z.string(), z.any()),
  timestamp: z.string().datetime(),
});

/**
 * Core consumer function that processes notification events from the MQ.
 *
 * Flow:
 * 1. Parse message and extract retry count from headers
 * 2. Perform idempotency check (skip if already COMPLETED)
 * 3. Dispatch notification via mock external service
 * 4. On success: mark COMPLETED, log to DB, ack message
 * 5. On transient failure: retry with exponential backoff or move to DLQ
 * 6. On permanent failure: immediately move to DLQ
 */
export async function processNotificationEvent(
  msg: ConsumeMessage | null,
  channel: Channel,
): Promise<void> {
  if (!msg) return;

  let event: NotificationEvent;
  const retryCount: number = (msg.properties.headers?.['x-retry-count'] as number) || 0;

  // Step 1: Parse message
  try {
    event = JSON.parse(msg.content.toString());
  } catch (parseError) {
    logger.error('Failed to parse message content, dead-lettering message', {
      error: String(parseError),
      rawContent: msg.content.toString().substring(0, 200),
    });

    // Invalid JSON is a permanent failure; reject without requeue so RabbitMQ routes to DLQ.
    channel.nack(msg, false, false);
    return;
  }

  const validationResult = NotificationEventSchema.safeParse(event);
  if (!validationResult.success) {
    logger.error('Invalid notification event schema, dead-lettering message', {
      errors: validationResult.error.issues,
    });
    channel.nack(msg, false, false);
    return;
  }

  event = validationResult.data;

  const eventId = event.event_id;
  logger.info('Received notification event', { eventId, type: event.type, retryCount });

  try {
    // Step 2: Idempotency check
    const { alreadyProcessed, isProcessing } = await checkAndMarkProcessing(eventId);

    if (alreadyProcessed) {
      logger.info('Event already processed (COMPLETED), skipping', { eventId });
      channel.ack(msg);
      return;
    }

    if (isProcessing && retryCount === 0) {
      logger.info('Event is currently being processed by another consumer, re-queueing', {
        eventId,
      });
      channel.nack(msg, false, true); // requeue
      return;
    }

    // Step 3: Dispatch notification
    await dispatchNotification(event);

    // Step 4: Mark as completed and log
    await updateEventStatus(eventId, EventStatus.COMPLETED);
    await logNotification(event, 'SENT');

    channel.ack(msg);
    logger.info('Event processed successfully', { eventId });
  } catch (error) {
    logger.error('Error processing event', {
      eventId,
      error: String(error),
      retryCount,
      errorType: (error as any)?.name || 'Unknown',
    });

    // Step 5 & 6: Error handling based on error type
    if (error instanceof PermanentError) {
      // Permanent errors go directly to DLQ, no retries
      logger.error('Permanent error detected, moving to DLQ immediately', { eventId });
      await handleDLQMovement(event, error, retryCount, channel, msg);
    } else if (retryCount < config.app.maxRetries) {
      // Transient error with retries remaining
      await handleRetry(event, retryCount, channel, msg);
    } else {
      // Retries exhausted
      logger.error('Max retries exhausted, moving to DLQ', {
        eventId,
        maxRetries: config.app.maxRetries,
        totalAttempts: retryCount + 1,
      });
      await handleDLQMovement(event, error as Error, retryCount, channel, msg);
    }
  }
}

/**
 * Handles retry with exponential backoff.
 * Re-publishes the message to the main queue with incremented retry count after a delay.
 */
async function handleRetry(
  event: NotificationEvent,
  retryCount: number,
  channel: Channel,
  msg: ConsumeMessage,
): Promise<void> {
  const nextRetryCount = retryCount + 1;
  // Exponential backoff: 1s, 5s, 25s (base * 5^retryCount)
  const delay = config.app.retryInitialDelay * Math.pow(5, retryCount);

  logger.info('Scheduling retry with exponential backoff', {
    eventId: event.event_id,
    attempt: nextRetryCount,
    maxRetries: config.app.maxRetries,
    delayMs: delay,
  });

  // Publish to retry queue with per-message TTL so delay survives process restarts.
  channel.sendToQueue(`${config.mq.queueName}.retry`, Buffer.from(JSON.stringify(event)), {
    persistent: true,
    expiration: String(delay),
    headers: { 'x-retry-count': nextRetryCount },
  });

  channel.ack(msg);
}

/**
 * Handles moving a message to the Dead-Letter Queue.
 * Updates the event status to FAILED and logs the DLQ movement.
 */
async function handleDLQMovement(
  event: NotificationEvent,
  error: Error,
  retryCount: number,
  channel: Channel,
  msg: ConsumeMessage,
): Promise<void> {
  try {
    await updateEventStatus(event.event_id, EventStatus.FAILED);
    await logNotification(event, 'DLQ_MOVED');
    await publishToDLQ(event, error.message, retryCount);
  } catch (dlqError) {
    logger.error('Failed to process DLQ movement, nacking message', {
      eventId: event.event_id,
      dlqError: String(dlqError),
    });
    // If DLQ processing itself fails, nack without requeue to use RabbitMQ's native DLQ routing
    channel.nack(msg, false, false);
    return;
  }

  channel.ack(msg);
}
