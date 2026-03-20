import { query } from '../db/index';
import { logger } from '../utils/logger';

export enum EventStatus {
  PROCESSING = 'PROCESSING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

/**
 * Atomically checks if an event_id has been processed and marks it as PROCESSING if not.
 * Uses INSERT ... ON CONFLICT for atomic upsert to prevent race conditions.
 *
 * Returns:
 *  - alreadyProcessed: true if event was already COMPLETED
 *  - isProcessing: true if event is currently being processed by another consumer
 */
export async function checkAndMarkProcessing(
  eventId: string,
): Promise<{ alreadyProcessed: boolean; isProcessing: boolean }> {
  try {
    // Attempt atomic insert with ON CONFLICT:
    // - If event_id doesn't exist → inserts with PROCESSING status
    // - If event_id exists with NOT COMPLETED/PROCESSING → updates to PROCESSING
    // - If event_id exists with COMPLETED or PROCESSING → WHERE clause prevents update, rowCount=0
    const result = await query(
      `INSERT INTO processed_events (event_id, status)
       VALUES ($1, $2)
       ON CONFLICT (event_id) DO UPDATE
       SET status = $2, updated_at = CURRENT_TIMESTAMP
       WHERE processed_events.status NOT IN ($3, $4)
       RETURNING status`,
      [eventId, EventStatus.PROCESSING, EventStatus.COMPLETED, EventStatus.PROCESSING],
    );

    if (result.rowCount === 0) {
      // Insert/update failed due to WHERE clause — check current status
      const currentStatusResult = await query(
        'SELECT status FROM processed_events WHERE event_id = $1',
        [eventId],
      );
      const status = currentStatusResult.rows[0]?.status;

      return {
        alreadyProcessed: status === EventStatus.COMPLETED,
        isProcessing: status === EventStatus.PROCESSING,
      };
    }

    // Successfully inserted/updated → this consumer owns the event
    return { alreadyProcessed: false, isProcessing: false };
  } catch (error) {
    logger.error('Error in idempotency check', { eventId, error: String(error) });
    throw error;
  }
}

/**
 * Updates the status of an event in the processed_events table.
 */
export async function updateEventStatus(eventId: string, status: EventStatus): Promise<void> {
  await query(
    'UPDATE processed_events SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE event_id = $2',
    [status, eventId],
  );
  logger.info('Event status updated', { eventId, status });
}
