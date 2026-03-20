// @ts-nocheck
import { processNotificationEvent } from '../../src/consumer/index';
import * as db from '../../src/db/index';
import * as mqService from '../../src/services/mq';
import { NotificationEvent, TransientError } from '../../src/services/notification';
import { EventStatus } from '../../src/services/idempotency';
import { Channel, ConsumeMessage } from 'amqplib';
import { jest } from '@jest/globals';

// Mock the database module
jest.mock('../../src/db/index');

// Mock the MQ service (only publishToDLQ — consumer uses channel directly)
jest.mock('../../src/services/mq', () => ({
  ...jest.requireActual('../../src/services/mq'),
  publishToDLQ: jest.fn().mockResolvedValue(undefined),
}));

describe('Integration Tests: Consumer End-to-End Flow', () => {
  const mockQuery = db.query as jest.Mock;
  const mockPublishToDLQ = mqService.publishToDLQ as jest.Mock;

  const mockChannel = {
    ack: jest.fn(),
    nack: jest.fn(),
    sendToQueue: jest.fn(),
  } as unknown as Channel;

  const baseEvent: NotificationEvent = {
    event_id: '550e8400-e29b-41d4-a716-446655440000',
    type: 'email',
    recipient: 'user@example.com',
    payload: { subject: 'Welcome', body: 'Hello!' },
    timestamp: '2026-03-19T10:30:00Z',
  };

  function createMsg(event: any, retryCount = 0): ConsumeMessage {
    return {
      content: Buffer.from(JSON.stringify(event)),
      properties: { headers: { 'x-retry-count': retryCount } },
      fields: {},
    } as unknown as ConsumeMessage;
  }

  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(Math, 'random').mockReturnValue(0.5); // No random failure
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('End-to-End Successful Processing', () => {
    it('should process a notification event, update DB, log, and ack', async () => {
      // Mock: idempotency INSERT succeeds (new event)
      mockQuery
        .mockResolvedValueOnce({ rowCount: 1, rows: [{ status: 'PROCESSING' }] })  // checkAndMarkProcessing
        .mockResolvedValueOnce({ rowCount: 1 })  // updateEventStatus → COMPLETED
        .mockResolvedValueOnce({ rowCount: 1 }); // logNotification → SENT

      const msg = createMsg(baseEvent);
      await processNotificationEvent(msg, mockChannel);

      // Verify idempotency check
      expect(mockQuery).toHaveBeenNthCalledWith(
        1,
        expect.stringContaining('INSERT INTO processed_events'),
        expect.arrayContaining([baseEvent.event_id, EventStatus.PROCESSING]),
      );

      // Verify status update to COMPLETED
      expect(mockQuery).toHaveBeenNthCalledWith(
        2,
        expect.stringContaining('UPDATE processed_events SET status'),
        [EventStatus.COMPLETED, baseEvent.event_id],
      );

      // Verify notification log entry
      expect(mockQuery).toHaveBeenNthCalledWith(
        3,
        expect.stringContaining('INSERT INTO notification_logs'),
        expect.arrayContaining([baseEvent.event_id, 'user@example.com', 'email', 'SENT']),
      );

      // Verify message acknowledged
      expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    });
  });

  describe('Idempotency: Duplicate Event Handling', () => {
    it('should skip processing and ack when same event_id is sent twice', async () => {
      // Mock: idempotency INSERT fails (event exists with COMPLETED)
      mockQuery
        .mockResolvedValueOnce({ rowCount: 0 })  // INSERT ON CONFLICT fails
        .mockResolvedValueOnce({ rows: [{ status: EventStatus.COMPLETED }] }); // SELECT status

      const msg = createMsg(baseEvent);
      await processNotificationEvent(msg, mockChannel);

      // Should not call dispatch or logging
      expect(mockQuery).toHaveBeenCalledTimes(2); // Only idempotency queries
      expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    });
  });

  describe('DLQ: Message Movement After Max Retries', () => {
    it('should move event to DLQ after exhausting retries and update status to FAILED', async () => {
      // Use force_fail to guarantee failure
      const failEvent = {
        ...baseEvent,
        event_id: '550e8400-e29b-41d4-a716-446655440001',
        payload: { subject: 'Test', force_fail: true },
      };

      // Mock: new event, allow processing
      mockQuery
        .mockResolvedValueOnce({ rowCount: 1, rows: [{ status: 'PROCESSING' }] }) // checkAndMarkProcessing
        .mockResolvedValueOnce({ rowCount: 1 }) // updateEventStatus → FAILED
        .mockResolvedValueOnce({ rowCount: 1 }); // logNotification → DLQ_MOVED

      // retryCount = 3 (maxRetries), exhausted
      const msg = createMsg(failEvent, 3);
      await processNotificationEvent(msg, mockChannel);

      // Verify status updated to FAILED
      expect(mockQuery).toHaveBeenNthCalledWith(
        2,
        expect.stringContaining('UPDATE processed_events SET status'),
        [EventStatus.FAILED, failEvent.event_id],
      );

      // Verify notification logged as DLQ_MOVED
      expect(mockQuery).toHaveBeenNthCalledWith(
        3,
        expect.stringContaining('INSERT INTO notification_logs'),
        expect.arrayContaining([failEvent.event_id, 'DLQ_MOVED']),
      );

      // Verify published to DLQ
      expect(mockPublishToDLQ).toHaveBeenCalledWith(
        failEvent,
        expect.any(String),
        3,
      );

      // Verify message acknowledged (not left on queue)
      expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    });
  });

  describe('Retry: Exponential Backoff', () => {
    it('should re-publish with incremented retry count on transient failure at retry 0', async () => {
      const failEvent = {
        ...baseEvent,
        event_id: '550e8400-e29b-41d4-a716-446655440002',
        payload: { subject: 'Test', force_fail: true },
      };

      // Mock: new event
      mockQuery.mockResolvedValueOnce({ rowCount: 1, rows: [{ status: 'PROCESSING' }] });

      const msg = createMsg(failEvent, 0);
      await processNotificationEvent(msg, mockChannel);

      // Original message acked
      expect(mockChannel.ack).toHaveBeenCalledWith(msg);

      // Verify re-publication with retry count = 1
      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        'notification_events.retry',
        expect.any(Buffer),
        expect.objectContaining({
          headers: { 'x-retry-count': 1 },
          persistent: true,
          expiration: '1000',
        }),
      );
    });

    it('should increase delay exponentially for each retry (1s, 5s, 25s)', async () => {
      const failEvent = {
        ...baseEvent,
        event_id: '550e8400-e29b-41d4-a716-446655440003',
        payload: { subject: 'Test', force_fail: true },
      };

      // Test at retry 1: delay should be 1000 * 5^1 = 5000ms
      mockQuery.mockResolvedValueOnce({ rowCount: 1, rows: [{ status: 'PROCESSING' }] });

      const msg = createMsg(failEvent, 1);
      await processNotificationEvent(msg, mockChannel);

      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        'notification_events.retry',
        expect.any(Buffer),
        expect.objectContaining({
          headers: { 'x-retry-count': 2 },
          expiration: '5000',
        }),
      );
    });
  });

  describe('SMS and Push Notification Types', () => {
    it('should process SMS notification events', async () => {
      const smsEvent = { ...baseEvent, type: 'sms' as const, recipient: '+1234567890' };
      mockQuery
        .mockResolvedValueOnce({ rowCount: 1, rows: [{ status: 'PROCESSING' }] })
        .mockResolvedValueOnce({ rowCount: 1 })
        .mockResolvedValueOnce({ rowCount: 1 });

      const msg = createMsg(smsEvent);
      await processNotificationEvent(msg, mockChannel);

      expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    });

    it('should process push notification events', async () => {
      const pushEvent = { ...baseEvent, type: 'push' as const, recipient: 'device-token-123' };
      mockQuery
        .mockResolvedValueOnce({ rowCount: 1, rows: [{ status: 'PROCESSING' }] })
        .mockResolvedValueOnce({ rowCount: 1 })
        .mockResolvedValueOnce({ rowCount: 1 });

      const msg = createMsg(pushEvent);
      await processNotificationEvent(msg, mockChannel);

      expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    });
  });
});
