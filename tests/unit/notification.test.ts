// @ts-nocheck
import {
  dispatchNotification,
  logNotification,
  NotificationEvent,
  TransientError,
  PermanentError,
} from '../../src/services/notification';
import * as db from '../../src/db/index';
import { jest } from '@jest/globals';

jest.mock('../../src/db/index');

describe('Notification Service', () => {
  const mockQuery = db.query as jest.Mock;

  const mockEvent: NotificationEvent = {
    event_id: '550e8400-e29b-41d4-a716-446655440000',
    type: 'email',
    recipient: 'test@example.com',
    payload: { subject: 'Test', body: 'Hello!' },
    timestamp: '2026-03-19T10:00:00Z',
  };

  beforeEach(() => {
    jest.clearAllMocks();
    jest.restoreAllMocks();
  });

  describe('dispatchNotification', () => {
    it('should dispatch a notification successfully when no failure occurs', async () => {
      // Mock random to return > 0.1 (success)
      jest.spyOn(Math, 'random').mockReturnValue(0.5);

      await expect(dispatchNotification(mockEvent)).resolves.not.toThrow();
    });

    it('should throw TransientError on random failure (< 0.1)', async () => {
      jest.spyOn(Math, 'random').mockReturnValue(0.05);

      await expect(dispatchNotification(mockEvent)).rejects.toThrow(TransientError);
      await expect(dispatchNotification(mockEvent)).rejects.toThrow(
        'Transient external service failure',
      );
    });

    it('should throw TransientError when payload has force_fail=true', async () => {
      const failEvent = {
        ...mockEvent,
        payload: { ...mockEvent.payload, force_fail: true },
      };

      await expect(dispatchNotification(failEvent)).rejects.toThrow(TransientError);
      await expect(dispatchNotification(failEvent)).rejects.toThrow(
        'Forced transient failure for testing',
      );
    });

    it('should throw PermanentError when payload has permanent_fail=true', async () => {
      const permanentFailEvent = {
        ...mockEvent,
        payload: { ...mockEvent.payload, permanent_fail: true },
      };

      await expect(dispatchNotification(permanentFailEvent)).rejects.toThrow(PermanentError);
    });
  });

  describe('logNotification', () => {
    it('should log a notification record to the database', async () => {
      mockQuery.mockResolvedValueOnce({ rowCount: 1 });

      await logNotification(mockEvent, 'SENT');

      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('INSERT INTO notification_logs'),
        [
          mockEvent.event_id,
          mockEvent.recipient,
          mockEvent.type,
          JSON.stringify(mockEvent.payload),
          'SENT',
        ],
      );
    });

    it('should throw error if database insert fails', async () => {
      mockQuery.mockRejectedValueOnce(new Error('DB write failed'));

      await expect(logNotification(mockEvent, 'SENT')).rejects.toThrow('DB write failed');
    });

    it('should log DLQ_MOVED status correctly', async () => {
      mockQuery.mockResolvedValueOnce({ rowCount: 1 });

      await logNotification(mockEvent, 'DLQ_MOVED');

      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('INSERT INTO notification_logs'),
        expect.arrayContaining(['DLQ_MOVED']),
      );
    });
  });
});
