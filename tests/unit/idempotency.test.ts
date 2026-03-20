// @ts-nocheck
import { checkAndMarkProcessing, updateEventStatus, EventStatus } from '../../src/services/idempotency';
import * as db from '../../src/db/index';
import { jest } from '@jest/globals';

jest.mock('../../src/db/index');

describe('Idempotency Service', () => {
  const mockQuery = db.query as jest.Mock;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('checkAndMarkProcessing', () => {
    it('should mark a new event as PROCESSING if it does not exist', async () => {
      mockQuery.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ status: EventStatus.PROCESSING }],
      });

      const result = await checkAndMarkProcessing('new-event-id');

      expect(result.alreadyProcessed).toBe(false);
      expect(result.isProcessing).toBe(false);
      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('INSERT INTO processed_events'),
        expect.arrayContaining(['new-event-id', EventStatus.PROCESSING]),
      );
    });

    it('should return alreadyProcessed=true if status is COMPLETED', async () => {
      // INSERT/UPDATE fails due to WHERE clause (event already COMPLETED)
      mockQuery.mockResolvedValueOnce({ rowCount: 0 });
      // Follow-up SELECT returns COMPLETED
      mockQuery.mockResolvedValueOnce({ rows: [{ status: EventStatus.COMPLETED }] });

      const result = await checkAndMarkProcessing('completed-event-id');

      expect(result.alreadyProcessed).toBe(true);
      expect(result.isProcessing).toBe(false);
      expect(mockQuery).toHaveBeenCalledTimes(2);
    });

    it('should return isProcessing=true if status is PROCESSING', async () => {
      // INSERT/UPDATE fails due to WHERE clause (event already PROCESSING)
      mockQuery.mockResolvedValueOnce({ rowCount: 0 });
      // Follow-up SELECT returns PROCESSING
      mockQuery.mockResolvedValueOnce({ rows: [{ status: EventStatus.PROCESSING }] });

      const result = await checkAndMarkProcessing('processing-event-id');

      expect(result.alreadyProcessed).toBe(false);
      expect(result.isProcessing).toBe(true);
      expect(mockQuery).toHaveBeenCalledTimes(2);
    });

    it('should throw error if database query fails', async () => {
      mockQuery.mockRejectedValueOnce(new Error('DB connection lost'));

      await expect(checkAndMarkProcessing('error-event-id')).rejects.toThrow('DB connection lost');
    });
  });

  describe('updateEventStatus', () => {
    it('should update event status to COMPLETED', async () => {
      mockQuery.mockResolvedValueOnce({ rowCount: 1 });

      await updateEventStatus('test-event-id', EventStatus.COMPLETED);

      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('UPDATE processed_events SET status'),
        [EventStatus.COMPLETED, 'test-event-id'],
      );
    });

    it('should update event status to FAILED', async () => {
      mockQuery.mockResolvedValueOnce({ rowCount: 1 });

      await updateEventStatus('test-event-id', EventStatus.FAILED);

      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('UPDATE processed_events SET status'),
        [EventStatus.FAILED, 'test-event-id'],
      );
    });
  });
});
