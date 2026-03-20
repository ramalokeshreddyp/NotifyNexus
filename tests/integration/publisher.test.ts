// @ts-nocheck
import request from 'supertest';
import express from 'express';
import apiRouter from '../../src/api/index';
import * as mq from '../../src/services/mq';
import { jest } from '@jest/globals';

jest.mock('../../src/services/mq');

const app = express();
app.use(express.json());
app.use('/api/v1', apiRouter);

describe('Integration Tests: Publisher API', () => {
  const mockPublishEvent = mq.publishEvent as jest.Mock;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const validEvent = {
    event_id: '550e8400-e29b-41d4-a716-446655440000',
    type: 'email',
    recipient: 'user@example.com',
    payload: { subject: 'Welcome', body: 'Hello!' },
    timestamp: '2026-03-19T10:30:00Z',
  };

  describe('POST /api/v1/publish-notification-event', () => {
    it('should return 202 and publish a valid notification event', async () => {
      mockPublishEvent.mockResolvedValueOnce(undefined);

      const response = await request(app)
        .post('/api/v1/publish-notification-event')
        .send(validEvent);

      expect(response.status).toBe(202);
      expect(response.body.message).toBe('Event successfully published to MQ');
      expect(response.body.eventId).toBe(validEvent.event_id);
      expect(mockPublishEvent).toHaveBeenCalledWith(expect.objectContaining({
        event_id: validEvent.event_id,
        type: validEvent.type,
        recipient: validEvent.recipient,
      }));
    });

    it('should return 400 for invalid UUID in event_id', async () => {
      const response = await request(app)
        .post('/api/v1/publish-notification-event')
        .send({ ...validEvent, event_id: 'not-a-uuid' });

      expect(response.status).toBe(400);
      expect(response.body.error).toBe('Invalid NotificationEvent payload');
    });

    it('should return 400 for invalid notification type', async () => {
      const response = await request(app)
        .post('/api/v1/publish-notification-event')
        .send({ ...validEvent, type: 'telegram' });

      expect(response.status).toBe(400);
      expect(response.body.error).toBe('Invalid NotificationEvent payload');
    });

    it('should return 400 when required fields are missing', async () => {
      const response = await request(app)
        .post('/api/v1/publish-notification-event')
        .send({ event_id: validEvent.event_id });

      expect(response.status).toBe(400);
      expect(response.body.error).toBe('Invalid NotificationEvent payload');
      expect(response.body.details).toBeDefined();
    });

    it('should return 400 for empty request body', async () => {
      const response = await request(app)
        .post('/api/v1/publish-notification-event')
        .send({});

      expect(response.status).toBe(400);
    });

    it('should return 400 for invalid timestamp format', async () => {
      const response = await request(app)
        .post('/api/v1/publish-notification-event')
        .send({ ...validEvent, timestamp: 'yesterday' });

      expect(response.status).toBe(400);
    });

    it('should return 500 when MQ publish fails', async () => {
      mockPublishEvent.mockRejectedValueOnce(new Error('MQ connection lost'));

      const response = await request(app)
        .post('/api/v1/publish-notification-event')
        .send(validEvent);

      expect(response.status).toBe(500);
      expect(response.body.error).toBe('Internal Server Error');
    });

    it('should accept all valid notification types (email, sms, push)', async () => {
      for (const type of ['email', 'sms', 'push']) {
        mockPublishEvent.mockResolvedValueOnce(undefined);

        const response = await request(app)
          .post('/api/v1/publish-notification-event')
          .send({ ...validEvent, type });

        expect(response.status).toBe(202);
      }
      expect(mockPublishEvent).toHaveBeenCalledTimes(3);
    });

    it('should accept events with force_fail payload flag for testing', async () => {
      mockPublishEvent.mockResolvedValueOnce(undefined);

      const testEvent = {
        ...validEvent,
        payload: { subject: 'Test', force_fail: true },
      };

      const response = await request(app)
        .post('/api/v1/publish-notification-event')
        .send(testEvent);

      expect(response.status).toBe(202);
    });
  });
});
