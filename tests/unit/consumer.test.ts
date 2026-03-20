// @ts-nocheck
import { processNotificationEvent } from '../../src/consumer/index';
import * as idempotency from '../../src/services/idempotency';
import * as notification from '../../src/services/notification';
import * as mq from '../../src/services/mq';
import { Channel, ConsumeMessage } from 'amqplib';
import { jest } from '@jest/globals';

jest.mock('../../src/services/idempotency');
jest.mock('../../src/services/notification', () => {
  const actual = jest.requireActual('../../src/services/notification');
  return {
    ...actual,
    dispatchNotification: jest.fn(),
    logNotification: jest.fn(),
  };
});
jest.mock('../../src/services/mq');
jest.mock('../../src/db/index');

describe('Consumer - processNotificationEvent', () => {
  const mockChannel = {
    ack: jest.fn(),
    nack: jest.fn(),
    sendToQueue: jest.fn(),
  } as unknown as Channel;

  const validEvent: notification.NotificationEvent = {
    event_id: '550e8400-e29b-41d4-a716-446655440000',
    type: 'email',
    recipient: 'user@example.com',
    payload: { subject: 'Welcome', body: 'Hello!' },
    timestamp: '2026-03-19T10:00:00Z',
  };

  function createMessage(
    event: any,
    retryCount = 0,
  ): ConsumeMessage {
    return {
      content: Buffer.from(JSON.stringify(event)),
      properties: {
        headers: { 'x-retry-count': retryCount },
      },
      fields: {},
    } as unknown as ConsumeMessage;
  }

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('should return immediately if message is null', async () => {
    await processNotificationEvent(null, mockChannel);
    expect(mockChannel.ack).not.toHaveBeenCalled();
  });

  it('should process a new event successfully (happy path)', async () => {
    (idempotency.checkAndMarkProcessing as jest.Mock).mockResolvedValue({
      alreadyProcessed: false,
      isProcessing: false,
    });
    (notification.dispatchNotification as jest.Mock).mockResolvedValue(undefined);
    (idempotency.updateEventStatus as jest.Mock).mockResolvedValue(undefined);
    (notification.logNotification as jest.Mock).mockResolvedValue(undefined);

    const msg = createMessage(validEvent);
    await processNotificationEvent(msg, mockChannel);

    expect(idempotency.checkAndMarkProcessing).toHaveBeenCalledWith(validEvent.event_id);
    expect(notification.dispatchNotification).toHaveBeenCalledWith(validEvent);
    expect(idempotency.updateEventStatus).toHaveBeenCalledWith(
      validEvent.event_id,
      idempotency.EventStatus.COMPLETED,
    );
    expect(notification.logNotification).toHaveBeenCalledWith(validEvent, 'SENT');
    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
  });

  it('should skip and ack already-processed events (idempotency)', async () => {
    (idempotency.checkAndMarkProcessing as jest.Mock).mockResolvedValue({
      alreadyProcessed: true,
      isProcessing: false,
    });

    const msg = createMessage(validEvent);
    await processNotificationEvent(msg, mockChannel);

    expect(notification.dispatchNotification).not.toHaveBeenCalled();
    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
  });

  it('should nack and requeue if event is being processed by another consumer (retryCount=0)', async () => {
    (idempotency.checkAndMarkProcessing as jest.Mock).mockResolvedValue({
      alreadyProcessed: false,
      isProcessing: true,
    });

    const msg = createMessage(validEvent, 0);
    await processNotificationEvent(msg, mockChannel);

    expect(notification.dispatchNotification).not.toHaveBeenCalled();
    expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, true);
  });

  it('should retry with exponential backoff on transient failure', async () => {
    (idempotency.checkAndMarkProcessing as jest.Mock).mockResolvedValue({
      alreadyProcessed: false,
      isProcessing: false,
    });
    (notification.dispatchNotification as jest.Mock).mockRejectedValue(
      new notification.TransientError('Transient failure'),
    );

    const msg = createMessage(validEvent, 0);
    await processNotificationEvent(msg, mockChannel);

    // Should ack original message and schedule retry
    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    expect(idempotency.updateEventStatus).not.toHaveBeenCalledWith(
      validEvent.event_id,
      idempotency.EventStatus.FAILED,
    );

    // Fast-forward timers to trigger the retry publication
    jest.runAllTimers();

    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(Buffer),
      expect.objectContaining({
        persistent: true,
        headers: { 'x-retry-count': 1 },
      }),
    );
  });

  it('should move to DLQ after max retries exhausted', async () => {
    (idempotency.checkAndMarkProcessing as jest.Mock).mockResolvedValue({
      alreadyProcessed: false,
      isProcessing: false,
    });
    (notification.dispatchNotification as jest.Mock).mockRejectedValue(
      new notification.TransientError('Persistent transient failure'),
    );
    (idempotency.updateEventStatus as jest.Mock).mockResolvedValue(undefined);
    (notification.logNotification as jest.Mock).mockResolvedValue(undefined);
    (mq.publishToDLQ as jest.Mock).mockResolvedValue(undefined);

    // Retry count = 3 (maxRetries), so retries exhausted
    const msg = createMessage(validEvent, 3);
    await processNotificationEvent(msg, mockChannel);

    expect(idempotency.updateEventStatus).toHaveBeenCalledWith(
      validEvent.event_id,
      idempotency.EventStatus.FAILED,
    );
    expect(notification.logNotification).toHaveBeenCalledWith(validEvent, 'DLQ_MOVED');
    expect(mq.publishToDLQ).toHaveBeenCalledWith(
      validEvent,
      'Persistent transient failure',
      3,
    );
    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
  });

  it('should move PermanentError directly to DLQ without retries', async () => {
    (idempotency.checkAndMarkProcessing as jest.Mock).mockResolvedValue({
      alreadyProcessed: false,
      isProcessing: false,
    });
    (notification.dispatchNotification as jest.Mock).mockRejectedValue(
      new notification.PermanentError('Invalid recipient format'),
    );
    (idempotency.updateEventStatus as jest.Mock).mockResolvedValue(undefined);
    (notification.logNotification as jest.Mock).mockResolvedValue(undefined);
    (mq.publishToDLQ as jest.Mock).mockResolvedValue(undefined);

    // Even at retryCount=0, permanent errors go to DLQ
    const msg = createMessage(validEvent, 0);
    await processNotificationEvent(msg, mockChannel);

    expect(idempotency.updateEventStatus).toHaveBeenCalledWith(
      validEvent.event_id,
      idempotency.EventStatus.FAILED,
    );
    expect(mq.publishToDLQ).toHaveBeenCalledWith(
      validEvent,
      'Invalid recipient format',
      0,
    );
    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
  });

  it('should dead-letter unparseable messages', async () => {
    const badMsg = {
      content: Buffer.from('not valid json!!!'),
      properties: { headers: {} },
      fields: {},
    } as unknown as ConsumeMessage;

    await processNotificationEvent(badMsg, mockChannel);

    expect(mockChannel.nack).toHaveBeenCalledWith(badMsg, false, false);
    expect(notification.dispatchNotification).not.toHaveBeenCalled();
  });
});
