import { connect, ChannelModel, Channel, ConsumeMessage } from 'amqplib';
import { config } from '../config/index';
import { logger } from '../utils/logger';

let connection: ChannelModel | null = null;
let channel: Channel | null = null;
let consumerTag: string | null = null;
let inFlightMessages = 0;
let isShuttingDown = false;

export async function connectMQ(): Promise<void> {
  const url = `amqp://${config.mq.user}:${config.mq.pass}@${config.mq.host}:${config.mq.port}`;
  try {
    const conn = await connect(url);
    connection = conn;
    const ch = await conn.createChannel();
    channel = ch;

    // Setup DLQ first (no special args needed)
    await ch.assertQueue(config.mq.dlqName, { durable: true });

    // Setup main queue with DLQ routing for nack'd messages
    await ch.assertQueue(config.mq.queueName, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': config.mq.dlqName,
      },
    });

    // Retry queue uses per-message TTL and routes back to main queue when expired.
    await ch.assertQueue(`${config.mq.queueName}.retry`, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': config.mq.queueName,
      },
    });

    // Process one message at a time
    await ch.prefetch(1);

    logger.info('Connected to RabbitMQ', {
      host: config.mq.host,
      queue: config.mq.queueName,
      retryQueue: `${config.mq.queueName}.retry`,
      dlq: config.mq.dlqName,
    });
  } catch (error) {
    logger.error('Error connecting to RabbitMQ', { error: String(error) });
    throw error;
  }
}

export async function publishEvent(event: any): Promise<void> {
  if (!channel) throw new Error('MQ channel not initialized');

  const msg = JSON.stringify(event);
  channel.sendToQueue(config.mq.queueName, Buffer.from(msg), {
    persistent: true,
    headers: { 'x-retry-count': 0 },
  });
  logger.info('Event published to queue', { eventId: event.event_id });
}

export async function publishToDLQ(event: any, error: string, retryCount: number): Promise<void> {
  if (!channel) throw new Error('MQ channel not initialized');

  const dlqMessage = {
    originalEvent: event,
    error,
    retryCount,
    failedAt: new Date().toISOString(),
  };

  channel.sendToQueue(config.mq.dlqName, Buffer.from(JSON.stringify(dlqMessage)), {
    persistent: true,
  });

  logger.info('Event published to DLQ', {
    eventId: event.event_id,
    error,
    retryCount,
  });
}

export async function consumeEvents(
  processor: (msg: ConsumeMessage | null, ch: Channel) => Promise<void>,
): Promise<void> {
  if (!channel) throw new Error('MQ channel not initialized');

  const ch = channel;
  const consumed = await channel.consume(
    config.mq.queueName,
    async (msg) => {
      if (!msg) return;

      // During shutdown, stop taking new work and requeue any delivered messages.
      if (isShuttingDown) {
        ch.nack(msg, false, true);
        return;
      }

      inFlightMessages += 1;
      try {
        await processor(msg, ch);
      } finally {
        inFlightMessages = Math.max(0, inFlightMessages - 1);
      }
    },
    { noAck: false },
  );
  consumerTag = consumed.consumerTag;
  logger.info('Started consuming events from queue', { queue: config.mq.queueName });
}

export async function beginGracefulDrain(timeoutMs = 10000): Promise<void> {
  if (!channel) return;

  isShuttingDown = true;

  if (consumerTag) {
    try {
      await channel.cancel(consumerTag);
      logger.info('Consumer cancelled for graceful shutdown', { consumerTag });
    } catch (error) {
      logger.warn('Failed to cancel consumer during shutdown', {
        error: String(error),
        consumerTag,
      });
    } finally {
      consumerTag = null;
    }
  }

  const started = Date.now();
  while (inFlightMessages > 0 && Date.now() - started < timeoutMs) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  if (inFlightMessages > 0) {
    logger.warn('Graceful drain timeout reached with in-flight messages', {
      inFlightMessages,
      timeoutMs,
    });
  } else {
    logger.info('In-flight message drain completed');
  }
}

export function getChannel(): Channel | null {
  return channel;
}

export async function closeMQ(): Promise<void> {
  try {
    if (channel) {
      await channel.close();
      channel = null;
    }
    if (connection) {
      await connection.close();
      connection = null;
    }
    consumerTag = null;
    inFlightMessages = 0;
    isShuttingDown = false;
    logger.info('RabbitMQ connection closed');
  } catch (error) {
    logger.error('Error closing RabbitMQ connection', { error: String(error) });
  }
}
