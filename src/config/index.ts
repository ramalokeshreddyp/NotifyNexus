import dotenv from 'dotenv';
dotenv.config();

export const config = {
  db: {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
    database: process.env.DB_NAME || 'notifications_db',
  },
  mq: {
    host: process.env.MQ_HOST || 'localhost',
    port: parseInt(process.env.MQ_PORT || '5672'),
    user: process.env.MQ_USER || 'guest',
    pass: process.env.MQ_PASS || 'guest',
    queueName: process.env.MQ_QUEUE_NAME || 'notification_events',
    dlqName: process.env.MQ_DLQ_NAME || 'notification_dead_letter_queue',
  },
  app: {
    port: parseInt(process.env.PORT || '3000'),
    maxRetries: parseInt(process.env.MAX_RETRIES || '3'),
    retryInitialDelay: parseInt(process.env.RETRY_INITIAL_DELAY || '1000'),
  },
};
