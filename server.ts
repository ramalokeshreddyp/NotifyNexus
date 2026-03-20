import express from 'express';
import { config } from './src/config';
import { logger } from './src/utils/logger';
import { connectMQ, consumeEvents, getChannel, closeMQ, beginGracefulDrain } from './src/services/mq';
import { processNotificationEvent } from './src/consumer/index';
import apiRouter from './src/api/index';
import { initializeDatabaseSchema, pool } from './src/db/index';

async function retryOperation(
  operationName: string,
  operation: () => Promise<void>,
  attempts: number,
  delayMs: number,
) {
  let lastError: unknown;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      await operation();
      logger.info(`${operationName} succeeded`, { attempt, attempts });
      return;
    } catch (error) {
      lastError = error;
      logger.warn(`${operationName} failed`, {
        attempt,
        attempts,
        error: String(error),
      });

      if (attempt < attempts) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
  }

  throw lastError;
}

async function startServer() {
  const app = express();
  app.use(express.json());

  // Health check endpoint
  app.get('/api/health', async (_req, res) => {
    try {
      await pool.query('SELECT 1');
      const mqConnected = getChannel() !== null;
      res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        services: {
          database: 'connected',
          messageQueue: mqConnected ? 'connected' : 'disconnected',
        },
      });
    } catch (error) {
      res.status(500).json({
        status: 'error',
        error: String(error),
      });
    }
  });

  app.get('/health', async (_req, res) => {
    try {
      await pool.query('SELECT 1');
      const mqConnected = getChannel() !== null;
      res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        services: {
          database: 'connected',
          messageQueue: mqConnected ? 'connected' : 'disconnected',
        },
      });
    } catch (error) {
      res.status(500).json({
        status: 'error',
        error: String(error),
      });
    }
  });

  // API Router
  app.use('/api/v1', apiRouter);

  try {
    await retryOperation(
      'Database schema initialization',
      async () => {
        // Ensure schema exists in environments without init scripts.
        await initializeDatabaseSchema();
      },
      config.app.startupRetryAttempts,
      config.app.startupRetryDelayMs,
    );

    await retryOperation(
      'RabbitMQ connection',
      async () => {
        await connectMQ();
      },
      config.app.startupRetryAttempts,
      config.app.startupRetryDelayMs,
    );

    // Start consuming events — channel is passed to consumer via callback
    await consumeEvents(processNotificationEvent);

    app.listen(config.app.port, '0.0.0.0', () => {
      logger.info(`🚀 Notification Service running on http://localhost:${config.app.port}`, {
        port: config.app.port,
        environment: process.env.NODE_ENV || 'development',
        maxRetries: config.app.maxRetries,
        retryInitialDelay: config.app.retryInitialDelay,
      });
    });
  } catch (error) {
    logger.error('Failed to start server', {
      error: String(error),
      startupRetryAttempts: config.app.startupRetryAttempts,
      startupRetryDelayMs: config.app.startupRetryDelayMs,
    });
    process.exit(1);
  }

  // Graceful shutdown handlers
  const shutdown = async (signal: string) => {
    logger.info(`Received ${signal}. Shutting down gracefully...`);

    // Stop accepting new deliveries and wait for active handlers to finish.
    await beginGracefulDrain(10000);

    await closeMQ();
    await pool.end();
    logger.info('All connections closed. Exiting.');
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

startServer();
