import express from 'express';
import { config } from './src/config';
import { logger } from './src/utils/logger';
import { connectMQ, consumeEvents, getChannel, closeMQ, beginGracefulDrain } from './src/services/mq';
import { processNotificationEvent } from './src/consumer/index';
import apiRouter from './src/api/index';
import { pool } from './src/db/index';

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
    // Connect to Message Queue
    await connectMQ();

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
    logger.error('Failed to start server', { error: String(error) });
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
