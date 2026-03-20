import { Pool } from 'pg';
import { config } from '../config/index';
import { logger } from '../utils/logger';

export const pool = new Pool({
  host: config.db.host,
  port: config.db.port,
  user: config.db.user,
  password: config.db.password,
  database: config.db.database,
});

pool.on('error', (err) => {
  logger.error('Unexpected error on idle client', err);
});

export const query = (text: string, params?: any[]) => pool.query(text, params);

export async function initializeDatabaseSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id VARCHAR(255) PRIMARY KEY,
      status VARCHAR(50) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS notification_logs (
      log_id SERIAL PRIMARY KEY,
      event_id VARCHAR(255) NOT NULL,
      recipient VARCHAR(255) NOT NULL,
      type VARCHAR(50) NOT NULL,
      message_payload JSONB,
      status VARCHAR(50) NOT NULL,
      processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (event_id) REFERENCES processed_events(event_id)
    );
  `);

  await pool.query(`
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = CURRENT_TIMESTAMP;
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `);

  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'update_processed_events_updated_at'
      ) THEN
        CREATE TRIGGER update_processed_events_updated_at
        BEFORE UPDATE ON processed_events
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
      END IF;
    END
    $$;
  `);

  logger.info('Database schema initialized');
}
