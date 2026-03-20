import express from 'express';
import { z } from 'zod';
import { publishEvent } from '../services/mq';
import { logger } from '../utils/logger';

const router = express.Router();

const NotificationEventSchema = z.object({
  event_id: z.string().uuid(),
  type: z.enum(['email', 'sms', 'push']),
  recipient: z.string().trim().min(1),
  payload: z.record(z.string(), z.any()),
  timestamp: z.string().datetime(),
});

router.post('/publish-notification-event', async (req, res) => {
  try {
    const event = NotificationEventSchema.parse(req.body);
    
    await publishEvent(event);
    
    res.status(202).json({
      message: 'Event successfully published to MQ',
      eventId: event.event_id
    });
  } catch (error) {
    if (error instanceof z.ZodError) {
      return res.status(400).json({
        error: 'Invalid NotificationEvent payload',
        details: error.issues
      });
    }
    
    logger.error('Error publishing event', { error: String(error) });
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

export default router;
