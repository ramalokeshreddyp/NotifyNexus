import { z } from 'zod';

// Replicate the schema from src/api/index.ts for isolated testing
const NotificationEventSchema = z.object({
  event_id: z.string().uuid(),
  type: z.enum(['email', 'sms', 'push']),
  recipient: z.string().trim().min(1),
  payload: z.record(z.string(), z.any()),
  timestamp: z.string().datetime(),
});

describe('NotificationEvent Validation Schema', () => {
  const validEvent = {
    event_id: '550e8400-e29b-41d4-a716-446655440000',
    type: 'email',
    recipient: 'user@example.com',
    payload: { subject: 'Welcome', body: 'Hello!' },
    timestamp: '2026-03-19T10:30:00Z',
  };

  it('should accept a valid notification event', () => {
    const result = NotificationEventSchema.safeParse(validEvent);
    expect(result.success).toBe(true);
  });

  it('should accept all valid notification types', () => {
    for (const type of ['email', 'sms', 'push']) {
      const result = NotificationEventSchema.safeParse({ ...validEvent, type });
      expect(result.success).toBe(true);
    }
  });

  it('should reject an invalid UUID for event_id', () => {
    const result = NotificationEventSchema.safeParse({
      ...validEvent,
      event_id: 'not-a-uuid',
    });
    expect(result.success).toBe(false);
  });

  it('should reject an invalid notification type', () => {
    const result = NotificationEventSchema.safeParse({
      ...validEvent,
      type: 'telegram',
    });
    expect(result.success).toBe(false);
  });

  it('should reject missing required fields', () => {
    const incompleteEvent = { event_id: validEvent.event_id };
    const result = NotificationEventSchema.safeParse(incompleteEvent);
    expect(result.success).toBe(false);
  });

  it('should reject an invalid timestamp format', () => {
    const result = NotificationEventSchema.safeParse({
      ...validEvent,
      timestamp: 'not-a-timestamp',
    });
    expect(result.success).toBe(false);
  });

  it('should reject an empty string for event_id', () => {
    const result = NotificationEventSchema.safeParse({
      ...validEvent,
      event_id: '',
    });
    expect(result.success).toBe(false);
  });

  it('should accept any payload structure', () => {
    const result = NotificationEventSchema.safeParse({
      ...validEvent,
      payload: { nested: { deeply: { value: 42 } } },
    });
    expect(result.success).toBe(true);
  });

  it('should reject null payload', () => {
    const result = NotificationEventSchema.safeParse({
      ...validEvent,
      payload: null,
    });
    expect(result.success).toBe(false);
  });
});
