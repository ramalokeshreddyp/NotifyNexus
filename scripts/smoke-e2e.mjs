import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';

const PROJECT_ROOT = process.cwd();
const SERVICE_URL = process.env.SMOKE_BASE_URL || 'http://localhost:3000';
const KEEP_UP = process.env.SMOKE_KEEP_UP === '1';
const TIMEOUT_MS = Number(process.env.SMOKE_TIMEOUT_MS || 180000);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: PROJECT_ROOT,
      shell: process.platform === 'win32',
      stdio: ['ignore', 'pipe', 'pipe'],
      ...options,
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('close', (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        reject(new Error(`${command} ${args.join(' ')} failed (${code})\n${stderr || stdout}`));
      }
    });
  });
}

async function detectComposeCommand() {
  try {
    await runCommand('docker', ['compose', 'version']);
    return { command: 'docker', baseArgs: ['compose'] };
  } catch {
    await runCommand('docker-compose', ['version']);
    return { command: 'docker-compose', baseArgs: [] };
  }
}

async function compose(composeBin, args) {
  return runCommand(composeBin.command, [...composeBin.baseArgs, ...args]);
}

async function waitForHealth(url, timeoutMs) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${url}/api/health`);
      if (res.ok) {
        const body = await res.json();
        if (body?.status === 'ok') {
          return;
        }
      }
    } catch {
      // Service not ready yet.
    }
    await sleep(3000);
  }
  throw new Error(`Timed out waiting for health endpoint: ${url}/api/health`);
}

async function publishEvent(event) {
  const res = await fetch(`${SERVICE_URL}/api/v1/publish-notification-event`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(event),
  });

  const body = await res.json().catch(() => ({}));
  if (res.status !== 202) {
    throw new Error(`Publish failed (${res.status}): ${JSON.stringify(body)}`);
  }
}

async function queryScalar(composeBin, sql) {
  const { stdout } = await compose(composeBin, [
    'exec',
    '-T',
    'postgres_db',
    'psql',
    '-U',
    'postgres',
    '-d',
    'notifications_db',
    '-t',
    '-A',
    '-c',
    sql,
  ]);

  const lines = stdout
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .filter((line) => !line.startsWith('WARNING:'))
    .filter((line) => !line.startsWith('could not change directory'))
    .filter((line) => !/^\(\d+ rows?\)$/.test(line));

  return lines.length > 0 ? lines[lines.length - 1] : '';
}

async function waitForCondition(description, fn, timeoutMs) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      if (await fn()) {
        return;
      }
    } catch {
      // Dependencies may still be warming up.
    }
    await sleep(2500);
  }
  throw new Error(`Timed out waiting for condition: ${description}`);
}

async function getDlqMessageCount(composeBin) {
  const { stdout } = await compose(composeBin, [
    'exec',
    '-T',
    'rabbitmq',
    'rabbitmqctl',
    'list_queues',
    'name',
    'messages',
  ]);

  const lines = stdout
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  for (const line of lines) {
    const parts = line.split(/\s+/);
    if (parts[0] === 'notification_dead_letter_queue') {
      return Number(parts[1] || 0);
    }
  }

  return 0;
}

async function main() {
  const composeBin = await detectComposeCommand();

  const successEventId = randomUUID();
  const failEventId = randomUUID();

  const successEvent = {
    event_id: successEventId,
    type: 'email',
    recipient: 'evaluator-success@example.com',
    payload: { subject: 'Smoke Success', body: 'Expected success path' },
    timestamp: new Date().toISOString(),
  };

  const failEvent = {
    event_id: failEventId,
    type: 'email',
    recipient: 'evaluator-failure@example.com',
    payload: { subject: 'Smoke Failure', force_fail: true },
    timestamp: new Date().toISOString(),
  };

  try {
    console.log('[smoke] Starting services with Docker Compose...');
    await compose(composeBin, ['up', '-d', '--build']);

    console.log('[smoke] Waiting for API health...');
    await waitForHealth(SERVICE_URL, TIMEOUT_MS);

    console.log('[smoke] Publishing success event...');
    await publishEvent(successEvent);

    console.log('[smoke] Publishing forced-failure event...');
    await publishEvent(failEvent);

    console.log('[smoke] Waiting for success event completion...');
    await waitForCondition(
      'success event COMPLETED',
      async () => {
        const status = await queryScalar(
          composeBin,
          `SELECT status FROM processed_events WHERE event_id = '${successEventId}';`,
        );
        return status.includes('COMPLETED');
      },
      TIMEOUT_MS,
    );

    console.log('[smoke] Waiting for failed event DLQ movement...');
    await waitForCondition(
      'failed event status FAILED',
      async () => {
        const status = await queryScalar(
          composeBin,
          `SELECT status FROM processed_events WHERE event_id = '${failEventId}';`,
        );
        return status.includes('FAILED');
      },
      TIMEOUT_MS,
    );

    await waitForCondition(
      'failed event log DLQ_MOVED',
      async () => {
        const count = await queryScalar(
          composeBin,
          `SELECT COUNT(*) FROM notification_logs WHERE event_id = '${failEventId}' AND status = 'DLQ_MOVED';`,
        );
        return Number(count) > 0;
      },
      TIMEOUT_MS,
    );

    await waitForCondition(
      'DLQ contains at least one message',
      async () => {
        const dlqCount = await getDlqMessageCount(composeBin);
        return dlqCount > 0;
      },
      TIMEOUT_MS,
    );

    console.log('[smoke] PASS');
    console.log(`[smoke] Success event id: ${successEventId}`);
    console.log(`[smoke] Failed event id:  ${failEventId}`);
  } finally {
    if (!KEEP_UP) {
      console.log('[smoke] Stopping services...');
      await compose(composeBin, ['down', '-v']).catch(() => {
        // Best effort cleanup.
      });
    } else {
      console.log('[smoke] Keeping services up because SMOKE_KEEP_UP=1');
    }
  }
}

main().catch((error) => {
  console.error('[smoke] FAIL');
  console.error(error.message || String(error));
  process.exit(1);
});
