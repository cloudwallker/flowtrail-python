import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import * as client from '../src/api.mjs';

const body = () => structuredClone(client.SAMPLE_WORKFLOW);

test('run API sends an idempotency key in the header without changing the body', async context => {
  let observed;
  context.mock.method(globalThis, 'fetch', async (_path, options) => {
    observed = options;
    return Response.json({ id: 'run-one' });
  });
  const payload = body();
  await client.createApi('test-token').createRun(payload, 'intent-one');
  assert.equal(observed.headers.get('Idempotency-Key'), 'intent-one');
  assert.deepEqual(JSON.parse(observed.body), payload);
  assert.equal(Object.hasOwn(payload, 'idempotency_key'), false);
});

test('a committed run with a lost response retries once with identical key and body', async context => {
  assert.equal(typeof client.createRunSubmission, 'function');
  const submissions = [];
  const committed = new Map();
  let loseResponse = true;
  const server = http.createServer(async (request, response) => {
    let raw = '';
    for await (const chunk of request) raw += chunk;
    const key = request.headers['idempotency-key'];
    submissions.push({ key, raw, auth: request.headers.authorization });
    if (!key) { response.writeHead(422).end(); return; }
    if (!committed.has(key)) committed.set(key, { id: `run-${committed.size + 1}`, raw });
    const run = committed.get(key);
    if (run.raw !== raw) { response.writeHead(409).end(); return; }
    if (loseResponse) { loseResponse = false; request.socket.destroy(); return; }
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify({ id: run.id }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  context.after(() => new Promise(resolve => server.close(resolve)));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const nativeFetch = globalThis.fetch;
  context.mock.method(globalThis, 'fetch', (path, options) => nativeFetch(new URL(path, origin), options));
  const api = client.createApi('test-token');
  const submission = client.createRunSubmission();
  await assert.rejects(submission.submit(api, 'tenant-a/session-1', body()), /无法连接服务/);
  assert.equal(submission.pending, true);
  const result = await submission.submit(api, 'tenant-a/session-1', body());
  assert.equal(result.id, 'run-1');
  assert.equal(committed.size, 1);
  assert.equal(submissions.length, 2);
  assert.equal(submissions[0].key, submissions[1].key);
  assert.equal(submissions[0].raw, submissions[1].raw);
  assert.equal(submissions[0].auth, 'Bearer test-token');
  assert.equal(submission.pending, false);
  await submission.submit(api, 'tenant-a/session-1', body());
  assert.equal(committed.size, 2, 'a deliberate submission after success creates a new run');
  assert.notEqual(submissions[2].key, submissions[0].key);
});

test('editing payload, switching identity and choosing a new run never reuse an old key', async () => {
  assert.equal(typeof client.createRunSubmission, 'function');
  const observed = [];
  const api = { createRun: async (payload, key) => {
    observed.push({ payload, key });
    throw new Error('simulated response loss');
  } };
  const submission = client.createRunSubmission();
  const original = body();
  await assert.rejects(submission.submit(api, 'tenant-a/session-1', original));
  original.inputs.topic = 'edited';
  assert.notEqual(observed[0].payload.inputs.topic, 'edited', 'the attempted body is an immutable snapshot');
  await assert.rejects(submission.submit(api, 'tenant-a/session-1', original));
  assert.notEqual(observed[0].key, observed[1].key, 'changed inputs require a new intent');
  await assert.rejects(submission.submit(api, 'tenant-b/session-2', original));
  assert.notEqual(observed[1].key, observed[2].key, 'an identity switch requires a new intent');
  await assert.rejects(submission.submit(api, 'tenant-b/session-2', original));
  assert.equal(observed[2].key, observed[3].key, 'the unchanged failed intent remains retryable');
  submission.reset();
  assert.equal(submission.pending, false);
  await assert.rejects(submission.submit(api, 'tenant-b/session-2', original));
  assert.notEqual(observed[3].key, observed[4].key, 'explicit new run abandons the prior intent');
  assert.equal(Object.hasOwn(observed[4].payload, 'idempotency_key'), false);
});

test('a request timeout also retains the original intent for retry', async context => {
  const calls = [];
  context.mock.method(globalThis, 'fetch', async (_path, options) => {
    calls.push(options);
    if (calls.length === 1) throw new DOMException('request timed out', 'TimeoutError');
    return Response.json({ id: 'run-after-timeout' });
  });
  const submission = client.createRunSubmission();
  const api = client.createApi('test-token');
  await assert.rejects(submission.submit(api, 'tenant-a/session-1', body()), /请求超时/);
  assert.equal(submission.pending, true);
  assert.equal((await submission.submit(api, 'tenant-a/session-1', body())).id, 'run-after-timeout');
  assert.equal(calls[0].headers.get('Idempotency-Key'), calls[1].headers.get('Idempotency-Key'));
  assert.equal(calls[0].body, calls[1].body);
});

test('a delayed success from a cleared session cannot clear another pending submission', async () => {
  assert.equal(typeof client.createRunSubmission, 'function');
  const submission = client.createRunSubmission();
  let finish;
  const oldRequest = submission.submit({ createRun: () => new Promise(resolve => { finish = resolve; }) }, 'old-session', body());
  submission.reset();
  await assert.rejects(submission.submit({ createRun: async () => { throw new Error('lost'); } }, 'new-session', body()));
  finish({ id: 'old-run' });
  await oldRequest;
  assert.equal(submission.pending, true);
});
