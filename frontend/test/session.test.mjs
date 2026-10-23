import test from 'node:test';
import assert from 'node:assert/strict';
import * as client from '../src/api.mjs';

const unauthorized = () => Response.json({ detail: 'A valid bearer token is required' }, { status: 401 });

test('HTTP errors retain status so 401 is distinguishable from 403 and connection failure', async context => {
  context.mock.method(globalThis, 'fetch', async () => unauthorized());
  await assert.rejects(client.createApi('example').me(), error => error.status === 401);
  context.mock.method(globalThis, 'fetch', async () => Response.json({ detail: 'Read-only' }, { status: 403 }));
  await assert.rejects(client.createApi('example').remove('one'), error => error.status === 403);
  context.mock.method(globalThis, 'fetch', async () => { throw new TypeError('failed to fetch'); });
  await assert.rejects(client.createApi('example').me(), error => !error.status && /无法连接服务/.test(error.message));
});

test('concurrent polling and operation 401 responses expire the active session only once', async context => {
  assert.equal(typeof client.createSessionBoundary, 'function');
  const states = { identity: { tenant_id: 'one' }, runs: ['run-one'], retry: true };
  let expired = 0;
  const session = client.createSessionBoundary(() => {
    expired++;
    states.identity = null; states.runs = []; states.retry = false;
  });
  const api = session.api('example');
  context.mock.method(globalThis, 'fetch', async () => unauthorized());
  const outcomes = await Promise.allSettled([api.runs(), api.quota(), api.remove('one')]);
  assert.ok(outcomes.every(result => result.status === 'rejected' && result.reason.status === 401));
  assert.equal(expired, 1);
  assert.deepEqual(states, { identity: null, runs: [], retry: false });
});

test('an old session 401 cannot expire a newer accepted connection', async context => {
  assert.equal(typeof client.createSessionBoundary, 'function');
  let expired = 0;
  const session = client.createSessionBoundary(() => { expired++; });
  const oldApi = session.api('old-example');
  let deliverOldResponse;
  context.mock.method(globalThis, 'fetch', async (_path, options) => {
    if (options.headers.get('Authorization') === 'Bearer old-example') {
      return new Promise(resolve => { deliverOldResponse = resolve; });
    }
    return Response.json({ tenant_id: 'new-tenant', role: 'operator' });
  });
  const oldRequest = oldApi.runs();
  session.advance();
  const acceptedVersion = session.version;
  const newApi = session.api('new-example');
  assert.equal((await newApi.me()).tenant_id, 'new-tenant');
  deliverOldResponse(unauthorized());
  await assert.rejects(oldRequest, error => error.status === 401);
  assert.equal(expired, 0);
  assert.equal(session.version, acceptedVersion);
  assert.equal((await newApi.me()).tenant_id, 'new-tenant');
});

test('invalid connection candidate has no authority to expire the accepted session', async context => {
  assert.equal(typeof client.createSessionBoundary, 'function');
  let expired = 0;
  const session = client.createSessionBoundary(() => { expired++; });
  const api = session.api('accepted-example');
  context.mock.method(globalThis, 'fetch', async (_path, options) => options.headers.get('Authorization') === 'Bearer candidate-example'
    ? unauthorized() : Response.json({ tenant_id: 'accepted-tenant', role: 'operator' }));
  await assert.rejects(client.createApi('candidate-example').me(), error => error.status === 401);
  assert.equal(expired, 0);
  assert.equal((await api.me()).tenant_id, 'accepted-tenant');
});

test('only an authenticated 401 clears a retryable submission; network errors and 403 preserve it', async context => {
  assert.equal(typeof client.createSessionBoundary, 'function');
  const submission = client.createRunSubmission();
  let expired = 0;
  const session = client.createSessionBoundary(() => { expired++; submission.reset(); });
  const api = session.api('accepted-example');
  const keys = [];
  let attempt = 0;
  context.mock.method(globalThis, 'fetch', async (_path, options) => {
    keys.push(options.headers.get('Idempotency-Key'));
    attempt++;
    if (attempt === 1) throw new TypeError('failed to fetch');
    if (attempt === 2) return Response.json({ detail: 'Forbidden' }, { status: 403 });
    return unauthorized();
  });
  for (let index = 0; index < 3; index++) {
    await assert.rejects(submission.submit(api, session.version, client.SAMPLE_WORKFLOW));
    assert.equal(submission.pending, index < 2);
  }
  assert.equal(expired, 1);
  assert.equal(new Set(keys).size, 1, 'the body is retried unchanged until authentication invalidates the session');
});
