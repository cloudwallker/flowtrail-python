import test from 'node:test';
import assert from 'node:assert/strict';
import { createApi, createSessionBoundary } from '../src/api.mjs';

const firstEvent = 'id: 1\nevent: RUN_STARTED\ndata: {"seq":1,"type":"RUN_STARTED"}\n\n';

test('an authentication error after valid SSE events rejects the partial replay and expires its session', async context => {
  let expired = 0;
  const session = createSessionBoundary(() => { expired++; });
  const api = session.api('test-token');
  context.mock.method(globalThis, 'fetch', async () => new Response(firstEvent
    + 'event: error\ndata: {"code":"authentication_invalid","message":"do not echo upstream details"}\n\n'));
  await assert.rejects(api.events('run-one'), error => error.status === 401
    && error.message === '当前身份已失效，请重新连接平台。');
  assert.equal(expired, 1);
});

test('other stream errors reject the replay without expiring a valid session or exposing server detail', async context => {
  let expired = 0;
  const api = createSessionBoundary(() => { expired++; }).api('test-token');
  context.mock.method(globalThis, 'fetch', async () => new Response(firstEvent
    + 'event: error\ndata: {"type":"NODE_FAILED","code":"event_read_failed","message":"private server details"}\n\n'));
  await assert.rejects(api.events('run-one'), error => error.status !== 401
    && /读取运行事件失败/.test(error.message) && !error.message.includes('private server details'));
  assert.equal(expired, 0);
});

test('a node failure is a legitimate execution event rather than a stream transport error', async context => {
  context.mock.method(globalThis, 'fetch', async () => new Response(firstEvent
    + 'id: 2\nevent: NODE_FAILED\ndata: {"seq":2,"type":"NODE_FAILED","data":{"error":"Node deadline exceeded"}}\n\n'));
  const events = await createApi('test-token').events('run-one');
  assert.equal(events.length, 2);
  assert.equal(events[1].type, 'NODE_FAILED');
  assert.equal(events[1].data.error, 'Node deadline exceeded');
});

test('a late SSE authentication error cannot expire a newly connected session', async context => {
  let expired = 0;
  const session = createSessionBoundary(() => { expired++; });
  const api = session.api('old-test-token');
  let deliver;
  context.mock.method(globalThis, 'fetch', async () => new Promise(resolve => { deliver = resolve; }));
  const request = api.events('old-run');
  session.advance();
  const current = session.version;
  deliver(new Response(firstEvent + 'event: error\ndata: {"code":"authentication_invalid"}\n\n'));
  await assert.rejects(request, error => error.status === 401);
  assert.equal(expired, 0);
  assert.equal(session.version, current);
});
