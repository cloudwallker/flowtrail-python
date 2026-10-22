import test from 'node:test';
import assert from 'node:assert/strict';
import { createApi, parseSSE, errorMessage, parseWorkflow, SAMPLE_WORKFLOW } from '../src/api.mjs';

test('SSE supports CRLF, comments and multiple data lines', () => {
  const events = parseSSE(': heartbeat\r\n\r\nid: 7\r\nevent: NODE_SUCCEEDED\r\ndata: {"node_id":"prepare",\r\ndata: "data":{"output":"hello"}}\r\n\r\n');
  assert.deepEqual(events, [{ node_id: 'prepare', data: { output: 'hello' }, type: 'NODE_SUCCEEDED', seq: '7' }]);
});
test('SSE uses server payload IDs and does not invent heartbeat events', () => {
  assert.deepEqual(parseSSE(': heartbeat\n\n'), []);
  assert.deepEqual(parseSSE('id: 3\nevent: message\ndata: {"seq":4,"type":"RUN_SUCCEEDED"}\n\n'), [{ seq: 4, type: 'RUN_SUCCEEDED' }]);
});
test('malformed JSON event is visible as text instead of breaking replay', () => {
  assert.deepEqual(parseSSE('event: warning\ndata: plain text\n\n'), [{ data: 'plain text', type: 'warning', seq: '' }]);
  assert.deepEqual(parseSSE('data: null\n\n'), [{ data: null, type: 'message', seq: '' }]);
});
test('error messages describe validation location and redact supplied bearer value', () => {
  assert.equal(errorMessage({ detail: [{ loc: ['body', 'workflow'], msg: 'Field required' }] }), 'body.workflow: Field required');
  assert.equal(errorMessage({ detail: 'Invalid example-secret-token' }, '', 'example-secret-token'), 'Invalid [已隐藏令牌]');
});
test('workflow editor accepts sample and rejects malformed or missing workflow', () => {
  assert.deepEqual(parseWorkflow(JSON.stringify(SAMPLE_WORKFLOW)), SAMPLE_WORKFLOW);
  assert.throws(() => parseWorkflow('{bad'), /JSON 格式/);
  assert.throws(() => parseWorkflow('null'), /JSON 对象/);
  assert.throws(() => parseWorkflow('{}'), /workflow.name/);
  assert.throws(() => parseWorkflow('{"workflow":{"name":42}}'), /workflow.name/);
});

test('authenticated SSE replay keeps bearer token in headers, never in URL', async context => {
  let observed;
  context.mock.method(globalThis, 'fetch', async (path, options) => {
    observed = { path, options };
    return new Response('id: 1\ndata: {"type":"RUN_QUEUED"}\n\n');
  });
  const events = await createApi('test-only-placeholder').events('run/one');
  assert.equal(observed.path, '/api/runs/run%2Fone/events?follow=false');
  assert.equal(observed.options.headers.get('Authorization'), 'Bearer test-only-placeholder');
  assert.equal(events[0].type, 'RUN_QUEUED');
});

test('API handles empty delete responses and redacts token in backend errors', async context => {
  context.mock.method(globalThis, 'fetch', async () => new Response(null, { status: 204 }));
  assert.equal(await createApi('placeholder').remove('example.txt'), null);
  context.mock.method(globalThis, 'fetch', async () => new Response(JSON.stringify({ detail: 'invalid placeholder' }), { status: 401 }));
  await assert.rejects(createApi('placeholder').me(), /invalid \[已隐藏令牌\]/);
});
