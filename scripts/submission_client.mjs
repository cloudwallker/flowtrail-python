// Exercise the production frontend module against the local fault proxy.
import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { createApi, createRunSubmission } from '../frontend/src/api.mjs';

globalThis.crypto ??= webcrypto;

async function main() {
  const base = process.env.FLOWTRAIL_SMOKE_BASE;
  const tokenA = process.env.FLOWTRAIL_SMOKE_TOKEN_A;
  const tokenB = process.env.FLOWTRAIL_SMOKE_TOKEN_B;
  assert.ok(base?.startsWith('http://127.0.0.1:'));
  assert.ok(tokenA?.length >= 32 && tokenB?.length >= 32);
  const nativeFetch = globalThis.fetch;
  const submissions = [];
  globalThis.fetch = (path, options = {}) => {
    if (options.method === 'POST' && path === '/api/runs') {
      const headers = new Headers(options.headers);
      submissions.push({ key: headers.get('Idempotency-Key'), body: options.body });
    }
    return nativeFetch(new URL(path, base), options);
  };
  try {
    const apiA = createApi(tokenA);
    const apiB = createApi(tokenB);
    const intent = createRunSubmission();
    const body = name => ({ workflow: { name, nodes: [
      { id: 'text', type: 'text', depends_on: [], config: { template: '${inputs.message}' } },
    ] }, inputs: { message: 'local smoke fixture' } });
    const lost = body('smoke-response-loss');

    await assert.rejects(intent.submit(apiA, 'session-a', lost));
    assert.equal(intent.pending, true);
    assert.equal((await apiA.runs()).items.length, 1, 'The first request committed before its response was lost');
    const recovered = await intent.submit(apiA, 'session-a', lost);
    assert.equal(intent.pending, false);
    assert.equal(submissions[0].key, submissions[1].key);
    assert.equal(submissions[0].body, submissions[1].body);
    assert.ok(submissions[0].key);
    let runs = (await apiA.runs()).items;
    assert.equal(runs.length, 1);
    assert.equal(runs[0].id, recovered.id);

    const intentionalRepeat = await intent.submit(apiA, 'session-a', lost);
    assert.notEqual(intentionalRepeat.id, recovered.id);
    assert.notEqual(submissions[1].key, submissions[2].key);
    assert.equal((await apiA.runs()).items.length, 2);

    const changed = body('smoke-body-change');
    await assert.rejects(intent.submit(apiA, 'session-a', changed));
    changed.inputs.message = 'changed after the response was lost';
    await intent.submit(apiA, 'session-a', changed);
    assert.notEqual(submissions[3].key, submissions[4].key);
    assert.notEqual(submissions[3].body, submissions[4].body);
    assert.equal((await apiA.runs()).items.length, 4);

    const identity = body('smoke-identity-change');
    await assert.rejects(intent.submit(apiA, 'session-a', identity));
    const otherTenant = await intent.submit(apiB, 'session-b', identity);
    assert.notEqual(submissions[5].key, submissions[6].key);
    assert.equal(submissions[5].body, submissions[6].body);
    assert.equal((await apiA.runs()).items.length, 5);
    runs = (await apiB.runs()).items;
    assert.equal(runs.length, 1);
    assert.equal(runs[0].id, otherTenant.id);
    assert.equal(intent.pending, false);

    console.log(JSON.stringify({ ok: true, submitted_requests: submissions.length,
      lost_responses: 3, first_intent_attempts: 2, first_intent_runs: 1,
      runs_after_successful_resubmission: 2, final_tenant_a_runs: 5, final_tenant_b_runs: 1,
      changed_body_rotates_key: true, changed_identity_rotates_key: true }));
  } finally {
    globalThis.fetch = nativeFetch;
  }
}

main().catch(error => {
  // The API's friendly error is not evidence; the Python driver verifies the
  // proxy actually committed the request and disconnected its response socket.
  console.error(`Submission client failed (${error.name ?? 'Error'})`);
  process.exitCode = 1;
});
