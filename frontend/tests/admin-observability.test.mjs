import assert from 'node:assert/strict';
import { test } from 'node:test';
import { api } from '../src/lib/api.ts';

test('administrator list requests share a page ID without altering payload', async () => {
  const original = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (path, init) => {
    calls.push({ path, headers: new Headers(init.headers) });
    return new Response(JSON.stringify({ items: [], pagination: { total: 0 }, usage_included: true }), { status: 200 });
  };
  try {
    const params = new URLSearchParams({ limit: '100', hours: '24', q: 'private-search' });
    const results = await Promise.all([api.adminUsers(params, 'page-fixture'), api.adminPoolSummaryByUser(params, 'page-fixture')]);
    assert.equal(calls.length, 2);
    for (const call of calls) assert.equal(call.headers.get('X-OAIX-Page-Load-ID'), 'page-fixture');
    for (const payload of results) assert.deepEqual(payload.items, []);
    assert.equal(params.get('q'), 'private-search');
  } finally { globalThis.fetch = original; }
});

test('API error carries only a validated response request ID', async () => {
  const original = globalThis.fetch;
  try {
    for (const [header, expected] of [['req-fixture', 'req-fixture'], ['bad id', undefined]]) {
      globalThis.fetch = async () => new Response(JSON.stringify({ detail: 'cost query timeout' }), { status: 503, headers: { 'X-Request-ID': header } });
      await assert.rejects(api.adminUsers(), error => {
        assert.equal(error.message, 'cost query timeout');
        assert.equal(error.status, 503);
        assert.equal(error.requestId, expected);
        return true;
      });
    }
  } finally { globalThis.fetch = original; }
});
