import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';
import { clamp } from '../src/lib/format.ts';

// Exercise the actual input handler with React's deferred-updater timing:
// currentTarget is cleared after dispatch, before the queued update is applied.
const source = ts.createSourceFile('SettingsPage.tsx', readFileSync(new URL('../src/features/settings/SettingsPage.tsx', import.meta.url), 'utf8'), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
let handlerSource;
function visit(node) {
  if (ts.isJsxSelfClosingElement(node)) {
    const attributes = node.attributes.properties;
    if (attributes.some((attr) => ts.isJsxAttribute(attr) && attr.name.getText(source) === 'id' && attr.initializer?.getText(source).includes('plan-concurrency-'))) {
      handlerSource = attributes.find((attr) => ts.isJsxAttribute(attr) && attr.name.getText(source) === 'onChange')?.initializer?.expression?.getText(source);
    }
  }
  ts.forEachChild(node, visit);
}
visit(source);
assert.ok(handlerSource, 'plan concurrency input must have an onChange handler');

function inputFixture() {
  let state = { free: 10, pro: 7 };
  const queued = [];
  const dirty = { current: false };
  const handler = runInNewContext(`(${handlerSource})`, {
    clamp, plan: { plan: 'free' }, concurrencyDirtyRef: dirty,
    setConcurrencyOverrides: (updater) => queued.push(updater),
  });
  return {
    change(value) {
      const event = { currentTarget: { value } };
      handler(event);
      event.currentTarget = null;
      while (queued.length) state = queued.shift()(state);
      assert.equal(state.pro, 7, 'editing one plan must preserve other plans');
      assert.equal(dirty.current, true);
      return state.free;
    },
  };
}

test('three increments survive React clearing the input event', () => {
  const input = inputFixture();
  for (const value of ['11', '12', '13']) assert.equal(input.change(value), Number(value));
});

test('deleting all digits preserves an empty draft that can be replaced', () => {
  const input = inputFixture();
  assert.equal(input.change(''), '');
  assert.equal(input.change('2'), 2);
  assert.equal(input.change('25'), 25);
});

test('concurrency changes remain integer values within the existing limits', () => {
  const input = inputFixture();
  for (const [value, expected] of [['0', 1], ['-4', 1], ['51', 50], ['2.5', 2], ['50', 50]]) {
    assert.equal(input.change(value), expected);
  }
});
