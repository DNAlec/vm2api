import assert from 'node:assert/strict'
import test from 'node:test'
import { createUsageLink } from '../../src/lib/gemini/usage-link.mjs'

test('usage links require negotiation and valid, distinct request IDs', () => {
  for (const headers of [{}, { 'x-vm2api-usage-link': '0' }]) {
    const link = createUsageLink({ headers }, { setHeader() { assert.fail('unnegotiated header') } }, 'child')
    assert.equal(link.complete({ local_tool_drain: true, tool_continuation_parent_request_id: 'parent' }, true), '')
  }
  const headers = {}
  const res = { setHeader(k, v) { headers[k] = v } }
  const link = createUsageLink({ headers: { 'x-vm2api-usage-link': '1' } }, res, 'child')
  assert.equal(headers['X-Vm2api-Request-Id'], 'child')
  for (const parent of ['', 'child', 'bad\r\nid', 'x'.repeat(129)]) {
    assert.equal(link.complete({ local_tool_drain: true, tool_continuation_parent_request_id: parent }, true), '')
  }
  assert.equal(link.complete({ tool_continuation_parent_request_id: 'parent' }, true), '')
  const turn = { local_tool_drain: true, tool_continuation_parent_request_id: 'parent' }
  assert.equal(link.complete(turn, true), ': vm2api-usage-link {"request_id":"child","parent_request_id":"parent","kind":"local_tool_drain"}\n\n')
  assert.equal(headers['X-Vm2api-Parent-Request-Id'], undefined)
  assert.equal(link.complete(turn, false), '')
  assert.equal(headers['X-Vm2api-Parent-Request-Id'], 'parent')
  assert.equal(headers['X-Vm2api-Continuation'], 'local_tool_drain')
})
