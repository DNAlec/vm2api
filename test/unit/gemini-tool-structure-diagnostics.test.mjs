import assert from 'node:assert/strict'
import fs from 'node:fs'
import vm from 'node:vm'
import crypto from 'node:crypto'
import { Readable } from 'node:stream'
import test from 'node:test'

// Exercise the actual decoder → restore → collector → validator without loading
// the unrelated DB/host dependency graph. Full route coverage lives in
// gemini-error-tool-validation.test.mjs; this harness is not a DB integration test.
const root = new URL('../../', import.meta.url)
const load = (file, baseline) => fs.readFileSync(baseline ? new URL(file, `file://${process.env.GEMINI_DIAG_BASELINE_DIR}/`) : new URL(file, root), 'utf8')
function functions(source) {
  const lines = source.split('\n')
  const out = []
  for (let i = 0; i < lines.length; i++) {
    if (!/^(export )?(async )?function /.test(lines[i])) continue
    const start = i
    while (lines[i] !== '}') i++
    out.push(lines.slice(start, i + 1).join('\n').replace(/^export /, ''))
  }
  return out.join('\n')
}
function harness(baseline = false) {
  const converter = load('src/lib/protocol/gemini-convert.mjs', baseline)
  const scheduler = load('src/lib/gemini/scheduler.mjs', baseline)
  const errors = load('src/lib/core/errors.mjs', baseline)
  const timing = load('src/lib/protocol/stream-timing.mjs', baseline)
  const fixedCrypto = { ...crypto, randomBytes: n => Buffer.alloc(n, 1), randomUUID: () => '00000000-0000-4000-8000-000000000001' }
  const context = vm.createContext({ Buffer, TextDecoder, crypto: fixedCrypto, console, GEMINI_HOST_RESOURCE_MESSAGES: {}, GEMINI_LICENSE_MESSAGES: {} })
  const constants = converter.slice(converter.indexOf('const MODEL_RE'), converter.indexOf('function customToolSchema')) + '\n' + converter.match(/^const MCP_CALL_PREFIXES = .*$/m)[0] + '\n' + converter.match(/^const AGY_BRIDGE_INJECTED_PARAMS = .*$/m)[0]
  const failures = scheduler.slice(scheduler.indexOf('const FAILURES ='), scheduler.indexOf('\n}', scheduler.indexOf('const FAILURES =')) + 2)
  vm.runInContext(`${constants}\n${failures}\n${errors.slice(errors.indexOf('const SAFE_ERROR_TOKEN_RE'), errors.indexOf('/** Strict bounded identifier sanitizer')).replace(/export function markGeminiToolDetailsForLog[\s\S]*?\n}/, '')}\n${functions(converter)}\n${functions(timing)}\n${functions(errors)}\n${functions(scheduler)}\nthis.api = {consumeStreamResponse, known, sanitizeErrorDetailsForLog, markGeminiToolDetailsForLog: typeof markGeminiToolDetailsForLog === 'function' ? markGeminiToolDetailsForLog : null, extractErrorLogFields};`, context)
  return context.api
}
const api = harness()
const sentinel = 'PRIVATE_SENTINEL'
const context = (extra = {}) => ({
  toolChoice: 'required',
  toolMap: new Map([['known', { wireName: 'known', origName: 'known', schema: {} }]]),
  declaredToolNames: new Set(['known']),
  ...extra,
})
const frame = (parts, finishReason = 'STOP') => `data: ${JSON.stringify({ candidates: [{ content: { parts }, finishReason }] })}\n\n`
async function run(parts, ctx = context(), impl = api, frames = null) {
  const events = []
  const response = Object.assign(Readable.from(frames || [frame(parts)]), { statusCode: 200 })
  try {
    const result = await impl.consumeStreamResponse(response, { model: 'gemini-test', restoreContext: ctx, onEvent: e => events.push(e), startedAt: 0, now: () => 10 })
    return { result, events }
  } catch (error) {
    return { error: impl.known(error) || error, events }
  }
}
const call = name => ({ functionCall: { name, args: { secret: sentinel } }, thoughtSignature: sentinel })
const plain = value => JSON.parse(JSON.stringify(value))

test('trusted diagnostics survive 1024-byte compaction and repeated sanitization', () => {
  const src = api.markGeminiToolDetailsForLog({
    gemini_tool_decoded: 65535, gemini_tool_restored: 0, gemini_tool_allowed_filtered: 65535,
    gemini_tool_decoder_end_reason: 'filter', gemini_tool_restored_end_reason: 'not_observed',
    upstream_type: 't'.repeat(128), upstream_code: 'c'.repeat(128), reason: 'r'.repeat(128),
    scheduler_exclusions: Object.fromEntries(Array.from({length: 8}, (_, i) => [`r${i}_${'a'.repeat(60)}`, 123456789])),
  })
  const out = api.sanitizeErrorDetailsForLog(src)
  assert.ok(Buffer.byteLength(JSON.stringify(out)) <= 1024)
  assert.equal(out.gemini_tool_decoded, 65535)
  assert.equal(api.sanitizeErrorDetailsForLog({...out}).gemini_tool_allowed_filtered, 65535)
  for (const bad of [-1, 65536, 1.5, '1', true, {}, NaN, Infinity]) {
    assert.equal(api.sanitizeErrorDetailsForLog(api.markGeminiToolDetailsForLog({gemini_tool_decoded: bad})), null)
  }
})

test('decoder and restored end reasons are separate; unobserved is not upstream absence', async () => {
  for (const [finish, expected] of [['STOP', 'stop']]) {
    const {error} = await run([{text: sentinel}], context(), api, [frame([{text: sentinel}], finish)])
    assert.equal(error.details.gemini_tool_decoder_end_reason, expected)
    assert.equal(error.details.gemini_tool_restored_end_reason, expected)
  }
  const {error} = await run([call(sentinel)], context({allowedToolNames: new Set(['known'])}))
  assert.equal(error.details.gemini_tool_decoder_end_reason, 'tool')
  assert.equal(error.details.gemini_tool_restored_end_reason, 'stop')
  const invalid = await run([call(sentinel)])
  assert.equal(invalid.error.details.gemini_tool_decoder_end_reason, 'tool')
  assert.equal(invalid.error.details.gemini_tool_restored_end_reason, 'not_observed')
})

test('decoder and allowlist counters saturate at 65535', async () => {
  const parts = Array.from({length: 65540}, () => call(sentinel))
  const {error} = await run(parts, context({allowedToolNames: new Set(['known'])}))
  assert.equal(error.details.gemini_tool_decoded, 65535)
  assert.equal(error.details.gemini_tool_allowed_filtered, 65535)
  assert.equal(error.details.gemini_tool_restored, 0)
  const restored = await run([...Array.from({length: 65536}, () => call('known')), call(sentinel)])
  assert.equal(restored.error.details.gemini_tool_restored, 65535)
  assert.equal(restored.error.details.gemini_tool_decoded, 65535)
})

test('untrusted same-name diagnostic fields are dropped', () => {
  assert.equal(api.sanitizeErrorDetailsForLog({ gemini_tool_decoded: 42, gemini_tool_restored: 1, gemini_tool_allowed_filtered: 41, gemini_tool_end_reason: 'stop' }), null)
})

test('undeclared tool error retains structural counts without names', async () => {
  const { error } = await run([call(sentinel)])
  assert.equal(error.code, 'gemini_inference_invalid_output')
  assert.equal(error.upstreamReason, 'undeclared_tool_call')
  const details = api.sanitizeErrorDetailsForLog(error.details)
  assert.equal(details.gemini_tool_decoded, 1)
  assert.equal(details.gemini_tool_restored, 0)
  assert.ok(!JSON.stringify(details).includes(sentinel))
})

test('diagnostics preserve baseline events and error attribution', { skip: !process.env.GEMINI_DIAG_BASELINE_DIR }, async () => {
  const base = harness(true)
  for (const [parts, ctx] of [
    [[call('known')], context()],
    [[{ text: sentinel }], context()],
    [[call(sentinel)], context()],
    [[call(sentinel)], context({ allowedToolNames: new Set(['known']) })],
    [[call(sentinel), call('known')], context({ allowedToolNames: new Set(['known']) })],
    [[call('known'), call(sentinel)], context()],
    [[{functionCall: {name: 'known', args: 'malformed'}}], context()],
    [[{text: sentinel}], context({maxOutputTokens: 1})],
    [[{text: sentinel}], context({stopSequences: ['SENTINEL']})],
  ]) {
    const before = await run(parts, ctx, base)
    const after = await run(parts, ctx)
    assert.deepEqual(plain(after.events), plain(before.events))
    if (before.error) {
      for (const k of ['code', 'status', 'localError', 'upstreamStatus', 'upstreamReason']) assert.equal(after.error[k], before.error[k])
    } else assert.deepEqual(plain(after.result), plain(before.result))
  }
})

test('allowlist-filtered call is distinguished from no decoded call', async () => {
  const { error } = await run([call(sentinel)], context({ allowedToolNames: new Set(['known']) }))
  assert.equal(error.code, 'gemini_tool_call_required', error.stack)
  const details = api.sanitizeErrorDetailsForLog(error.details)
  assert.equal(details.gemini_tool_decoded, 1)
  assert.equal(details.gemini_tool_allowed_filtered, 1)
  assert.equal(details.gemini_tool_restored, 0)
  assert.ok(!JSON.stringify(details).includes(sentinel))
})

test('required call absent: diagnostic survives known and normal-log sanitizer', async () => {
  const { error, events } = await run([{ text: sentinel }])
  assert.equal(error.code, 'gemini_tool_call_required', error.stack)
  assert.equal(error.localError, true)
  assert.equal(error.upstreamStatus, 200)
  assert.ok(events.some(e => e.kind === 'text')) // committed stream content remains emitted
  const details = api.sanitizeErrorDetailsForLog(error.details)
  assert.equal(details?.gemini_tool_decoded, 0)
  assert.equal(details.gemini_tool_restored, 0)
  assert.equal(details.gemini_tool_decoder_end_reason, 'stop')
  assert.ok(!JSON.stringify(details).includes(sentinel))
})
