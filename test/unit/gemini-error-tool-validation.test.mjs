import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { Readable } from 'node:stream'
import test from 'node:test'
import { RequestLogStore } from '../../src/lib/admin/request-log.mjs'
import { sanitizeErrorDetailsForLog } from '../../src/lib/core/errors.mjs'
import { createGeminiPublicHandler } from '../../src/lib/gemini/routes.mjs'
import { createGeminiScheduler } from '../../src/lib/gemini/scheduler.mjs'
import { readBody } from '../../src/lib/http/respond.mjs'
import {
  FULL_GEMINI_CAPABILITIES,
  buildGeminiRequest,
  createGeminiCollector,
  createGeminiEventDecoder,
  createGeminiRestoreTransform,
  parseInboundToGemini,
} from '../../src/lib/protocol/gemini-convert.mjs'
import { SchedulerApiError } from '../../src/lib/transport/scheduler-client.mjs'
import { createMockSchedulerRuntime } from '../helpers/mock-scheduler-runtime.mjs'

const model = 'gemini-4-neon-xhigh'
const tools = [{ type: 'function', name: 'known', parameters: { type: 'object', properties: {} } }]
const requestBody = (extra = {}) => ({ model, input: 'synthetic check', tools, ...extra })
const parse = (extra = {}) => parseInboundToGemini('openai-responses', requestBody(extra))
const chunk = (names, extra = {}) => ({
  candidates: [
    {
      content: { role: 'model', parts: names.map((name) => ({ functionCall: { name, args: {} } })) },
      finishReason: 'STOP',
    },
  ],
  ...extra,
})
const frame = (body) => `data: ${JSON.stringify(body)}\n\n`

function restore(ir, body) {
  const decoder = createGeminiEventDecoder()
  const transform = createGeminiRestoreTransform(ir.restoreContext, { target: 'antigravity' })
  const collector = createGeminiCollector()
  decoder.decodeChunk(body, (event) => transform.push(event, (out) => collector.push(out)))
  transform.flush((out) => collector.push(out))
  return collector.result()
}

function fixture({ frames = [], runtime = createMockSchedulerRuntime(), bodyReader = readBody, requestLog } = {}) {
  let calls = 0
  const logs = []
  const scheduler = createGeminiScheduler({
    schedulerRuntime: runtime,
    callHost: async (op) => {
      const env = {
        id: 'env-validation',
        proxy_id: 'px',
        auth_method: 'oauth',
        auth_state: 'authorized',
        models_discovered: true,
        models: [model],
        capabilities: [...FULL_GEMINI_CAPABILITIES],
      }
      if (op === 'list') return { payload_state: 'installed', environments: [env] }
      if (op === 'get') return env
      throw new Error(`unexpected host operation: ${op}`)
    },
    callHostStream: async () => {
      calls++
      return Object.assign(Readable.from(frames), { statusCode: 200, headers: { 'content-type': 'text/event-stream' } })
    },
    resolveProxy: () => 'socks5h://192.0.2.1:1080',
  })
  const handler = createGeminiPublicHandler({
    scheduler,
    readBody: bodyReader,
    json: (res, status, body) => {
      res.statusCode = status
      res.end(JSON.stringify(body))
    },
    requestLog: requestLog || {
      start: () => ({ request_id: `validation-${logs.length}` }),
      finish: (_ctx, log) => logs.push(log),
    },
  })
  return {
    logs,
    get calls() {
      return calls
    },
    async call(body, { path = '/v1/responses', raw } = {}) {
      const req = Object.assign(raw === undefined ? new EventEmitter() : Readable.from([raw]), {
        method: 'POST',
        url: path,
        body,
        headers: {},
        apiKeyKind: 'managed',
        apiKeyRecord: { id: 'test', group_type: 'gemini' },
      })
      const res = new EventEmitter()
      Object.assign(res, { statusCode: 200, headers: {}, chunks: [], writableEnded: false, headersSent: false })
      res.setHeader = (key, value) => {
        res.headers[key.toLowerCase()] = value
      }
      res.writeHead = (status, headers = {}) => {
        res.statusCode = status
        res.headersSent = true
        Object.assign(res.headers, headers)
      }
      res.write = (data) => {
        res.headersSent = true
        res.chunks.push(String(data))
        return true
      }
      res.end = (data) => {
        if (data) res.write(data)
        res.writableEnded = true
        res.emit('finish')
      }
      await handler(req, res)
      return { status: res.statusCode, headers: res.headers, text: res.chunks.join('') }
    },
  }
}

test('invalid tool constraints fail before dispatch, including an empty Responses allowlist', async () => {
  for (const tool_choice of [
    { type: 'allowed_tools', mode: 'auto', tools: [] },
    { type: 'allowed_tools', mode: 'required', tools: [] },
    { type: 'allowed_tools', mode: 'auto', tools: [{ type: 'function', name: 'missing' }] },
    { type: 'function', name: 'missing' },
  ]) {
    const f = fixture()
    const res = await f.call(requestBody({ tool_choice }))
    assert.equal(res.status, 400)
    assert.equal(JSON.parse(res.text).error.code, 'gemini_invalid_request')
    assert.equal(f.calls, 0)
    assert.equal(f.logs[0].attempt_count, 0)
  }
  assert.throws(
    () =>
      parseInboundToGemini('openai-chat', {
        model,
        messages: [{ role: 'user', content: 'check' }],
        tools: [{ type: 'function', function: { name: 'known' } }],
        tool_choice: { type: 'allowed_tools', allowed_tools: { mode: 'auto', tools: [] } },
      }),
    { status: 400, code: 'gemini_invalid_request' },
  )
})

test('only current declarations authorize emitted calls; history names do not grant tool access', () => {
  for (const tool_choice of ['auto', 'required']) {
    const ir = parse({
      tool_choice,
      input: [
        { type: 'function_call', call_id: 'old-call', name: 'old_tool', arguments: '{}' },
        { type: 'function_call_output', call_id: 'old-call', output: 'done' },
        { role: 'user', content: 'continue' },
      ],
    })
    buildGeminiRequest(ir, { target: 'antigravity' })
    for (const name of ['unknown', 'old_tool']) {
      const result = restore(
        ir,
        chunk([name, 'known'], {
          usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 5, totalTokenCount: 15 },
        }),
      )
      assert.equal(result.toolCalls.length, 0)
      assert.equal(result.error?.code, 'gemini_inference_invalid_output')
      assert.equal(result.usage.total_tokens, 15, 'usage survives output validation failure')
    }
  }
  assert.equal(restore(parse({ tools: [] }), chunk(['unknown'])).error?.code, 'gemini_inference_invalid_output')
})

test('valid declared, namespaced and collision-disambiguated tools still round-trip', () => {
  const ir = parse({
    tools: [
      ...tools,
      { type: 'function', name: 'a.b' },
      { type: 'function', name: 'a_b' },
      { type: 'namespace', name: 'ns', tools: [{ type: 'function', name: 'run' }] },
    ],
  })
  const result = restore(ir, chunk(ir.tools.map((t) => `mcp_crag_${t.wireName}`)))
  assert.equal(result.error, null)
  assert.deepEqual(
    result.toolCalls.map((t) => t.name),
    ['known', 'a.b', 'a_b', 'run'],
  )
  const forced = parse({ tool_choice: { type: 'function', name: 'known' } })
  assert.equal(restore(forced, chunk(['known'])).toolCalls.length, 1)
  assert.equal(restore(parse({ parallel_tool_calls: false }), chunk(['known', 'known'])).toolCalls.length, 1)
})

test('all SSE error envelopes retain ErrorInfo reason without copying arbitrary metadata', async () => {
  const error = {
    code: 504,
    status: 'DEADLINE_EXCEEDED',
    message: 'timeout',
    details: [
      {
        '@type': 'type.googleapis.com/google.rpc.ErrorInfo',
        reason: 'gemini_inference_timeout',
        metadata: { private: 'not-for-public-output' },
      },
    ],
  }
  for (const wire of [frame({ error }), `event: error\n${frame({ error })}`, frame({ response: { error } })]) {
    const events = []
    createGeminiEventDecoder().decodeSseFrame(wire, (e) => events.push(e))
    assert.equal(events[0].reason, 'gemini_inference_timeout')
    assert.equal(events[0].code, 'DEADLINE_EXCEEDED')
    const f = fixture({ frames: [wire] })
    const res = await f.call(requestBody())
    assert.equal(res.status, 504)
    assert.equal(JSON.parse(res.text).error.code, 'gemini_inference_timeout')
    assert.equal(f.logs[0].hop_meta.upstream_reason, 'gemini_inference_timeout')
    assert.doesNotMatch(res.text, /not-for-public-output/)
  }
})

test('unknown upstream reasons stay diagnostic; a committed error stays an SSE error', async () => {
  const error = {
    code: 504,
    status: 'DEADLINE_EXCEEDED',
    message: 'timeout',
    details: [{ '@type': 'type.googleapis.com/google.rpc.ErrorInfo', reason: 'UNRECOGNIZED_REASON' }],
  }
  const f = fixture({
    frames: [
      frame({ candidates: [{ content: { role: 'model', parts: [{ text: 'thinking', thought: true }] } }] }),
      frame({ error }),
    ],
  })
  const res = await f.call(requestBody({ stream: true }))
  assert.equal(res.status, 200)
  assert.match(res.text, /gemini_inference_failed/)
  assert.equal(f.logs[0].upstream_status, 504)
  assert.equal(f.logs[0].hop_meta.upstream_reason, 'UNRECOGNIZED_REASON')
  assert.equal(f.calls, 1, 'no retry after protocol data is committed')
})

test('pool exhaustion preserves scheduler explain internally and Retry-After without exposing it', async () => {
  const explain = { excluded: [{ node: 'private-node', reason: 'node_cooldown', detail: 'cooldown_left_ms=30000' }] }
  const runtime = createMockSchedulerRuntime()
  runtime.acquireNode = async () => {
    throw new SchedulerApiError({ status: 429, code: 'pool_exhausted', retryAfterSec: 30, explain })
  }
  const f = fixture({ runtime })
  for (const stream of [false, true]) {
    const res = await f.call(requestBody({ stream }))
    assert.equal(res.status, 429)
    assert.equal(res.headers['retry-after'], '30')
    assert.deepEqual(f.logs.at(-1).hop_meta.scheduler_explain, explain)
    assert.doesNotMatch(res.text, /private-node|node_cooldown|excluded/)
    assert.equal(f.calls, 0)
  }
})

test('real body reader distinguishes oversized bodies from invalid JSON at the public route', async () => {
  const f = fixture()
  for (const [raw, status, code] of [
    [Buffer.alloc(12 * 1024 * 1024 + 1, 32), 413, 'body_too_large'],
    [Buffer.from('{'), 400, 'invalid_json'],
  ]) {
    const res = await f.call(undefined, { raw })
    assert.equal(res.status, status)
    assert.equal(JSON.parse(res.text).error.code, code)
    assert.equal(f.logs.at(-1).error_code, code)
    assert.equal(f.logs.at(-1).attempt_count, 0)
  }
  assert.equal(f.calls, 0)
})

test('normal logs persist exclusion counts and upstream reasons without enabling body capture', async (t) => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gemini-error-diagnostics-'))
  const store = new RequestLogStore({ dataDir, mode: 'normal' })
  t.after(() => {
    store.db.close()
    fs.rmSync(dataDir, { recursive: true, force: true })
  })
  const runtime = createMockSchedulerRuntime()
  runtime.acquireNode = async () => {
    throw new SchedulerApiError({
      status: 429,
      code: 'pool_exhausted',
      retryAfterSec: 30,
      explain: {
        excluded: [
          { node: 'private-node-1', reason: 'node_cooldown' },
          { node: 'private-node-2', reason: 'node_cooldown' },
          { node: 'private-node-3', reason: 'max_concurrency' },
        ],
        prompt_policy: { text: 'private policy' },
      },
    })
  }
  const f = fixture({ runtime, requestLog: store })
  await f.call(requestBody())
  const details = store.db.prepare('SELECT error_details FROM usage_logs').get().error_details
  assert.deepEqual(JSON.parse(details).scheduler_exclusions, { node_cooldown: 2, max_concurrency: 1 })
  assert.doesNotMatch(details, /private-node|private policy/)
  const timeout = fixture({
    requestLog: store,
    frames: [
      frame({
        error: {
          code: 504,
          status: 'DEADLINE_EXCEEDED',
          details: [{ '@type': 'type.googleapis.com/google.rpc.ErrorInfo', reason: 'gemini_inference_timeout' }],
        },
      }),
    ],
  })
  await timeout.call(requestBody())
  const timeoutRow = store.db
    .prepare("SELECT error_details FROM usage_logs WHERE error_code = 'gemini_inference_timeout'")
    .get()
  assert.equal(JSON.parse(timeoutRow.error_details).reason, 'gemini_inference_timeout')
  assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM request_log_debug').get().n, 0)
})

test('scheduler diagnostic counts are bounded and contain only safe identifiers and positive counts', () => {
  const details = sanitizeErrorDetailsForLog({
    scheduler_exclusions: {
      node_cooldown: 3,
      bad: 'raw detail',
      zero: 0,
      negative: -1,
      'secret with spaces': 1,
      'sk-ant-secret': 1,
    },
  })
  assert.deepEqual(details, { scheduler_exclusions: { node_cooldown: 3 } })
  const bounded = sanitizeErrorDetailsForLog({
    reason: 'x'.repeat(128),
    prior_reason: 'y'.repeat(128),
    upstream_code: 'z'.repeat(128),
    upstream_type: 't'.repeat(128),
    scheduler_exclusions: Object.fromEntries(
      Array.from({ length: 20 }, (_, i) => [`r${i}_${'a'.repeat(60)}`, Number.MAX_SAFE_INTEGER]),
    ),
  })
  assert.ok(Buffer.byteLength(JSON.stringify(bounded)) <= 1024)
  assert.ok(Object.keys(bounded.scheduler_exclusions || {}).length <= 8)
})

test('undeclared calls produce an error at the public route and retain reported usage', async () => {
  for (const stream of [false, true]) {
    const f = fixture({
      frames: [
        frame(
          chunk(['unknown'], { usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 5, totalTokenCount: 15 } }),
        ),
      ],
    })
    const res = await f.call(requestBody({ stream }))
    assert.equal(res.status, stream ? 200 : 502)
    assert.match(res.text, /gemini_inference_invalid_output/)
    assert.doesNotMatch(res.text, /"name":"unknown"/)
    assert.equal(f.logs[0].final_state, 'failed')
    assert.equal(f.logs[0].error_details.reason, 'undeclared_tool_call')
    assert.equal(f.logs[0].usage.output_tokens, 5)
  }
})

test('forced missing names fail in Chat, Messages and Gemini native requests', () => {
  for (const [protocol, body] of [
    [
      'openai-chat',
      {
        model,
        messages: [{ role: 'user', content: 'check' }],
        tools: [{ type: 'function', function: { name: 'known' } }],
        tool_choice: { type: 'function', function: { name: 'missing' } },
      },
    ],
    [
      'anthropic',
      {
        model,
        max_tokens: 100,
        messages: [{ role: 'user', content: 'check' }],
        tools: [{ name: 'known', input_schema: { type: 'object' } }],
        tool_choice: { type: 'tool', name: 'missing' },
      },
    ],
    [
      'gemini',
      {
        model,
        contents: [{ role: 'user', parts: [{ text: 'check' }] }],
        tools: [{ functionDeclarations: [{ name: 'known' }] }],
        toolConfig: { functionCallingConfig: { mode: 'ANY', allowedFunctionNames: ['missing'] } },
      },
    ],
  ]) {
    assert.throws(() => parseInboundToGemini(protocol, body), { status: 400, code: 'gemini_invalid_request' })
  }
})

test('required tools retain MAX_TOKENS completion semantics', async () => {
  const f = fixture({
    frames: [
      frame({ candidates: [{ content: { role: 'model', parts: [{ text: 'partial' }] }, finishReason: 'MAX_TOKENS' }] }),
    ],
  })
  const res = await f.call(requestBody({ tool_choice: 'required' }))
  assert.equal(res.status, 200)
  assert.equal(JSON.parse(res.text).incomplete_details.reason, 'max_output_tokens')
  assert.equal(f.logs[0].final_state, 'verified')
})
