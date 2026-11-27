// Opt-in gateway metadata. Never change public protocol payloads or hold the
// first token while waiting to learn whether a turn is a local tool drain.
export function createUsageLink(req, res, requestId) {
  const enabled = req?.headers?.['x-vm2api-usage-link'] === '1'
  if (!enabled || typeof requestId !== 'string' || !/^[A-Za-z0-9_.:-]{1,128}$/.test(requestId)) {
    return { complete: () => '' }
  }
  res.setHeader('X-Vm2api-Usage-Link', '1')
  res.setHeader('X-Vm2api-Request-Id', requestId)
  return {
    complete(turn, stream) {
      const parent = turn?.tool_continuation_parent_request_id
      if (!turn?.local_tool_drain || typeof parent !== 'string' ||
          !/^[A-Za-z0-9_.:-]{1,128}$/.test(parent) || parent === requestId) return ''
      if (stream) {
        return `: vm2api-usage-link ${JSON.stringify({ request_id: requestId, parent_request_id: parent, kind: 'local_tool_drain' })}\n\n`
      }
      res.setHeader('X-Vm2api-Parent-Request-Id', parent)
      res.setHeader('X-Vm2api-Continuation', 'local_tool_drain')
      return ''
    },
  }
}
