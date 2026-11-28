# Gemini account quota admission

Gemini scheduling consumes the host account snapshot used by the panel: both `account.quotas` (per model) and `account.quota_groups[].windows` (shared buckets). A numeric `remaining_fraction: 0` excludes the account for the applicable model while its reset is in the future or unknown. Either the five-hour or weekly window can block admission.

The model catalog determines the quota group. Window IDs `gemini-5h` / `gemini-weekly` apply to Gemini, including Argon; `gemini-argon-wtu-5h` / `gemini-argon-wtu-weekly` additionally constrain Argon. `3p-5h` / `3p-weekly` apply to Claude/GPT. Display names are not identifiers. Per-model entries respect resolved aliases and effort variants; a base-model limit applies to its variants.

Missing, null, invalid quota values and unrecognized window IDs are unknown and do not independently block admission. A known zero without a valid reset remains blocked for that bucket until refreshed data permits it. A past reset expires that zero signal; positive refreshed quota also restores eligibility. Existing health, license, manual-disable, tenant and key restrictions still apply. This does not change the account's overall `schedulable` switch or clear cooldowns.

The Gemini request entry filters candidates before acquiring a lease. If every authorized, capable candidate is exhausted, it returns HTTP 429 `gemini_quota_exhausted` with zero upstream attempts. It never falls back to dispatching depleted candidates. `Retry-After` is included only when an account has known future reset times for all its exhausted windows: take the latest reset for each account, then the earliest recovery across accounts. Internal request logs record quota exclusions without exposing account topology in the public error.

Both host-list discovery and cached-catalog bootstrap preserve only quota windows, per-model quotas and model catalog metadata in the environment projection. Account identity and credentials are not forwarded.

The Node M3 bridge independently intersects every acquire/switch request's `allowed_node_keys` with quota eligibility from the latest available account snapshot, including supplied API scopes. Rust enforces that intersection through its existing admission gate. This uses model-scoped candidate restrictions rather than node-wide quota telemetry, preserving other providers on the same account. An empty eligible set is represented by a nonmatching sentinel, since Rust treats an empty list as unrestricted.

After Rust grants an acquire or switch lease, the bridge checks the selected account against the latest snapshot again. If the quota became exhausted while the request waited, it releases the lease as aborted and returns `gemini_quota_exhausted` before exposing it to inference. This rejection does not create a node-wide cooldown. Rust acquire/switch idempotency caches only replay leases that still exist and have not expired. Replaying a released attempt returns `lease_not_found`; a new request or attempt is required for a fresh lease, rather than silently reusing capacity that was already released. Model compatibility and quota resolution both receive the explicitly requested effort.

Local undeclared-tool validation errors retain the real HTTP response status in diagnostics; their gateway 502 response does not trigger the policy for suspending an account on an upstream 502. Actual upstream error frames retain their own error status and policy.

This relies on host snapshot freshness; it does not infer quota from response text or make additional upstream calls. New quota consumption cannot be detected until the host snapshot or a structured upstream quota error reports it.
