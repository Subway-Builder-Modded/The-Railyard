# scripts/ops

Manual operational tooling — nothing here runs in scheduled automation.
Invoke via the package scripts in `scripts/package.json` (names unchanged from
when these lived at the top level), e.g. `pnpm --dir scripts run audit-download-history`.

- `audit-download-history.ts` — verify download-history snapshots for consistency.
- `rebuild-download-history-from-git.ts` / `rebuild-download-version-buckets.ts` —
  deterministic rebuilds from git history; part of the 429-cascade recovery playbook.
- `backfill-download-attribution.ts` — step 1 of the canonical consistency suite
  (see README "Canonical consistency suite").
- `spotcheck-attribution-logs.ts` — attribution log diagnostics.
- `create-manual-download-attribution.ts` — the manual download-correction path.
  Applied specs are committed under `history/manual-attribution-specs/`;
  application is idempotent (`applied_delta_ids`), so **after any
  attribution-ledger rebuild, re-apply every committed spec**.
- `repair-loop-inflated-downloads.ts` — spec-driven, re-runnable repair for client
  re-download loops (excess over a baseline/superseded-peer/peer-adoption organic
  allowance → per-day attribution deltas + snapshot clamp + bucket-ceiling
  lowering). Specs live under
  `history/loop-repair-specs/`; like manual-attribution specs, **re-apply after any
  attribution-ledger rebuild**. See KNOWN_INCIDENTS.md 2026-08 French-city entry.
  If a repair target has an entry in `maps|mods/grandfathered-downloads.json`,
  **lower that entry too**: the deprecate/delete freeze takes the max of
  ledger, committed, and existing grandfathered values, so a stale high
  grandfathered count silently resurrects the inflated total if the listing is
  later deprecated or deleted (or otherwise leaves pipeline output).
- `backfill-charleston-snapshot-clamp.ts` — one-shot snapshot re-interpolation
  for the charleston-huntington-wv faulty-client inflation (KNOWN_INCIDENTS.md,
  2026-07 entry). Retained because **any snapshot rebuild from git
  (`rebuild-download-history-from-git.ts`) must be followed by re-running this
  clamp** — pre-correction snapshot values would otherwise reseed an inflated
  `history-max:` floor via `rebuild-download-version-buckets.ts`.
- `audit-shared-map-attribution.ts` + `export-shared-map-attribution-audit.sh` —
  parameterized shared-pack attribution audits.
- `backfill-website-analytics.ts` — refetch missed hourly Cloudflare snapshots
  (worker/capture outage gap-filler).
- `backfill-hourly-downloads.ts` — deterministically rebuilds the hourly
  download series (`analytics/hourly/downloads-YYYY-MM.csv` monthly shards)
  from the git history of
  `downloads.json`; initial backfill and the recovery path if the hourly
  appender's series is ever lost or corrupted. The window defaults to the
  full span back to `HOURLY_DOWNLOADS_BACKFILL_FLOOR` (2026-07-01 — hourly
  commits are too sparse before the Cloudflare Worker scheduler; see
  KNOWN_INCIDENTS.md) and `--days` can only shorten it. Any administrative
  counter RAISE (grandfathered restore, ledger-rebuild recovery) reads as a
  one-hour download burst — the clamp only guards drops. **After such a
  recovery, add an entry to `history/hourly-suppressions.json`** (bucket hour +
  listing; omit `downloads` to drop the whole row, set it to subtract an
  amount): the backfill applies committed suppressions automatically, so
  re-runs spanning the raise stay pruned. If the burst row already landed via
  the live appender, delete it from the CSV in the same commit.

One-time migrations that already ran are deleted rather than kept here — git
history is the archive (see tmp/plans/registry-downsizing-audit.md for the list).
