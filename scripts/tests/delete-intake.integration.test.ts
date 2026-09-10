import test from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync, type SpawnSyncReturns } from "node:child_process";

const scriptsRoot = resolve(import.meta.dirname, "..", "..");

const AUTHOR_ID = 1000;
const COLLABORATOR_ID = 2000;
const ACTIVE_CARETAKER_ID = 3000;
const STRANGER_ID = 9000;

function baseModManifest(id: string): Record<string, unknown> {
  return {
    schema_version: 1,
    id,
    name: "Fixture Mod",
    author: "fixture-author",
    github_id: AUTHOR_ID,
    collaborators: [COLLABORATOR_ID, ACTIVE_CARETAKER_ID],
    caretakers: [
      { github_id: ACTIVE_CARETAKER_ID, since: "2026-03-01T00:00:00Z" },
    ],
    description: "A fixture mod.",
    tags: ["qol"],
    gallery: [],
    is_test: false,
    source: "https://example.com/fixture",
    update: { type: "github", repo: "fixture/fixture" },
  };
}

function makeFixtureRepo(manifests: Record<string, Record<string, unknown>>): string {
  const root = mkdtempSync(join(tmpdir(), "railyard-delete-"));
  mkdirSync(join(root, "maps"), { recursive: true });
  mkdirSync(join(root, "scripts"), { recursive: true });
  for (const [id, manifest] of Object.entries(manifests)) {
    const dir = join(root, "mods", id);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n", "utf-8");
  }
  return root;
}

function runScript(
  scriptName: "validate-delete" | "delete-listing" | "validate-undeprecate",
  fixtureRoot: string,
  env: Record<string, string>,
): SpawnSyncReturns<string> {
  const compiledScriptPath = resolve(scriptsRoot, ".test-dist", "intake", `${scriptName}.js`);
  return spawnSync(process.execPath, [compiledScriptPath], {
    cwd: fixtureRoot,
    env: {
      ...process.env,
      RAILYARD_REPO_ROOT: fixtureRoot,
      ...env,
    },
    encoding: "utf-8",
  });
}

function issueJson(assetId: string, opts?: { reason?: string; confirmed?: boolean }): string {
  return JSON.stringify({
    "asset-id": assetId,
    reason: opts?.reason ?? "_No response_",
    confirmation: opts?.confirmed === false
      ? "_No response_"
      : "I understand deletion is permanent and cannot be undone",
  });
}

function readManifest(root: string, id: string): Record<string, unknown> {
  return JSON.parse(readFileSync(join(root, "mods", id, "manifest.json"), "utf-8")) as Record<string, unknown>;
}

// --- validate-delete ---

test("delete validation passes for the original publisher", (t) => {
  const root = makeFixtureRepo({ "fixture-mod": baseModManifest("fixture-mod") });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  const result = runScript("validate-delete", root, {
    ISSUE_JSON: issueJson("fixture-mod"),
    ISSUE_AUTHOR_ID: String(AUTHOR_ID),
  });
  assert.equal(result.status, 0, result.stderr);
});

test("delete validation passes for the active caretaker", (t) => {
  const root = makeFixtureRepo({ "fixture-mod": baseModManifest("fixture-mod") });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  const result = runScript("validate-delete", root, {
    ISSUE_JSON: issueJson("fixture-mod"),
    ISSUE_AUTHOR_ID: String(ACTIVE_CARETAKER_ID),
  });
  assert.equal(result.status, 0, result.stderr);
});

test("delete validation rejects collaborators and strangers", (t) => {
  const root = makeFixtureRepo({ "fixture-mod": baseModManifest("fixture-mod") });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  for (const requester of [COLLABORATOR_ID, STRANGER_ID]) {
    const result = runScript("validate-delete", root, {
      ISSUE_JSON: issueJson("fixture-mod"),
      ISSUE_AUTHOR_ID: String(requester),
    });
    assert.notEqual(result.status, 0);
    assert.match(readFileSync(join(root, "scripts", "validation-error.md"), "utf-8"), /Ownership check failed/);
  }
});

test("delete validation requires the permanence acknowledgement", (t) => {
  const root = makeFixtureRepo({ "fixture-mod": baseModManifest("fixture-mod") });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  const result = runScript("validate-delete", root, {
    ISSUE_JSON: issueJson("fixture-mod", { confirmed: false }),
    ISSUE_AUTHOR_ID: String(AUTHOR_ID),
  });
  assert.notEqual(result.status, 0);
  assert.match(readFileSync(join(root, "scripts", "validation-error.md"), "utf-8"), /acknowledge that deletion is permanent/);
});

test("delete validation accepts an already-deprecated asset (escalation) but rejects re-deletion", (t) => {
  const deprecated = baseModManifest("dep-mod");
  deprecated.deprecation = { since: "2026-08-01T00:00:00Z", by_github_id: AUTHOR_ID };
  const deleted = baseModManifest("del-mod");
  deleted.deprecation = { since: "2026-08-01T00:00:00Z", by_github_id: AUTHOR_ID, deleted: true };
  const root = makeFixtureRepo({ "dep-mod": deprecated, "del-mod": deleted });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  const escalation = runScript("validate-delete", root, {
    ISSUE_JSON: issueJson("dep-mod"),
    ISSUE_AUTHOR_ID: String(AUTHOR_ID),
  });
  assert.equal(escalation.status, 0, escalation.stderr);

  const reDelete = runScript("validate-delete", root, {
    ISSUE_JSON: issueJson("del-mod"),
    ISSUE_AUTHOR_ID: String(AUTHOR_ID),
  });
  assert.notEqual(reDelete.status, 0);
  assert.match(readFileSync(join(root, "scripts", "validation-error.md"), "utf-8"), /already deleted/);
});

// --- delete-listing ---

test("delete-listing stamps a schema-valid deleted deprecation record", (t) => {
  const root = makeFixtureRepo({ "fixture-mod": baseModManifest("fixture-mod") });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  const result = runScript("delete-listing", root, {
    ISSUE_JSON: issueJson("fixture-mod", { reason: "Source repository removed" }),
    ISSUE_AUTHOR_ID: String(AUTHOR_ID),
  });
  assert.equal(result.status, 0, result.stderr);

  const deprecation = readManifest(root, "fixture-mod").deprecation as Record<string, unknown>;
  assert.equal(deprecation.deleted, true);
  assert.equal(deprecation.by_github_id, AUTHOR_ID);
  assert.equal(deprecation.reason, "Source repository removed");
  assert.equal(typeof deprecation.since, "string");
});

test("delete-listing escalation keeps since, takes over by_github_id, keeps reason unless replaced", (t) => {
  const manifest = baseModManifest("fixture-mod");
  manifest.deprecation = {
    since: "2026-08-01T00:00:00Z",
    by_github_id: AUTHOR_ID,
    reason: "Original deprecation reason",
  };
  const root = makeFixtureRepo({ "fixture-mod": manifest });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  const result = runScript("delete-listing", root, {
    ISSUE_JSON: issueJson("fixture-mod"),
    ISSUE_AUTHOR_ID: String(ACTIVE_CARETAKER_ID),
  });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /escalated from deprecation/);

  const deprecation = readManifest(root, "fixture-mod").deprecation as Record<string, unknown>;
  assert.equal(deprecation.since, "2026-08-01T00:00:00Z");
  assert.equal(deprecation.by_github_id, ACTIVE_CARETAKER_ID);
  assert.equal(deprecation.reason, "Original deprecation reason");
  assert.equal(deprecation.deleted, true);
});

test("delete-listing freezes published counts into grandfathered-downloads.json", (t) => {
  const root = makeFixtureRepo({ "fixture-mod": baseModManifest("fixture-mod") });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  // Published state: downloads.json carries v1.0.0 at 77. The bucket ledger
  // additionally remembers a never-surfaced old-tag asset (v0.9.0 at 343) and
  // a pre-correction high for v1.0.0 (90) — neither may be frozen: the freeze
  // preserves the listing's PUBLISHED state only, or deprecation day shows a
  // phantom download spike (advanced-analytics 2026-08-22, busan-3 2026-09-10).
  writeFileSync(join(root, "mods", "downloads.json"), JSON.stringify({
    "fixture-mod": { "v1.0.0": 77 },
  }) + "\n", "utf-8");
  writeFileSync(join(root, "mods", "download-version-buckets.json"), JSON.stringify({
    schema_version: 1,
    updated_at: "2026-08-01T00:00:00.000Z",
    listings: {
      "fixture-mod": {
        versions: {
          "v0.9.0": { max_total_downloads: 343, buckets: {}, updated_at: "2026-08-01T00:00:00.000Z" },
          "v1.0.0": { max_total_downloads: 90, buckets: {}, updated_at: "2026-08-01T00:00:00.000Z" },
        },
      },
    },
  }) + "\n", "utf-8");

  const result = runScript("delete-listing", root, {
    ISSUE_JSON: issueJson("fixture-mod"),
    ISSUE_AUTHOR_ID: String(AUTHOR_ID),
  });
  assert.equal(result.status, 0, result.stderr);

  const grandfathered = JSON.parse(
    readFileSync(join(root, "mods", "grandfathered-downloads.json"), "utf-8"),
  ) as Record<string, Record<string, number>>;
  assert.deepEqual(grandfathered["fixture-mod"], { "v1.0.0": 77 });
});

test("delete-listing refuses to re-delete", (t) => {
  const manifest = baseModManifest("fixture-mod");
  manifest.deprecation = { since: "2026-08-01T00:00:00Z", by_github_id: AUTHOR_ID, deleted: true };
  const root = makeFixtureRepo({ "fixture-mod": manifest });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  const result = runScript("delete-listing", root, {
    ISSUE_JSON: issueJson("fixture-mod"),
    ISSUE_AUTHOR_ID: String(AUTHOR_ID),
  });
  assert.notEqual(result.status, 0);
});

// --- un-deprecate guard ---

test("un-deprecation refuses permanently deleted assets", (t) => {
  const manifest = baseModManifest("fixture-mod");
  manifest.deprecation = { since: "2026-08-01T00:00:00Z", by_github_id: AUTHOR_ID, deleted: true };
  const root = makeFixtureRepo({ "fixture-mod": manifest });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  const result = runScript("validate-undeprecate", root, {
    ISSUE_JSON: JSON.stringify({ "asset-id": "fixture-mod" }),
    ISSUE_AUTHOR_ID: String(AUTHOR_ID),
  });
  assert.notEqual(result.status, 0);
  assert.match(
    readFileSync(join(root, "scripts", "validation-error.md"), "utf-8"),
    /cannot be restored/,
  );
  assert.equal(existsSync(join(root, "mods", "fixture-mod", "grandfathered-downloads.json")), false);
});

test("a code owner may delete a listing they do not own", (t) => {
  const root = makeFixtureRepo({ "fixture-mod": baseModManifest("fixture-mod") });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  // ahkimn — see lib/maintainers.ts.
  const result = runScript("validate-delete", root, {
    ISSUE_JSON: issueJson("fixture-mod"),
    ISSUE_AUTHOR_ID: "19807509",
  });
  assert.equal(result.status, 0, result.stderr);
});

test("a code owner still has to acknowledge permanence", (t) => {
  const root = makeFixtureRepo({ "fixture-mod": baseModManifest("fixture-mod") });
  t.after(() => rmSync(root, { recursive: true, force: true }));

  // The override covers ownership only; every other guard still applies.
  const result = runScript("validate-delete", root, {
    ISSUE_JSON: issueJson("fixture-mod", { confirmed: false }),
    ISSUE_AUTHOR_ID: "19807509",
  });
  assert.notEqual(result.status, 0);
});
