import test from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, unlinkSync } from "node:fs";
import { resolve } from "node:path";
import { spawnSync, type SpawnSyncReturns } from "node:child_process";
import JSZip from "jszip";

const scriptsRoot = resolve(import.meta.dirname, "..", "..");
const repoRoot = resolve(scriptsRoot, "..");
const validationErrorPath = resolve(repoRoot, "scripts", "validation-error.md");

function cleanupValidationErrorFile(): void {
  if (existsSync(validationErrorPath)) {
    unlinkSync(validationErrorPath);
  }
}

function runScript(
  scriptName: "validate-publish" | "validate-update",
  env: Record<string, string>,
): SpawnSyncReturns<string> {
  cleanupValidationErrorFile();
  const compiledScriptPath = resolve(scriptsRoot, ".test-dist", `${scriptName}.js`);
  return spawnSync(
    process.execPath,
    [compiledScriptPath],
    {
      cwd: repoRoot,
      env: {
        ...process.env,
        RAILYARD_REPO_ROOT: repoRoot,
        ...env,
      },
      encoding: "utf-8",
    },
  );
}

function readValidationError(): string {
  return existsSync(validationErrorPath)
    ? readFileSync(validationErrorPath, "utf-8")
    : "";
}

function basePublishMapIssue(overrides: Record<string, string>): Record<string, string> {
  return {
    "map-id": `zz-test-map-${Date.now()}-${Math.floor(Math.random() * 10000)}`,
    name: "Validation Test Map",
    "city-code": "RDU",
    country: "US",
    description: "Validation test payload.",
    data_source: "OSM",
    source_quality: "low-quality",
    level_of_detail: "low-detail",
    methodology: "Generated for validation tests.",
    location: "north-america",
    source: "https://example.com/test-map",
    "update-type": "GitHub Releases",
    "github-repo": "invalid-repo-format",
    gallery: "https://example.com/screenshot.png",
    ...overrides,
  };
}

async function makeZipDataUrl(payload: unknown): Promise<string> {
  const zip = new JSZip();
  zip.file("demand_data.json", JSON.stringify(payload));
  const zipBuffer = await zip.generateAsync({ type: "nodebuffer" });
  return `data:application/zip;base64,${zipBuffer.toString("base64")}`;
}

test("publish validation rejects city codes that clash with vanilla maps", () => {
  const issue = basePublishMapIssue({ "city-code": "NYC" });
  const result = runScript("validate-publish", {
    LISTING_TYPE: "map",
    ISSUE_JSON: JSON.stringify(issue),
  });

  assert.notEqual(result.status, 0, "Validation should fail for vanilla city code");
  const output = readValidationError();
  assert.match(output, /\*\*city-code\*\*: `NYC` clashes with a vanilla city code\./);
});

test("publish validation enforces ISO country code format (2 uppercase letters)", () => {
  const issue = basePublishMapIssue({ country: "uS" });
  const result = runScript("validate-publish", {
    LISTING_TYPE: "map",
    ISSUE_JSON: JSON.stringify(issue),
  });

  assert.notEqual(result.status, 0, "Validation should fail for invalid country format");
  const output = readValidationError();
  assert.match(output, /\*\*country\*\*: Country must be a 2-letter ISO 3166-1 alpha-2 code/);
});

test("update validation rejects map updates when map ID does not exist", () => {
  const issue = {
    "map-id": `zz-missing-map-${Date.now()}-${Math.floor(Math.random() * 10000)}`,
  };
  const result = runScript("validate-update", {
    LISTING_TYPE: "map",
    ISSUE_AUTHOR_ID: "1",
    ISSUE_JSON: JSON.stringify(issue),
  });

  assert.notEqual(result.status, 0, "Validation should fail for missing map ID");
  const output = readValidationError();
  assert.match(output, /\*\*map-id\*\*: No map with ID `.*` exists in the registry\./);
});

test("publish validation rejects map demand data with negative population size", async () => {
  const zipUrl = await makeZipDataUrl({
    points: [{ id: "p1", residents: 10 }],
    pops: [{ id: "pop-1329", size: -5 }],
  });
  const customUpdateJson = {
    schema_version: 1,
    versions: [
      {
        version: "1.0.0",
        game_version: "1.0.0",
        date: "2026-03-12",
        download: zipUrl,
        sha256: "deadbeef",
      },
    ],
  };
  const customUpdateUrl = `data:application/json,${encodeURIComponent(JSON.stringify(customUpdateJson))}`;
  const issue = basePublishMapIssue({
    "update-type": "Custom URL",
    "custom-update-url": customUpdateUrl,
  });
  delete issue["github-repo"];

  const result = runScript("validate-publish", {
    LISTING_TYPE: "map",
    ISSUE_JSON: JSON.stringify(issue),
  });

  assert.notEqual(result.status, 0, "Validation should fail for negative population size in demand_data");
  const output = readValidationError();
  assert.match(output, /\*\*demand_data\*\*: population entry 'pop-1329' has negative size value/);
});
