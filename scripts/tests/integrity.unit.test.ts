import test from "node:test";
import assert from "node:assert/strict";
import JSZip from "jszip";
import { inspectZipCompleteness } from "../lib/integrity.js";

async function makeZip(entries: Record<string, string>): Promise<Buffer> {
  const zip = new JSZip();
  for (const [name, content] of Object.entries(entries)) {
    zip.file(name, content);
  }
  return zip.generateAsync({ type: "nodebuffer" });
}

test("map integrity requires exact top-level files including city pmtiles", async () => {
  const zipBuffer = await makeZip({
    "config.json": "{}",
    "demand_data.json.gz": "stub",
    "buildings_index.json": "{}",
    "roads.geojson": "{}",
    "runways_taxiways.geojson.gz": "stub",
    "ABC.pmtiles": "stub",
  });

  const result = await inspectZipCompleteness("map", zipBuffer, { cityCode: "ABC" });
  assert.equal(result.isComplete, true);
  assert.equal(result.requiredChecks.config_json, true);
  assert.equal(result.requiredChecks.demand_data, true);
  assert.equal(result.requiredChecks.buildings_index, true);
  assert.equal(result.requiredChecks.roads_geojson, true);
  assert.equal(result.requiredChecks.runways_taxiways_geojson, true);
  assert.equal(result.requiredChecks.city_pmtiles, true);
});

test("map integrity rejects nested paths and missing top-level city pmtiles", async () => {
  const zipBuffer = await makeZip({
    "nested/config.json": "{}",
    "demand_data.json": "{}",
    "buildings_index.json": "{}",
    "roads.geojson": "{}",
    "runways_taxiways.geojson": "{}",
    "nested/ABC.pmtiles": "stub",
  });

  const result = await inspectZipCompleteness("map", zipBuffer, { cityCode: "ABC" });
  assert.equal(result.isComplete, false);
  assert.ok(result.errors.some((error) => error.includes("config.json")));
  assert.ok(result.errors.some((error) => error.includes("ABC.pmtiles")));
});

test("mod integrity requires both release manifest asset and top-level zip manifest", async () => {
  const zipBuffer = await makeZip({
    "manifest.json": "{}",
    "plugin.dll": "binary",
  });

  const missingReleaseAsset = await inspectZipCompleteness("mod", zipBuffer, {
    releaseHasManifestAsset: false,
  });
  assert.equal(missingReleaseAsset.isComplete, false);
  assert.ok(missingReleaseAsset.errors.some((error) => error.includes("release asset manifest.json")));

  const valid = await inspectZipCompleteness("mod", zipBuffer, {
    releaseHasManifestAsset: true,
  });
  assert.equal(valid.isComplete, true);
});
