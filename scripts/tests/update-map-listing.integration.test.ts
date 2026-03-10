import test from "node:test";
import assert from "node:assert/strict";
import type { MapManifest } from "../lib/manifests.js";
import {
  applyMapManifestUpdates,
  validateMapUpdateFields,
} from "../lib/map-update-logic.js";

function makeBaseManifest(): MapManifest {
  return {
    schema_version: 2,
    id: "sample-map",
    name: "Sample Map",
    author: "Example Author",
    github_id: 12345,
    description: "Sample description",
    tags: ["north-america", "airports"],
    gallery: ["gallery/1.webp"],
    source: "https://example.com",
    update: { type: "github", repo: "owner/repo" },
    city_code: "NYC",
    country: "United States",
    population: 1000000,
    data_source: "Census",
    source_quality: "medium-quality",
    level_of_detail: "medium-detail",
    location: "north-america",
    special_demand: ["airports"],
  };
}

test("map update applies only provided fields", () => {
  const manifest = makeBaseManifest();
  const before = makeBaseManifest();

  applyMapManifestUpdates(manifest, {
    source_quality: "low-quality",
    data_source: "No change",
    level_of_detail: "_No response_",
    location: "None",
    special_demand: "_No response_",
    country: "",
  });

  assert.equal(manifest.source_quality, "low-quality");
  assert.equal(manifest.id, before.id);
  assert.equal(manifest.city_code, before.city_code);
  assert.equal(manifest.country, before.country);
  assert.equal(manifest.population, before.population);
  assert.equal(manifest.data_source, before.data_source);
  assert.equal(manifest.level_of_detail, before.level_of_detail);
  assert.equal(manifest.location, before.location);
  assert.deepEqual(manifest.special_demand, before.special_demand);
  assert.deepEqual(manifest.tags, before.tags);
});

test("map update ignores non-registry methodology field", () => {
  const manifest = makeBaseManifest();

  applyMapManifestUpdates(manifest, {
    methodology: "Synthetic example methodology text",
  });

  assert.equal(Object.hasOwn(manifest, "methodology"), false);
});

test("map update validation fails when existing manifest is invalid", () => {
  const invalidManifest = {
    ...makeBaseManifest(),
    source_quality: "",
    special_demand: "airports",
  } as unknown as MapManifest;
  const errors: string[] = [];

  validateMapUpdateFields(invalidManifest, {}, errors);

  assert.ok(
    errors.some((error) => error.includes("manifest.source_quality")),
  );
  assert.ok(
    errors.some((error) => error.includes("manifest.special_demand")),
  );
});
