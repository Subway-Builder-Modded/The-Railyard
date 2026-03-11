import test from "node:test";
import assert from "node:assert/strict";
import { gzipSync } from "node:zlib";
import JSZip from "jszip";
import { extractDemandStatsFromZipBuffer } from "../lib/map-demand-stats.js";

async function makeZipBuffer(fileName: string, content: Buffer | string): Promise<Buffer> {
  const zip = new JSZip();
  zip.file(fileName, content);
  return zip.generateAsync({ type: "nodebuffer" });
}

test("extractDemandStatsFromZipBuffer parses demand_data.json", async () => {
  const payload = {
    points: {
      a: { residents: 10, jobs: 1 },
      b: { residents: 15, jobs: 2 },
    },
    pops_map: {
      p1: { size: 1 },
      p2: { size: 1 },
      p3: { size: 1 },
    },
  };

  const zipBuffer = await makeZipBuffer("demand_data.json", JSON.stringify(payload));
  const stats = await extractDemandStatsFromZipBuffer("sample-map", zipBuffer);

  assert.deepEqual(stats, {
    residents_total: 25,
    points_count: 2,
    population_count: 3,
  });
});

test("extractDemandStatsFromZipBuffer parses demand_data.json.gz", async () => {
  const payload = {
    points: {
      a: { residents: 7 },
      b: { residents: 8 },
      c: { residents: 9 },
    },
    pops_map: {
      p1: { size: 1 },
    },
  };

  const compressed = gzipSync(Buffer.from(JSON.stringify(payload), "utf-8"));
  const zipBuffer = await makeZipBuffer("demand_data.json.gz", compressed);
  const stats = await extractDemandStatsFromZipBuffer("sample-map", zipBuffer);

  assert.deepEqual(stats, {
    residents_total: 24,
    points_count: 3,
    population_count: 1,
  });
});

test("extractDemandStatsFromZipBuffer derives residents from popIds when residents is missing", async () => {
  const payload = {
    points: [
      { id: "p1", popIds: ["a", "b"] },
      { id: "p2", popIds: ["c"] },
    ],
    pops: [
      { id: "a", size: 3 },
      { id: "b", size: 4 },
      { id: "c", size: 5 },
    ],
  };

  const zipBuffer = await makeZipBuffer("demand_data.json", JSON.stringify(payload));
  const stats = await extractDemandStatsFromZipBuffer("sample-map", zipBuffer);

  assert.deepEqual(stats, {
    residents_total: 12,
    points_count: 2,
    population_count: 3,
  });
});

test("extractDemandStatsFromZipBuffer does not mix residents fallback with explicit residents values", async () => {
  const payload = {
    points: [
      { id: "p1", residents: 10, popIds: ["a"] },
      { id: "p2", popIds: ["b"] },
    ],
    pops: [
      { id: "a", size: 10 },
      { id: "b", size: 50 },
    ],
  };

  const zipBuffer = await makeZipBuffer("demand_data.json", JSON.stringify(payload));
  const stats = await extractDemandStatsFromZipBuffer("sample-map", zipBuffer);

  assert.deepEqual(stats, {
    residents_total: 10,
    points_count: 2,
    population_count: 2,
  });
});

test("extractDemandStatsFromZipBuffer rejects negative residents values", async () => {
  const payload = {
    points: {
      a: { residents: -3 },
      b: { residents: 7 },
    },
    pops_map: {
      p1: { size: 1 },
    },
  };

  const zipBuffer = await makeZipBuffer("demand_data.json", JSON.stringify(payload));
  await assert.rejects(
    extractDemandStatsFromZipBuffer("sample-map", zipBuffer),
    /demand point 'a' has negative residents value/,
  );
});

test("extractDemandStatsFromZipBuffer rejects negative population size using population id", async () => {
  const payload = {
    points: [
      { id: "point-a", residents: 10 },
      { id: "point-b", residents: 5 },
    ],
    pops: [
      { id: "pop-1329", size: -10 },
    ],
  };

  const zipBuffer = await makeZipBuffer("demand_data.json", JSON.stringify(payload));
  await assert.rejects(
    extractDemandStatsFromZipBuffer("sample-map", zipBuffer),
    /population entry 'pop-1329' has negative size value/,
  );
});

test("extractDemandStatsFromZipBuffer rejects malformed payloads", async () => {
  const badPayload = { points: "invalid", pops_map: {} };
  const zipBuffer = await makeZipBuffer("demand_data.json", JSON.stringify(badPayload));

  await assert.rejects(
    extractDemandStatsFromZipBuffer("sample-map", zipBuffer),
    /missing collection field 'points'/,
  );
});
