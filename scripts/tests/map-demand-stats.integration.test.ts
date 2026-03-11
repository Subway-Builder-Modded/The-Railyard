import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import JSZip from "jszip";
import { generateMapDemandStats } from "../lib/map-demand-stats.js";

type FetchRoute = {
  match: (url: string) => boolean;
  handle: (input: RequestInfo | URL, init?: RequestInit) => Response | Promise<Response>;
};

function makeFetchRouter(routes: FetchRoute[]): typeof fetch {
  return (async (input, init) => {
    const url = String(input);
    const route = routes.find((entry) => entry.match(url));
    if (!route) {
      throw new Error(`Unexpected URL: ${url}`);
    }
    return route.handle(input, init);
  }) as typeof fetch;
}

function writeJson(path: string, value: unknown): void {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, "utf-8");
}

async function makeDemandZip(residents: number[]): Promise<Buffer> {
  const points: Record<string, { residents: number }> = {};
  const popsMap: Record<string, { size: number }> = {};
  residents.forEach((value, index) => {
    points[`pt${index + 1}`] = { residents: value };
    popsMap[`pop${index + 1}`] = { size: 1 };
  });
  const payload = { points, pops_map: popsMap };
  const zip = new JSZip();
  zip.file("demand_data.json", JSON.stringify(payload));
  return zip.generateAsync({ type: "nodebuffer" });
}

test("generateMapDemandStats updates manifests for github/custom install targets", async () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-map-demand-integration-"));
  mkdirSync(join(repoRoot, "maps"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "github-map"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "custom-map"), { recursive: true });

  writeJson(join(repoRoot, "maps", "index.json"), {
    schema_version: 1,
    maps: ["github-map", "custom-map"],
  });

  writeJson(join(repoRoot, "maps", "github-map", "manifest.json"), {
    schema_version: 1,
    id: "github-map",
    name: "GitHub Map",
    author: "test",
    github_id: 1,
    description: "desc",
    tags: ["europe"],
    gallery: ["gallery/a.png"],
    source: "https://github.com/owner/repo",
    update: { type: "github", repo: "owner/repo" },
    city_code: "ABC",
    country: "US",
    population: 1,
    residents_total: 1,
    points_count: 1,
    population_count: 1,
    data_source: "LODES",
    source_quality: "medium-quality",
    level_of_detail: "medium-detail",
    location: "europe",
    special_demand: [],
  });

  writeJson(join(repoRoot, "maps", "custom-map", "manifest.json"), {
    schema_version: 1,
    id: "custom-map",
    name: "Custom Map",
    author: "test",
    github_id: 1,
    description: "desc",
    tags: ["europe"],
    gallery: ["gallery/a.png"],
    source: "https://example.com/source",
    update: { type: "custom", url: "https://example.com/update.json" },
    city_code: "DEF",
    country: "US",
    population: 2,
    residents_total: 2,
    points_count: 1,
    population_count: 2,
    data_source: "LODES",
    source_quality: "medium-quality",
    level_of_detail: "medium-detail",
    location: "europe",
    special_demand: [],
  });

  const githubZip = await makeDemandZip([11, 22, 33]);
  const customZip = await makeDemandZip([5, 6]);

  const fetchMock = makeFetchRouter([
    {
      match: (url) => url === "https://api.github.com/graphql",
      handle: () => new Response(JSON.stringify({
        data: {
          repository: {
            releases: {
              nodes: [
                {
                  tagName: "v1.0.0",
                  releaseAssets: {
                    nodes: [
                      {
                        name: "map.zip",
                        downloadCount: 10,
                        downloadUrl: "https://downloads.example.com/github-map.zip",
                      },
                    ],
                    pageInfo: { hasNextPage: false, endCursor: null },
                  },
                },
              ],
              pageInfo: { hasNextPage: false, endCursor: null },
            },
          },
          rateLimit: {
            remaining: 4999,
            cost: 1,
            resetAt: "2026-03-11T00:00:00Z",
          },
        },
      })),
    },
    {
      match: (url) => url === "https://downloads.example.com/github-map.zip",
      handle: () => new Response(new Uint8Array(githubZip)),
    },
    {
      match: (url) => url === "https://example.com/update.json",
      handle: () => new Response(JSON.stringify({
        schema_version: 1,
        versions: [
          {
            version: "1.0.0",
            download: "https://downloads.example.com/custom-map.zip",
          },
        ],
      })),
    },
    {
      match: (url) => url === "https://downloads.example.com/custom-map.zip",
      handle: () => new Response(new Uint8Array(customZip)),
    },
  ]);

  try {
    const result = await generateMapDemandStats({
      repoRoot,
      fetchImpl: fetchMock,
      token: "test-token",
    });

    assert.equal(result.processedMaps, 2);
    assert.equal(result.updatedMaps, 2);
    assert.equal(result.skippedMaps, 0);
    assert.equal(result.skippedUnchanged, 0);
    assert.equal(result.extractionFailures, 0);
    assert.equal(result.residentsDeltaTotal, 74);
    assert.deepEqual(result.warnings, []);

    const githubManifest = JSON.parse(readFileSync(join(repoRoot, "maps", "github-map", "manifest.json"), "utf-8"));
    assert.equal(githubManifest.population, 66);
    assert.equal(githubManifest.residents_total, 66);
    assert.equal(githubManifest.points_count, 3);
    assert.equal(githubManifest.population_count, 3);

    const customManifest = JSON.parse(readFileSync(join(repoRoot, "maps", "custom-map", "manifest.json"), "utf-8"));
    assert.equal(customManifest.population, 11);
    assert.equal(customManifest.residents_total, 11);
    assert.equal(customManifest.points_count, 2);
    assert.equal(customManifest.population_count, 2);
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("generateMapDemandStats skips failed maps and keeps existing manifests", async () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-map-demand-skip-"));
  mkdirSync(join(repoRoot, "maps"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "broken-map"), { recursive: true });

  writeJson(join(repoRoot, "maps", "index.json"), {
    schema_version: 1,
    maps: ["broken-map"],
  });
  writeJson(join(repoRoot, "maps", "broken-map", "manifest.json"), {
    schema_version: 1,
    id: "broken-map",
    name: "Broken",
    author: "test",
    github_id: 1,
    description: "desc",
    tags: ["europe"],
    gallery: ["gallery/a.png"],
    source: "https://example.com/source",
    update: { type: "custom", url: "https://example.com/broken-update.json" },
    city_code: "BROK",
    country: "US",
    population: 12,
    residents_total: 12,
    points_count: 3,
    population_count: 12,
    data_source: "LODES",
    source_quality: "medium-quality",
    level_of_detail: "medium-detail",
    location: "europe",
    special_demand: [],
  });

  const fetchMock = makeFetchRouter([
    {
      match: (url) => url === "https://example.com/broken-update.json",
      handle: () => new Response("{}", { status: 200 }),
    },
  ]);

  try {
    const result = await generateMapDemandStats({
      repoRoot,
      fetchImpl: fetchMock,
    });

    assert.equal(result.processedMaps, 1);
    assert.equal(result.updatedMaps, 0);
    assert.equal(result.skippedMaps, 1);
    assert.equal(result.skippedUnchanged, 0);
    assert.equal(result.extractionFailures, 1);
    assert.ok(
      result.warnings.some((warning) => warning.includes("broken-map") && warning.includes("versions array")),
    );

    const manifest = JSON.parse(readFileSync(join(repoRoot, "maps", "broken-map", "manifest.json"), "utf-8"));
    assert.equal(manifest.population, 12);
    assert.equal(manifest.residents_total, 12);
    assert.equal(manifest.points_count, 3);
    assert.equal(manifest.population_count, 12);
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("generateMapDemandStats skips unchanged sha fingerprint regardless of cache age", async () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-map-demand-cache-"));
  mkdirSync(join(repoRoot, "maps"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "cached-map"), { recursive: true });

  writeJson(join(repoRoot, "maps", "index.json"), {
    schema_version: 1,
    maps: ["cached-map"],
  });
  writeJson(join(repoRoot, "maps", "cached-map", "manifest.json"), {
    schema_version: 1,
    id: "cached-map",
    name: "Cached",
    author: "test",
    github_id: 1,
    description: "desc",
    tags: ["europe"],
    gallery: ["gallery/a.png"],
    source: "https://example.com/source",
    update: { type: "custom", url: "https://example.com/cached-update.json" },
    city_code: "CACH",
    country: "US",
    population: 12,
    residents_total: 12,
    points_count: 3,
    population_count: 12,
    data_source: "LODES",
    source_quality: "medium-quality",
    level_of_detail: "medium-detail",
    location: "europe",
    special_demand: [],
  });
  writeJson(join(repoRoot, "maps", "demand-stats-cache.json"), {
    "cached-map": {
      source_fingerprint: "sha256:abc123",
      last_checked_at: new Date(Date.now() - (48 * 60 * 60 * 1000)).toISOString(),
      stats: {
        residents_total: 12,
        points_count: 3,
        population_count: 12,
      },
    },
  });

  const fetchMock = makeFetchRouter([
    {
      match: (url) => url === "https://example.com/cached-update.json",
      handle: () => new Response(JSON.stringify({
        schema_version: 1,
        versions: [
          {
            version: "1.0.0",
            sha256: "abc123",
            download: "https://downloads.example.com/should-not-be-fetched.zip",
          },
        ],
      })),
    },
  ]);

  try {
    const result = await generateMapDemandStats({
      repoRoot,
      fetchImpl: fetchMock,
    });

    assert.equal(result.processedMaps, 1);
    assert.equal(result.updatedMaps, 0);
    assert.equal(result.skippedMaps, 1);
    assert.equal(result.skippedUnchanged, 1);
    assert.equal(result.extractionFailures, 0);
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("generateMapDemandStats recomputes unchanged sha fingerprint when cache has no stats", async () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-map-demand-legacy-cache-"));
  mkdirSync(join(repoRoot, "maps"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "legacy-map"), { recursive: true });

  writeJson(join(repoRoot, "maps", "index.json"), {
    schema_version: 1,
    maps: ["legacy-map"],
  });
  writeJson(join(repoRoot, "maps", "legacy-map", "manifest.json"), {
    schema_version: 1,
    id: "legacy-map",
    name: "Legacy",
    author: "test",
    github_id: 1,
    description: "desc",
    tags: ["europe"],
    gallery: ["gallery/a.png"],
    source: "https://example.com/source",
    update: { type: "custom", url: "https://example.com/legacy-update.json" },
    city_code: "LEGC",
    country: "US",
    population: 100,
    residents_total: 100,
    points_count: 1,
    population_count: 1,
    data_source: "LODES",
    source_quality: "medium-quality",
    level_of_detail: "medium-detail",
    location: "europe",
    special_demand: [],
  });
  writeJson(join(repoRoot, "maps", "demand-stats-cache.json"), {
    "legacy-map": {
      source_fingerprint: "sha256:abc123",
      last_checked_at: new Date().toISOString(),
    },
  });

  const legacyZip = await makeDemandZip([7, 8, 9]);
  const fetchMock = makeFetchRouter([
    {
      match: (url) => url === "https://example.com/legacy-update.json",
      handle: () => new Response(JSON.stringify({
        schema_version: 1,
        versions: [
          {
            version: "1.0.0",
            sha256: "abc123",
            download: "https://downloads.example.com/legacy-map.zip",
          },
        ],
      })),
    },
    {
      match: (url) => url === "https://downloads.example.com/legacy-map.zip",
      handle: () => new Response(new Uint8Array(legacyZip)),
    },
  ]);

  try {
    const result = await generateMapDemandStats({
      repoRoot,
      fetchImpl: fetchMock,
    });

    assert.equal(result.processedMaps, 1);
    assert.equal(result.updatedMaps, 1);
    assert.equal(result.skippedMaps, 0);
    assert.equal(result.skippedUnchanged, 0);
    assert.equal(result.extractionFailures, 0);

    const manifest = JSON.parse(readFileSync(join(repoRoot, "maps", "legacy-map", "manifest.json"), "utf-8"));
    assert.equal(manifest.population, 24);
    assert.equal(manifest.residents_total, 24);
    assert.equal(manifest.points_count, 3);
    assert.equal(manifest.population_count, 3);
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("generateMapDemandStats force mode bypasses unchanged sha cache checks", async () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-map-demand-force-"));
  mkdirSync(join(repoRoot, "maps"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "force-map"), { recursive: true });

  writeJson(join(repoRoot, "maps", "index.json"), {
    schema_version: 1,
    maps: ["force-map"],
  });
  writeJson(join(repoRoot, "maps", "force-map", "manifest.json"), {
    schema_version: 1,
    id: "force-map",
    name: "Force",
    author: "test",
    github_id: 1,
    description: "desc",
    tags: ["europe"],
    gallery: ["gallery/a.png"],
    source: "https://example.com/source",
    update: { type: "custom", url: "https://example.com/force-update.json" },
    city_code: "FORC",
    country: "US",
    population: 5,
    residents_total: 5,
    points_count: 1,
    population_count: 1,
    data_source: "LODES",
    source_quality: "medium-quality",
    level_of_detail: "medium-detail",
    location: "europe",
    special_demand: [],
  });
  writeJson(join(repoRoot, "maps", "demand-stats-cache.json"), {
    "force-map": {
      source_fingerprint: "sha256:abc123",
      last_checked_at: new Date().toISOString(),
    },
  });

  const forceZip = await makeDemandZip([7, 8, 9]);
  const fetchMock = makeFetchRouter([
    {
      match: (url) => url === "https://example.com/force-update.json",
      handle: () => new Response(JSON.stringify({
        schema_version: 1,
        versions: [
          {
            version: "1.0.0",
            sha256: "abc123",
            download: "https://downloads.example.com/force-map.zip",
          },
        ],
      })),
    },
    {
      match: (url) => url === "https://downloads.example.com/force-map.zip",
      handle: () => new Response(new Uint8Array(forceZip)),
    },
  ]);

  try {
    const result = await generateMapDemandStats({
      repoRoot,
      fetchImpl: fetchMock,
      force: true,
    });

    assert.equal(result.processedMaps, 1);
    assert.equal(result.updatedMaps, 1);
    assert.equal(result.skippedMaps, 0);
    assert.equal(result.skippedUnchanged, 0);
    assert.equal(result.extractionFailures, 0);

    const manifest = JSON.parse(readFileSync(join(repoRoot, "maps", "force-map", "manifest.json"), "utf-8"));
    assert.equal(manifest.population, 24);
    assert.equal(manifest.residents_total, 24);
    assert.equal(manifest.points_count, 3);
    assert.equal(manifest.population_count, 3);
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("generateMapDemandStats mapId option processes only the target map", async () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-map-demand-id-"));
  mkdirSync(join(repoRoot, "maps"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "target-map"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "other-map"), { recursive: true });

  writeJson(join(repoRoot, "maps", "index.json"), {
    schema_version: 1,
    maps: ["target-map", "other-map"],
  });
  writeJson(join(repoRoot, "maps", "target-map", "manifest.json"), {
    schema_version: 1,
    id: "target-map",
    name: "Target",
    author: "test",
    github_id: 1,
    description: "desc",
    tags: ["europe"],
    gallery: ["gallery/a.png"],
    source: "https://example.com/source",
    update: { type: "custom", url: "https://example.com/target-update.json" },
    city_code: "TARG",
    country: "US",
    population: 5,
    residents_total: 5,
    points_count: 1,
    population_count: 1,
    data_source: "LODES",
    source_quality: "medium-quality",
    level_of_detail: "medium-detail",
    location: "europe",
    special_demand: [],
  });
  writeJson(join(repoRoot, "maps", "other-map", "manifest.json"), {
    schema_version: 1,
    id: "other-map",
    name: "Other",
    author: "test",
    github_id: 1,
    description: "desc",
    tags: ["europe"],
    gallery: ["gallery/a.png"],
    source: "https://example.com/source",
    update: { type: "custom", url: "https://example.com/other-update.json" },
    city_code: "OTHR",
    country: "US",
    population: 9,
    residents_total: 9,
    points_count: 1,
    population_count: 1,
    data_source: "LODES",
    source_quality: "medium-quality",
    level_of_detail: "medium-detail",
    location: "europe",
    special_demand: [],
  });

  const targetZip = await makeDemandZip([4, 5, 6]);
  const fetchMock = makeFetchRouter([
    {
      match: (url) => url === "https://example.com/target-update.json",
      handle: () => new Response(JSON.stringify({
        schema_version: 1,
        versions: [
          {
            version: "1.0.0",
            sha256: "target-sha",
            download: "https://downloads.example.com/target-map.zip",
          },
        ],
      })),
    },
    {
      match: (url) => url === "https://downloads.example.com/target-map.zip",
      handle: () => new Response(new Uint8Array(targetZip)),
    },
  ]);

  try {
    const result = await generateMapDemandStats({
      repoRoot,
      fetchImpl: fetchMock,
      mapId: "target-map",
    });

    assert.equal(result.processedMaps, 1);
    assert.equal(result.updatedMaps, 1);
    assert.equal(result.skippedMaps, 0);
    assert.equal(result.extractionFailures, 0);

    const targetManifest = JSON.parse(readFileSync(join(repoRoot, "maps", "target-map", "manifest.json"), "utf-8"));
    assert.equal(targetManifest.population, 15);
    assert.equal(targetManifest.residents_total, 15);

    const otherManifest = JSON.parse(readFileSync(join(repoRoot, "maps", "other-map", "manifest.json"), "utf-8"));
    assert.equal(otherManifest.population, 9);
    assert.equal(otherManifest.residents_total, 9);
    assert.equal(otherManifest.points_count, 1);
    assert.equal(otherManifest.population_count, 1);
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});
