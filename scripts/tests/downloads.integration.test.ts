import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import JSZip from "jszip";
import { generateDownloadsData } from "../lib/downloads.js";

function writeJson(path: string, value: unknown): void {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, "utf-8");
}

function makeBaseModManifest(id: string): Record<string, unknown> {
  return {
    schema_version: 1,
    id,
    name: id,
    author: "test",
    github_id: 1,
    description: "desc",
    tags: [],
    gallery: [],
    source: "https://github.com/example/example",
  };
}

async function makeModZip(includeTopLevelManifest: boolean): Promise<Buffer> {
  const zip = new JSZip();
  if (includeTopLevelManifest) {
    zip.file("manifest.json", "{\"schema_version\":1}");
  }
  zip.file("mod.dll", "binary");
  return zip.generateAsync({ type: "nodebuffer" });
}

interface TempRegistryContext {
  repoRoot: string;
  writeIndex: (kind: "maps" | "mods", ids: string[]) => void;
  writeManifest: (kind: "maps" | "mods", id: string, manifest: Record<string, unknown>) => void;
}

async function withTempRegistry(
  run: (context: TempRegistryContext) => Promise<void>,
): Promise<void> {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-downloads-test-"));
  mkdirSync(join(repoRoot, "mods"), { recursive: true });
  mkdirSync(join(repoRoot, "maps"), { recursive: true });

  const context: TempRegistryContext = {
    repoRoot,
    writeIndex: (kind, ids) => {
      writeJson(join(repoRoot, kind, "index.json"), {
        schema_version: 1,
        [kind]: ids,
      });
    },
    writeManifest: (kind, id, manifest) => {
      mkdirSync(join(repoRoot, kind, id), { recursive: true });
      writeJson(join(repoRoot, kind, id, "manifest.json"), manifest);
    },
  };

  try {
    await run(context);
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
}

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

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status });
}

test("github releases are integrity-validated and filtered before download aggregation", async () => {
  await withTempRegistry(async ({ repoRoot, writeIndex, writeManifest }) => {
    writeIndex("mods", ["github-mod"]);
    writeIndex("maps", []);
    writeManifest("mods", "github-mod", {
      ...makeBaseModManifest("github-mod"),
      update: { type: "github", repo: "owner/good" },
    });

    const validZip = await makeModZip(true);
    const invalidZip = await makeModZip(false);
    const fetchMock = makeFetchRouter([
      {
        match: (url) => url === "https://downloads.example.com/good-v2.zip",
        handle: () => new Response(new Uint8Array(validZip)),
      },
      {
        match: (url) => url === "https://downloads.example.com/good-v1.zip",
        handle: () => new Response(new Uint8Array(invalidZip)),
      },
      {
        match: (url) => url === "https://api.github.com/graphql",
        handle: (_input, init) => {
          const body = JSON.parse(String(init?.body)) as { variables: { owner: string; name: string; cursor: string | null } };
          assert.equal(body.variables.owner, "owner");
          assert.equal(body.variables.name, "good");
          assert.equal(body.variables.cursor, null);
          return jsonResponse({
            data: {
              repository: {
                releases: {
                  nodes: [
                    {
                      tagName: "v2.0.0",
                      releaseAssets: {
                        nodes: [
                          { name: "good-v2.zip", downloadCount: 15, downloadUrl: "https://downloads.example.com/good-v2.zip" },
                          { name: "manifest.json", downloadCount: 30, downloadUrl: "https://downloads.example.com/manifest-v2.json" },
                        ],
                        pageInfo: { hasNextPage: false, endCursor: null },
                      },
                    },
                    {
                      tagName: "v1.0.0",
                      releaseAssets: {
                        nodes: [
                          { name: "good-v1.zip", downloadCount: 4, downloadUrl: "https://downloads.example.com/good-v1.zip" },
                          { name: "manifest.json", downloadCount: 20, downloadUrl: "https://downloads.example.com/manifest-v1.json" },
                        ],
                        pageInfo: { hasNextPage: false, endCursor: null },
                      },
                    },
                    {
                      tagName: "latest",
                      releaseAssets: {
                        nodes: [
                          { name: "good-latest.zip", downloadCount: 999, downloadUrl: "https://downloads.example.com/good-latest.zip" },
                        ],
                        pageInfo: { hasNextPage: false, endCursor: null },
                      },
                    },
                  ],
                  pageInfo: { hasNextPage: false, endCursor: null },
                },
              },
              rateLimit: {
                remaining: 120,
                cost: 1,
                resetAt: "2026-03-14T00:00:00Z",
              },
            },
          });
        },
      },
    ]);

    const { downloads, integrity, stats, warnings } = await generateDownloadsData({
      repoRoot,
      listingType: "mod",
      fetchImpl: fetchMock,
      token: "test-token",
    });

    assert.deepEqual(downloads, {
      "github-mod": {
        "v2.0.0": 15,
      },
    });
    assert.equal(stats.filtered_versions, 1);
    assert.equal(stats.complete_versions, 1);
    assert.equal(stats.incomplete_versions, 2);
    assert.equal(integrity.listings["github-mod"]?.has_complete_version, true);
    assert.equal(integrity.listings["github-mod"]?.versions["latest"]?.is_complete, false);
    assert.ok(
      warnings.some((warning) => warning.includes("v1.0.0") && warning.includes("excluded by integrity validation")),
    );
  });
});

test("custom mixed versions produce explicit invalid integrity entries and hard-filter downloads", async () => {
  await withTempRegistry(async ({ repoRoot, writeIndex, writeManifest }) => {
    writeIndex("mods", ["custom-mod"]);
    writeIndex("maps", []);
    writeManifest("mods", "custom-mod", {
      ...makeBaseModManifest("custom-mod"),
      update: { type: "custom", url: "https://example.com/custom-update.json" },
    });

    const validZip = await makeModZip(true);
    const fetchMock = makeFetchRouter([
      {
        match: (url) => url === "https://example.com/custom-update.json",
        handle: () => jsonResponse({
          schema_version: 1,
          versions: [
            {
              version: "1.0.0",
              download: "https://github.com/Owner/Good/releases/download/v1.0.0/good.zip",
              sha256: "sha-a",
            },
            {
              version: "1.1.0",
              download: "https://example.com/non-github.zip",
              sha256: "sha-b",
            },
            {
              version: "1.2.0",
              download: "https://github.com/Owner/Good/releases/download/v1.0.0/missing.zip",
              sha256: "sha-c",
            },
            {
              version: "beta",
              download: "https://github.com/Owner/Good/releases/download/latest/good.zip",
              sha256: "sha-d",
            },
          ],
        }),
      },
      {
        match: (url) => url === "https://downloads.example.com/good.zip",
        handle: () => new Response(new Uint8Array(validZip)),
      },
      {
        match: (url) => url === "https://api.github.com/graphql",
        handle: (_input, init) => {
          const body = JSON.parse(String(init?.body)) as { variables: { owner: string; name: string } };
          assert.equal(body.variables.owner, "owner");
          assert.equal(body.variables.name, "good");
          return jsonResponse({
            data: {
              repository: {
                releases: {
                  nodes: [
                    {
                      tagName: "v1.0.0",
                      releaseAssets: {
                        nodes: [
                          { name: "good.zip", downloadCount: 12, downloadUrl: "https://downloads.example.com/good.zip" },
                          { name: "manifest.json", downloadCount: 10, downloadUrl: "https://downloads.example.com/manifest.json" },
                        ],
                        pageInfo: { hasNextPage: false, endCursor: null },
                      },
                    },
                  ],
                  pageInfo: { hasNextPage: false, endCursor: null },
                },
              },
            },
          });
        },
      },
    ]);

    const { downloads, integrity, stats } = await generateDownloadsData({
      repoRoot,
      listingType: "mod",
      fetchImpl: fetchMock,
      token: "test-token",
    });

    assert.deepEqual(downloads, {
      "custom-mod": {
        "1.0.0": 12,
      },
    });
    assert.equal(stats.filtered_versions, 2);
    assert.equal(integrity.listings["custom-mod"]?.versions["1.0.0"]?.is_complete, true);
    assert.equal(integrity.listings["custom-mod"]?.versions["1.1.0"]?.is_complete, false);
    assert.equal(integrity.listings["custom-mod"]?.versions["1.2.0"]?.is_complete, false);
    assert.equal(integrity.listings["custom-mod"]?.versions["beta"]?.is_complete, false);
    assert.ok(
      (integrity.listings["custom-mod"]?.versions["beta"]?.errors ?? []).some((error) => error.includes("non-semver")),
    );
  });
});

test("non-sha integrity cache reuses recent entries without refetching ZIPs", async () => {
  await withTempRegistry(async ({ repoRoot, writeIndex, writeManifest }) => {
    writeIndex("mods", ["cache-mod"]);
    writeIndex("maps", []);
    writeManifest("mods", "cache-mod", {
      ...makeBaseModManifest("cache-mod"),
      update: { type: "github", repo: "owner/cache" },
    });

    const validZip = await makeModZip(true);
    let zipFetchCount = 0;
    const fetchMock = makeFetchRouter([
      {
        match: (url) => url === "https://downloads.example.com/cache.zip",
        handle: () => {
          zipFetchCount += 1;
          return new Response(new Uint8Array(validZip));
        },
      },
      {
        match: (url) => url === "https://api.github.com/graphql",
        handle: () => jsonResponse({
          data: {
            repository: {
              releases: {
                nodes: [
                  {
                    tagName: "v1.0.0",
                    releaseAssets: {
                      nodes: [
                        { name: "cache.zip", downloadCount: 3, downloadUrl: "https://downloads.example.com/cache.zip" },
                        { name: "manifest.json", downloadCount: 3, downloadUrl: "https://downloads.example.com/cache-manifest.json" },
                      ],
                      pageInfo: { hasNextPage: false, endCursor: null },
                    },
                  },
                ],
                pageInfo: { hasNextPage: false, endCursor: null },
              },
            },
          },
        }),
      },
    ]);

    const first = await generateDownloadsData({
      repoRoot,
      listingType: "mod",
      fetchImpl: fetchMock,
      token: "test-token",
    });
    assert.equal(zipFetchCount, 1);
    writeJson(join(repoRoot, "mods", "integrity-cache.json"), first.integrityCache);

    const second = await generateDownloadsData({
      repoRoot,
      listingType: "mod",
      fetchImpl: fetchMock,
      token: "test-token",
    });
    assert.equal(second.stats.cache_hits, 1);
    assert.equal(zipFetchCount, 1);
    assert.deepEqual(second.downloads, first.downloads);
  });
});

test("download-only mode skips ZIP inspection and keeps semver zip counts", async () => {
  await withTempRegistry(async ({ repoRoot, writeIndex, writeManifest }) => {
    writeIndex("mods", ["hourly-mod"]);
    writeIndex("maps", []);
    writeManifest("mods", "hourly-mod", {
      ...makeBaseModManifest("hourly-mod"),
      update: { type: "github", repo: "owner/hourly" },
    });

    let zipFetchCount = 0;
    const fetchMock = makeFetchRouter([
      {
        match: (url) => url === "https://downloads.example.com/hourly.zip",
        handle: () => {
          zipFetchCount += 1;
          return new Response("unexpected");
        },
      },
      {
        match: (url) => url === "https://api.github.com/graphql",
        handle: () => jsonResponse({
          data: {
            repository: {
              releases: {
                nodes: [
                  {
                    tagName: "v1.0.0",
                    releaseAssets: {
                      nodes: [
                        { name: "hourly.zip", downloadCount: 7, downloadUrl: "https://downloads.example.com/hourly.zip" },
                        { name: "manifest.json", downloadCount: 7, downloadUrl: "https://downloads.example.com/hourly-manifest.json" },
                      ],
                      pageInfo: { hasNextPage: false, endCursor: null },
                    },
                  },
                ],
                pageInfo: { hasNextPage: false, endCursor: null },
              },
            },
          },
        }),
      },
    ]);

    const result = await generateDownloadsData({
      repoRoot,
      listingType: "mod",
      mode: "download-only",
      fetchImpl: fetchMock,
      token: "test-token",
    });

    assert.deepEqual(result.downloads, { "hourly-mod": { "v1.0.0": 7 } });
    assert.equal(result.stats.filtered_versions, 0);
    assert.equal(zipFetchCount, 0);
  });
});
