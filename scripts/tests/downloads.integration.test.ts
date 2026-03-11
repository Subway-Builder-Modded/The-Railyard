import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
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

test("custom update versions map to GitHub asset download counts", async () => {
  await withTempRegistry(async ({ repoRoot, writeIndex, writeManifest }) => {
    writeIndex("mods", ["example-mod"]);
    writeIndex("maps", []);
    writeManifest("mods", "example-mod", {
      ...makeBaseModManifest("example-mod"),
      update: { type: "custom", url: "https://example.com/update.json" },
    });

    const fetchMock = makeFetchRouter([
      {
        match: (url) => url === "https://example.com/update.json",
        handle: () => jsonResponse({
          schema_version: 1,
          versions: [
            {
              version: "2.0.0",
              download: "https://github.com/Acme/CityPack/releases/download/v2.0.0/citypack-v2.zip",
              sha256: "abc",
            },
            {
              version: "1.0.0",
              download: "https://github.com/Acme/CityPack/releases/download/v1.0.0/citypack-v1.zip",
              sha256: "def",
            },
          ],
        }),
      },
      {
        match: (url) => url === "https://api.github.com/graphql",
        handle: (_input, init) => {
          const body = JSON.parse(String(init?.body)) as { variables: { owner: string; name: string; cursor: string | null } };
          assert.equal(body.variables.owner, "acme");
          assert.equal(body.variables.name, "citypack");
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
                        { name: "citypack-v2.zip", downloadCount: 9 },
                      ],
                      pageInfo: { hasNextPage: false, endCursor: null },
                    },
                  },
                  {
                    tagName: "v1.0.0",
                    releaseAssets: {
                      nodes: [
                        { name: "citypack-v1.zip", downloadCount: 4 },
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

    const { downloads, warnings } = await generateDownloadsData({
      repoRoot,
      listingType: "mod",
      fetchImpl: fetchMock,
      token: "test-token",
    });

    assert.deepEqual(warnings, []);
    assert.deepEqual(downloads, {
      "example-mod": {
        "1.0.0": 4,
        "2.0.0": 9,
      },
    });
  });
});

test("partial failures continue and emit warnings while preserving valid counts", async () => {
  await withTempRegistry(async ({ repoRoot, writeIndex, writeManifest }) => {
    // Intentionally unsorted IDs to verify deterministic sort in output.
    writeIndex("mods", ["c-github-unavailable", "b-custom", "a-github"]);
    writeIndex("maps", []);

    writeManifest("mods", "a-github", {
      ...makeBaseModManifest("a-github"),
      update: { type: "github", repo: "owner/good" },
    });
    writeManifest("mods", "b-custom", {
      ...makeBaseModManifest("b-custom"),
      update: { type: "custom", url: "https://example.com/custom-update.json" },
    });
    writeManifest("mods", "c-github-unavailable", {
      ...makeBaseModManifest("c-github-unavailable"),
      update: { type: "github", repo: "owner/bad" },
    });

    const fetchMock = makeFetchRouter([
      {
        match: (url) => url === "https://example.com/custom-update.json",
        handle: () => jsonResponse({
          schema_version: 1,
          versions: [
            {
              version: "1.0.0",
              download: "https://github.com/owner/good/releases/download/v1.0.0/good.zip",
              sha256: "a",
            },
            {
              version: "1.1.0",
              download: "https://example.com/non-github.zip",
              sha256: "b",
            },
            {
              version: "1.2.0",
              download: "https://github.com/owner/good/releases/download/v1.0.0/missing.zip",
              sha256: "c",
            },
          ],
        }),
      },
      {
        match: (url) => url === "https://api.github.com/graphql",
        handle: (_input, init) => {
          const body = JSON.parse(String(init?.body)) as { variables: { owner: string; name: string } };
          if (body.variables.owner === "owner" && body.variables.name === "bad") {
            return new Response("{}", { status: 500 });
          }
          if (body.variables.owner === "owner" && body.variables.name === "good") {
            return jsonResponse({
            data: {
              repository: {
                releases: {
                  nodes: [
                  {
                    tagName: "v1.0.0",
                    releaseAssets: {
                      nodes: [
                        { name: "good.zip", downloadCount: 15 },
                        { name: "readme.txt", downloadCount: 200 },
                      ],
                      pageInfo: { hasNextPage: false, endCursor: null },
                    },
                  },
                  {
                    tagName: "v2.0.0",
                    releaseAssets: {
                      nodes: [
                        { name: "good-v2.zip", downloadCount: 0 },
                      ],
                      pageInfo: { hasNextPage: false, endCursor: null },
                    },
                  },
                  {
                    tagName: "latest",
                    releaseAssets: {
                      nodes: [
                        { name: "good.zip", downloadCount: 999 },
                        ],
                        pageInfo: { hasNextPage: false, endCursor: null },
                      },
                    },
                  ],
                  pageInfo: { hasNextPage: false, endCursor: null },
                },
              },
              rateLimit: {
                remaining: 150,
                cost: 1,
                resetAt: "2026-03-10T00:00:00Z",
              },
            },
            });
          }
          throw new Error(`Unexpected GraphQL variables: ${JSON.stringify(body.variables)}`);
        },
      },
    ]);

    const { downloads, warnings } = await generateDownloadsData({
      repoRoot,
      listingType: "mod",
      fetchImpl: fetchMock,
    });

    assert.deepEqual(Object.keys(downloads), ["a-github", "b-custom", "c-github-unavailable"]);
    assert.deepEqual(downloads["a-github"], { "v1.0.0": 15, "v2.0.0": 0 });
    assert.deepEqual(downloads["b-custom"], { "1.0.0": 15 });
    assert.deepEqual(downloads["c-github-unavailable"], {});
    assert.ok(
      warnings.some((warning) => warning.includes("version=1.1.0") && warning.includes("non-GitHub")),
    );
    assert.ok(
      warnings.some((warning) => warning.includes("version=1.2.0") && warning.includes("missing.zip")),
    );
    assert.ok(
      warnings.some((warning) => warning.includes("repo=owner/bad") && warning.includes("HTTP 500")),
    );
    assert.ok(
      warnings.some((warning) => warning.includes("GraphQL rate limit low: remaining=150")),
    );
    assert.ok(
      warnings.some((warning) => warning.includes("a-github") && warning.includes("non-semver release tag 'latest'")),
    );
  });
});
