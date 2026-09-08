import test from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { runGenerateAnalyticsCli } from "../lib/analytics-core.js";

function writeJson(path: string, value: unknown): void {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, "utf-8");
}

function readAnalyticsCsv(repoRoot: string, fileName: string): string {
  return readFileSync(join(repoRoot, "analytics", fileName), "utf-8");
}

const AUTHORS_INDEX = {
  schema_version: 1,
  authors: [
    { github_id: 100, author_id: "bquelhas", author_alias: "bquelhas", attribution_method: "github" },
    { github_id: 200, author_id: "capitao", author_alias: "Miguel Sousa", attribution_method: "github" },
  ],
};

function writePortoManifest(repoRoot: string, caretakers: unknown): void {
  writeJson(join(repoRoot, "maps", "porto", "manifest.json"), {
    schema_version: 1,
    id: "porto",
    name: "Porto",
    author: "bquelhas",
    github_id: 100,
    source: "https://github.com/example/porto",
    city_code: "OPO",
    country: "PT",
    population: 0,
    population_count: 0,
    points_count: 0,
    collaborators: [200],
    ...(caretakers === undefined ? {} : { caretakers }),
  });
}

function writePortoFixture(repoRoot: string, caretakers: unknown): void {
  mkdirSync(join(repoRoot, "history"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "porto"), { recursive: true });
  mkdirSync(join(repoRoot, "mods", "test-mod"), { recursive: true });
  mkdirSync(join(repoRoot, "authors"), { recursive: true });

  writeJson(join(repoRoot, "maps", "index.json"), { schema_version: 1, maps: ["porto"] });
  writePortoManifest(repoRoot, caretakers);
  writeJson(join(repoRoot, "maps", "integrity.json"), {
    schema_version: 1,
    generated_at: "2026-07-31T00:00:00Z",
    listings: {
      porto: {
        versions: {
          "v1.0.0": { released_at: "2026-05-01T00:00:00Z" },
          "v2.0.0": { released_at: "2026-07-01T00:00:00Z" },
        },
      },
    },
  });
  writeJson(join(repoRoot, "maps", "downloads.json"), {
    porto: { "v1.0.0": 30, "v2.0.0": 25 },
  });
  // Test listings must not appear in listing_version_credits.csv.
  writeJson(join(repoRoot, "mods", "test-mod", "manifest.json"), {
    schema_version: 1,
    id: "test-mod",
    name: "Test Mod",
    author: "tester",
    github_id: 3,
    is_test: true,
  });
  writeJson(join(repoRoot, "mods", "downloads.json"), {
    "test-mod": { "v1.0.0": 5 },
  });
  writeJson(join(repoRoot, "authors", "index.json"), AUTHORS_INDEX);

  writeJson(join(repoRoot, "history", "snapshot_2026_07_30.json"), {
    schema_version: 2,
    snapshot_date: "2026_07_30",
    generated_at: "2026-07-30T00:00:00.000Z",
    maps: { downloads: { porto: { "v1.0.0": 28, "v2.0.0": 20 } } },
    mods: { downloads: {} },
  });
  writeJson(join(repoRoot, "history", "snapshot_2026_07_31.json"), {
    schema_version: 2,
    snapshot_date: "2026_07_31",
    generated_at: "2026-07-31T00:00:00.000Z",
    maps: { downloads: { porto: { "v1.0.0": 30, "v2.0.0": 25 } } },
    mods: { downloads: {} },
  });
}

test("analytics splits a listing's credit at the caretaker window (porto invariant)", () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-credits-porto-"));
  try {
    // v1 released before the caretaker's `since`, v2 after it.
    writePortoFixture(repoRoot, [{ github_id: 200, since: "2026-06-01T00:00:00Z" }]);

    runGenerateAnalyticsCli([], repoRoot);

    assert.equal(
      readAnalyticsCsv(repoRoot, "listing_version_credits.csv"),
      [
        "listing_type,listing_id,version,credited_author_id",
        "map,porto,v1.0.0,bquelhas",
        "map,porto,v2.0.0,capitao",
        "",
      ].join("\n"),
    );

    assert.equal(
      readAnalyticsCsv(repoRoot, "authors_by_total_downloads.csv"),
      [
        "rank,author,author_alias,attribution_link,total_downloads,adjusted_total_downloads,asset_count,map_count,mod_count",
        "1,bquelhas,bquelhas,https://github.com/bquelhas,30,30,1,1,0",
        "2,capitao,Miguel Sousa,https://github.com/capitao,25,25,1,1,0",
        "",
      ].join("\n"),
    );

    // Authorship-based columns keep the FULL listing totals for the author;
    // the caretaker only appears through the appended caretaken_asset_count.
    assert.equal(
      readAnalyticsCsv(repoRoot, "authors_by_asset_count.csv"),
      [
        "rank,author,author_alias,attribution_link,asset_count,map_count,mod_count,total_downloads,adjusted_total_downloads,caretaken_asset_count",
        "1,bquelhas,bquelhas,https://github.com/bquelhas,1,1,0,55,55,0",
        "2,capitao,Miguel Sousa,https://github.com/capitao,0,0,0,0,0,1",
        "",
      ].join("\n"),
    );

    assert.equal(
      readAnalyticsCsv(repoRoot, "authors_by_day.csv"),
      [
        "author,author_alias,attribution_link,asset_count,map_count,mod_count,total_downloads,2026_07_30,2026_07_31",
        "bquelhas,bquelhas,https://github.com/bquelhas,1,1,0,30,28,2",
        "capitao,Miguel Sousa,https://github.com/capitao,1,1,0,25,20,5",
        "",
      ].join("\n"),
    );

    assert.equal(
      readAnalyticsCsv(repoRoot, "authors_last_1d.csv"),
      [
        "rank,author,author_alias,attribution_link,asset_count,map_count,mod_count,download_change,adjusted_download_change,current_total,adjusted_current_total,baseline_total,adjusted_baseline_total,latest_snapshot,baseline_snapshot,map_download_change,adjusted_map_download_change,mod_download_change,adjusted_mod_download_change",
        "1,capitao,Miguel Sousa,https://github.com/capitao,1,1,0,5,5,25,25,20,20,snapshot_2026_07_31.json,snapshot_2026_07_30.json,5,5,0,0",
        "2,bquelhas,bquelhas,https://github.com/bquelhas,1,1,0,2,2,30,30,28,28,snapshot_2026_07_31.json,snapshot_2026_07_30.json,2,2,0,0",
        "",
      ].join("\n"),
    );
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("analytics credits every version to a caretaker-since-epoch (devenperez invariant)", () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-credits-deven-"));
  try {
    writePortoFixture(repoRoot, [{ github_id: 200, since: "1970-01-01T00:00:00Z" }]);

    runGenerateAnalyticsCli([], repoRoot);

    assert.equal(
      readAnalyticsCsv(repoRoot, "listing_version_credits.csv"),
      [
        "listing_type,listing_id,version,credited_author_id",
        "map,porto,v1.0.0,capitao",
        "map,porto,v2.0.0,capitao",
        "",
      ].join("\n"),
    );

    // All download credit moves to the caretaker (listing-grain totals intact);
    // authorship (asset counts) stays with the author.
    assert.equal(
      readAnalyticsCsv(repoRoot, "authors_by_total_downloads.csv"),
      [
        "rank,author,author_alias,attribution_link,total_downloads,adjusted_total_downloads,asset_count,map_count,mod_count",
        "1,capitao,Miguel Sousa,https://github.com/capitao,55,55,1,1,0",
        "",
      ].join("\n"),
    );
    assert.equal(
      readAnalyticsCsv(repoRoot, "authors_by_asset_count.csv"),
      [
        "rank,author,author_alias,attribution_link,asset_count,map_count,mod_count,total_downloads,adjusted_total_downloads,caretaken_asset_count",
        "1,bquelhas,bquelhas,https://github.com/bquelhas,1,1,0,55,55,0",
        "2,capitao,Miguel Sousa,https://github.com/capitao,0,0,0,0,0,1",
        "",
      ].join("\n"),
    );
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("analytics output is unchanged for listings without caretakers", () => {
  const withoutCaretakers = mkdtempSync(join(tmpdir(), "railyard-credits-none-"));
  const withClosedWindow = mkdtempSync(join(tmpdir(), "railyard-credits-closed-"));
  try {
    writePortoFixture(withoutCaretakers, undefined);
    runGenerateAnalyticsCli([], withoutCaretakers);

    // A fully closed caretaker window that never covers a release behaves
    // identically to no caretakers at all (credit-wise), and the active
    // caretaker count stays zero.
    writePortoFixture(withClosedWindow, [
      { github_id: 200, since: "2020-01-01T00:00:00Z", until: "2020-02-01T00:00:00Z" },
    ]);
    runGenerateAnalyticsCli([], withClosedWindow);

    for (const fileName of [
      "authors_by_total_downloads.csv",
      "authors_by_asset_count.csv",
      "authors_by_day.csv",
      "authors_last_1d.csv",
      "authors_last_30d.csv",
    ]) {
      assert.equal(
        readAnalyticsCsv(withClosedWindow, fileName),
        readAnalyticsCsv(withoutCaretakers, fileName),
        `expected ${fileName} to match the no-caretakers output`,
      );
    }

    assert.equal(
      readAnalyticsCsv(withoutCaretakers, "authors_by_total_downloads.csv"),
      [
        "rank,author,author_alias,attribution_link,total_downloads,adjusted_total_downloads,asset_count,map_count,mod_count",
        "1,bquelhas,bquelhas,https://github.com/bquelhas,55,55,1,1,0",
        "",
      ].join("\n"),
    );
    assert.equal(
      readAnalyticsCsv(withoutCaretakers, "listing_version_credits.csv"),
      [
        "listing_type,listing_id,version,credited_author_id",
        "map,porto,v1.0.0,bquelhas",
        "map,porto,v2.0.0,bquelhas",
        "",
      ].join("\n"),
    );
  } finally {
    rmSync(withoutCaretakers, { recursive: true, force: true });
    rmSync(withClosedWindow, { recursive: true, force: true });
  }
});
