import test from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { runGenerateAnalyticsCli } from "../lib/analytics-core.js";

function writeJson(path: string, value: unknown): void {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, "utf-8");
}

function findCsvRow(csv: string, id: string): string | undefined {
  return csv
    .trim()
    .split(/\r?\n/)
    .find((line, index) => index > 0 && line.split(",")[1] === id);
}

test("runGenerateAnalyticsCli writes assets_by_day.csv grouped by listing type", () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-analytics-core-"));
  mkdirSync(join(repoRoot, "history"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "sample-map"), { recursive: true });
  mkdirSync(join(repoRoot, "mods", "sample-mod"), { recursive: true });

  try {
    writeJson(join(repoRoot, "maps", "index.json"), {
      schema_version: 1,
      maps: ["sample-map"],
    });
    writeJson(join(repoRoot, "maps", "sample-map", "manifest.json"), {
      schema_version: 1,
      id: "sample-map",
      name: "Sample Map",
      author: "mapmaker",
      github_id: 1,
      source: "https://github.com/example/sample-map",
      city_code: "ABC",
      country: "US",
      population: 0,
      population_count: 0,
      points_count: 0,
    });
    writeJson(join(repoRoot, "mods", "sample-mod", "manifest.json"), {
      schema_version: 1,
      id: "sample-mod",
      name: "Sample Mod",
      author: "modder",
      github_id: 2,
      source: "https://github.com/example/sample-mod",
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_30.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_30",
      generated_at: "2026-03-30T00:00:00.000Z",
      maps: {
        downloads: {
          "sample-map": {
            "1.0.0": 10,
          },
        },
      },
      mods: {
        downloads: {
          "sample-mod": {
            "1.0.0": 5,
          },
        },
      },
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_31.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_31",
      generated_at: "2026-03-31T00:00:00.000Z",
      maps: {
        downloads: {
          "sample-map": {
            "1.0.0": 13,
          },
        },
      },
      mods: {
        downloads: {
          "sample-mod": {
            "1.0.0": 7,
          },
        },
      },
    });

    runGenerateAnalyticsCli([], repoRoot);

    const assetsByDayCsv = readFileSync(join(repoRoot, "analytics", "assets_by_day.csv"), "utf-8");
    assert.equal(
      assetsByDayCsv,
      [
        "snapshot_date,total_downloads,maps,mods,total_downloads_signed,maps_signed,mods_signed,total_downloads_clamped,maps_clamped,mods_clamped,cumulative_total,cumulative_maps,cumulative_mods,total_new_assets,new_maps,new_mods,cumulative_assets,cumulative_maps_assets,cumulative_mods_assets,total_new_assets_versions,new_maps_versions,new_mods_versions,cumulative_asset_versions,cumulative_maps_versions,cumulative_mods_versions",
        "2026_03_30,15,10,5,15,10,5,15,10,5,15,10,5,2,1,1,2,1,1,2,1,1,2,1,1",
        "2026_03_31,5,3,2,5,3,2,5,3,2,20,13,7,0,0,0,2,1,1,0,0,0,2,1,1",
        "",
      ].join("\n"),
    );
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("runGenerateAnalyticsCli writes asset_versions_by_day.csv with clamped per-version deltas", () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-version-by-day-"));
  mkdirSync(join(repoRoot, "history"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "sample-map"), { recursive: true });
  mkdirSync(join(repoRoot, "mods", "sample-mod"), { recursive: true });

  try {
    writeJson(join(repoRoot, "maps", "index.json"), {
      schema_version: 1,
      maps: ["sample-map"],
    });
    writeJson(join(repoRoot, "maps", "sample-map", "manifest.json"), {
      schema_version: 1,
      id: "sample-map",
      name: "Sample Map",
      author: "mapmaker",
      github_id: 1,
      source: "https://github.com/example/sample-map",
      city_code: "ABC",
      country: "US",
      population: 0,
      population_count: 0,
      points_count: 0,
    });
    writeJson(join(repoRoot, "mods", "sample-mod", "manifest.json"), {
      schema_version: 1,
      id: "sample-mod",
      name: "Sample Mod",
      author: "modder",
      github_id: 2,
      source: "https://github.com/example/sample-mod",
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_30.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_30",
      generated_at: "2026-03-30T00:00:00.000Z",
      maps: {
        downloads: {
          "sample-map": {
            "1.0.0": 10,
          },
        },
      },
      mods: {
        downloads: {
          "sample-mod": {
            "1.0.0": 5,
          },
        },
      },
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_31.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_31",
      generated_at: "2026-03-31T00:00:00.000Z",
      maps: {
        downloads: {
          "sample-map": {
            "1.0.0": 12,
            "1.1.0": 4,
          },
        },
      },
      mods: {
        downloads: {
          "sample-mod": {
            // Dips below the previous snapshot (e.g. asset re-upload): the
            // monotonic series holds 5, so the day's delta clamps to 0.
            "1.0.0": 3,
          },
        },
      },
    });

    runGenerateAnalyticsCli([], repoRoot);

    const versionByDayCsv = readFileSync(join(repoRoot, "analytics", "asset_versions_by_day.csv"), "utf-8");
    assert.equal(
      versionByDayCsv,
      [
        "listing_type,id,version,total_downloads,2026_03_30,2026_03_31",
        "map,sample-map,1.0.0,12,10,2",
        "map,sample-map,1.1.0,4,0,4",
        "mod,sample-mod,1.0.0,5,5,0",
        "",
      ].join("\n"),
    );
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("runGenerateAnalyticsCli writes maps_statistics.csv from grid.geojson and removes legacy maps_by_population.csv", () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-map-statistics-"));
  mkdirSync(join(repoRoot, "analytics"), { recursive: true });
  mkdirSync(join(repoRoot, "history"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "sample-map"), { recursive: true });

  try {
    writeJson(join(repoRoot, "maps", "index.json"), {
      schema_version: 1,
      maps: ["sample-map"],
    });
    writeJson(join(repoRoot, "maps", "sample-map", "manifest.json"), {
      schema_version: 1,
      id: "sample-map",
      name: "Sample Map",
      author: "mapmaker",
      github_id: 1,
      source: "https://github.com/example/sample-map",
      city_code: "ABC",
      country: "US",
      population: 600,
      population_count: 9,
      points_count: 12,
    });
    writeJson(join(repoRoot, "maps", "sample-map", "grid.geojson"), {
      type: "FeatureCollection",
      features: [
        {
          type: "Feature",
          properties: {
            jobs: 10,
            pop: 100,
            pointCount: 4,
          },
          geometry: {
            type: "Polygon",
            coordinates: [],
          },
        },
        {
          type: "Feature",
          properties: {
            jobs: 30,
            pop: 300,
            pointCount: 2,
          },
          geometry: {
            type: "Polygon",
            coordinates: [],
          },
        },
        {
          type: "Feature",
          properties: {
            jobs: 20,
            pop: 200,
            pointCount: 6,
          },
          geometry: {
            type: "Polygon",
            coordinates: [],
          },
        },
      ],
      properties: {
        residentWeightedNearestNeighborKm: {
          p10: 0.1,
          p25: 0.15,
          p50: 0.2,
          p75: 0.25,
          p90: 0.3,
          mean: 0.25,
        },
        workerWeightedNearestNeighborKm: {
          p10: 0.2,
          p25: 0.3,
          p50: 0.4,
          p75: 0.5,
          p90: 0.6,
          mean: 0.45,
        },
        detail: {
          radiusKm: 0.283,
          expectedPointSpacingKm: 0.5,
          normalizedRadius: 0.566,
          activityPerPoint: 160,
          playableAreaKm2: 18,
          playableAreaPerPointKm2: 1.5,
          playableCatchmentRadiusKm: 0.691,
          localityScore: 0.82,
          deaggregationScore: 0.66,
          score: 0.73,
        },
        commuteDistanceKm: {
          p10: 9,
          p25: 10,
          p50: 12,
          p75: 14,
          p90: 16,
          mean: 15,
        },
        polycentrism: {
          activity: {
            detectedCenterCount: 2,
            continuousScore: 0.678,
          },
        },
      },
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_31.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_31",
      generated_at: "2026-03-31T00:00:00.000Z",
      maps: {
        downloads: {
          "sample-map": {
            "1.0.0": 1,
          },
        },
      },
      mods: {
        downloads: {},
      },
    });
    writeFileSync(
      join(repoRoot, "analytics", "maps_by_population.csv"),
      "legacy,stale,file\n",
      "utf-8",
    );

    runGenerateAnalyticsCli([], repoRoot);

    const mapsStatisticsCsv = readFileSync(join(repoRoot, "analytics", "maps_statistics.csv"), "utf-8");
    assert.equal(
      mapsStatisticsCsv,
      [
        "rank,id,name,author,author_alias,attribution_link,city_code,country,population,population_count,points_count,n_cells,mean_point_density,median_resident_weighted_nn_km,mean_resident_weighted_nn_km,median_worker_weighted_nn_km,mean_worker_weighted_nn_km,detail_radius_km,detail_score,playable_area_cells,median_cell_resident_density,mean_cell_resident_density,pct_cells_with_residents,median_cell_worker_density,mean_cell_worker_density,pct_cells_with_workers,median_commute_distance,mean_commute_distance,detected_center_count,polycentrism_score",
        "1,sample-map,Sample Map,mapmaker,mapmaker,https://github.com/mapmaker,ABC,US,600,9,12,3,4,0.2,0.25,0.4,0.45,0.283,0.73,18,200,200,100,20,20,100,12,15,2,0.68",
        "",
      ].join("\n"),
    );
    assert.equal(existsSync(join(repoRoot, "analytics", "maps_by_population.csv")), false);
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("runGenerateAnalyticsCli computes resident and worker densities from non-zero cells only", () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-map-statistics-positive-only-"));
  mkdirSync(join(repoRoot, "analytics"), { recursive: true });
  mkdirSync(join(repoRoot, "history"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "sample-map"), { recursive: true });

  try {
    writeJson(join(repoRoot, "maps", "index.json"), {
      schema_version: 1,
      maps: ["sample-map"],
    });
    writeJson(join(repoRoot, "maps", "sample-map", "manifest.json"), {
      schema_version: 1,
      id: "sample-map",
      name: "Sample Map",
      author: "mapmaker",
      github_id: 1,
      source: "https://github.com/example/sample-map",
      city_code: "ABC",
      country: "US",
      population: 600,
      population_count: 9,
      points_count: 12,
    });
    writeJson(join(repoRoot, "maps", "sample-map", "grid.geojson"), {
      type: "FeatureCollection",
      features: [
        {
          type: "Feature",
          properties: {
            jobs: 0,
            pop: 100,
            pointCount: 4,
          },
          geometry: {
            type: "Polygon",
            coordinates: [],
          },
        },
        {
          type: "Feature",
          properties: {
            jobs: 30,
            pop: 0,
            pointCount: 2,
          },
          geometry: {
            type: "Polygon",
            coordinates: [],
          },
        },
        {
          type: "Feature",
          properties: {
            jobs: 20,
            pop: 200,
            pointCount: 6,
          },
          geometry: {
            type: "Polygon",
            coordinates: [],
          },
        },
      ],
      properties: {
        residentWeightedNearestNeighborKm: {
          p10: 0.2,
          p25: 0.25,
          p50: 0.3,
          p75: 0.4,
          p90: 0.5,
          mean: 0.35,
        },
        workerWeightedNearestNeighborKm: {
          p10: 0.3,
          p25: 0.35,
          p50: 0.5,
          p75: 0.6,
          p90: 0.7,
          mean: 0.55,
        },
        detail: {
          radiusKm: 0.387,
          expectedPointSpacingKm: 0.45,
          normalizedRadius: 0.86,
          activityPerPoint: 420,
          playableAreaKm2: 24,
          playableAreaPerPointKm2: 2,
          playableCatchmentRadiusKm: 0.798,
          localityScore: 0.12,
          deaggregationScore: 0.94,
          score: 0.34,
        },
        commuteDistanceKm: {
          p10: 8.111,
          p25: 10.222,
          p50: 12.994,
          p75: 14.555,
          p90: 16.777,
          mean: 15.126,
        },
        polycentrism: {
          activity: {
            detectedCenterCount: 3,
            continuousScore: 0.444,
          },
        },
      },
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_31.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_31",
      generated_at: "2026-03-31T00:00:00.000Z",
      maps: {
        downloads: {
          "sample-map": {
            "1.0.0": 1,
          },
        },
      },
      mods: {
        downloads: {},
      },
    });

    runGenerateAnalyticsCli([], repoRoot);

    const mapsStatisticsCsv = readFileSync(join(repoRoot, "analytics", "maps_statistics.csv"), "utf-8");
    assert.equal(
      mapsStatisticsCsv,
      [
        "rank,id,name,author,author_alias,attribution_link,city_code,country,population,population_count,points_count,n_cells,mean_point_density,median_resident_weighted_nn_km,mean_resident_weighted_nn_km,median_worker_weighted_nn_km,mean_worker_weighted_nn_km,detail_radius_km,detail_score,playable_area_cells,median_cell_resident_density,mean_cell_resident_density,pct_cells_with_residents,median_cell_worker_density,mean_cell_worker_density,pct_cells_with_workers,median_commute_distance,mean_commute_distance,detected_center_count,polycentrism_score",
        "1,sample-map,Sample Map,mapmaker,mapmaker,https://github.com/mapmaker,ABC,US,600,9,12,3,4,0.3,0.35,0.5,0.55,0.387,0.34,24,200,150,66.67,30,25,66.67,12.99,15.13,3,0.44",
        "",
      ].join("\n"),
    );
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("runGenerateAnalyticsCli keeps detail_score stable regardless of other map rows", () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-map-detail-score-stable-"));
  mkdirSync(join(repoRoot, "analytics"), { recursive: true });
  mkdirSync(join(repoRoot, "history"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "sample-map"), { recursive: true });

  try {
    writeJson(join(repoRoot, "maps", "index.json"), {
      schema_version: 1,
      maps: ["sample-map"],
    });
    writeJson(join(repoRoot, "maps", "sample-map", "manifest.json"), {
      schema_version: 1,
      id: "sample-map",
      name: "Sample Map",
      author: "mapmaker",
      github_id: 1,
      source: "https://github.com/example/sample-map",
      city_code: "ABC",
      country: "US",
      population: 600,
      population_count: 9,
      points_count: 12,
    });
    writeJson(join(repoRoot, "maps", "sample-map", "grid.geojson"), {
      type: "FeatureCollection",
      features: [],
      properties: {
        residentWeightedNearestNeighborKm: { p10: 0.1, p25: 0.15, p50: 0.2, p75: 0.25, p90: 0.3, mean: 0.25 },
        workerWeightedNearestNeighborKm: { p10: 0.2, p25: 0.3, p50: 0.4, p75: 0.5, p90: 0.6, mean: 0.45 },
        commuteDistanceKm: { p10: 1, p25: 2, p50: 3, p75: 4, p90: 5, mean: 3 },
        polycentrism: { activity: { detectedCenterCount: 1, continuousScore: 0.2 } },
      },
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_31.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_31",
      generated_at: "2026-03-31T00:00:00.000Z",
      maps: { downloads: { "sample-map": { "1.0.0": 1 } } },
      mods: { downloads: {} },
    });

    runGenerateAnalyticsCli([], repoRoot);
    const beforeCsv = readFileSync(join(repoRoot, "analytics", "maps_statistics.csv"), "utf-8");
    const beforeRow = findCsvRow(beforeCsv, "sample-map");
    assert.equal(
      beforeRow,
      "1,sample-map,Sample Map,mapmaker,mapmaker,https://github.com/mapmaker,ABC,US,600,9,12,0,0,0.2,0.25,0.4,0.45,0,0,0,0,0,0,0,0,0,3,3,1,0.2",
    );

    mkdirSync(join(repoRoot, "maps", "coarse-map"), { recursive: true });
    writeJson(join(repoRoot, "maps", "index.json"), {
      schema_version: 1,
      maps: ["sample-map", "coarse-map"],
    });
    writeJson(join(repoRoot, "maps", "coarse-map", "manifest.json"), {
      schema_version: 1,
      id: "coarse-map",
      name: "Coarse Map",
      author: "othermaker",
      github_id: 2,
      source: "https://github.com/example/coarse-map",
      city_code: "DEF",
      country: "US",
      population: 300,
      population_count: 4,
      points_count: 6,
    });
    writeJson(join(repoRoot, "maps", "coarse-map", "grid.geojson"), {
      type: "FeatureCollection",
      features: [],
      properties: {
        residentWeightedNearestNeighborKm: { p10: 0.7, p25: 0.8, p50: 0.9, p75: 1.0, p90: 1.1, mean: 0.95 },
        workerWeightedNearestNeighborKm: { p10: 0.8, p25: 0.9, p50: 1.0, p75: 1.1, p90: 1.2, mean: 1.05 },
        commuteDistanceKm: { p10: 1, p25: 2, p50: 3, p75: 4, p90: 5, mean: 3 },
        polycentrism: { activity: { detectedCenterCount: 1, continuousScore: 0.1 } },
      },
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_31.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_31",
      generated_at: "2026-03-31T00:00:00.000Z",
      maps: { downloads: { "sample-map": { "1.0.0": 1 }, "coarse-map": { "1.0.0": 1 } } },
      mods: { downloads: {} },
    });

    runGenerateAnalyticsCli([], repoRoot);
    const afterCsv = readFileSync(join(repoRoot, "analytics", "maps_statistics.csv"), "utf-8");
    const afterRow = findCsvRow(afterCsv, "sample-map");

    assert.equal(afterRow, beforeRow);
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("runGenerateAnalyticsCli defaults to full listing and author outputs and still honors explicit top-k limits", () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-analytics-topk-defaults-"));
  mkdirSync(join(repoRoot, "analytics"), { recursive: true });
  mkdirSync(join(repoRoot, "history"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "sample-map"), { recursive: true });
  mkdirSync(join(repoRoot, "mods", "sample-mod"), { recursive: true });

  try {
    writeJson(join(repoRoot, "maps", "index.json"), {
      schema_version: 1,
      maps: ["sample-map"],
    });
    writeJson(join(repoRoot, "mods", "sample-mod", "manifest.json"), {
      schema_version: 1,
      id: "sample-mod",
      name: "Sample Mod",
      author: "modder",
      github_id: 2,
      source: "https://github.com/example/sample-mod",
    });
    writeJson(join(repoRoot, "maps", "sample-map", "manifest.json"), {
      schema_version: 1,
      id: "sample-map",
      name: "Sample Map",
      author: "mapmaker",
      github_id: 1,
      source: "https://github.com/example/sample-map",
      city_code: "ABC",
      country: "US",
      population: 0,
      population_count: 0,
      points_count: 0,
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_31.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_31",
      generated_at: "2026-03-31T00:00:00.000Z",
      maps: { downloads: { "sample-map": { "1.0.0": 10 } } },
      mods: { downloads: { "sample-mod": { "1.0.0": 5 } } },
    });

    runGenerateAnalyticsCli([], repoRoot);

    const allTimeRows = readFileSync(join(repoRoot, "analytics", "most_popular_all_time.csv"), "utf-8").trim().split(/\r?\n/);
    const authorRows = readFileSync(join(repoRoot, "analytics", "authors_by_total_downloads.csv"), "utf-8").trim().split(/\r?\n/);
    assert.equal(allTimeRows.length, 3);
    assert.equal(authorRows.length, 3);
    for (const days of [1, 3, 7, 14, 30]) {
      assert.equal(existsSync(join(repoRoot, "analytics", `most_popular_last_${days}d.csv`)), true);
      assert.equal(existsSync(join(repoRoot, "analytics", `projects_most_popular_last_${days}d.csv`)), true);
      assert.equal(existsSync(join(repoRoot, "analytics", `authors_last_${days}d.csv`)), true);
    }

    runGenerateAnalyticsCli(["--top-k-listings", "1", "--top-k-authors", "1"], repoRoot);

    const limitedAllTimeRows = readFileSync(join(repoRoot, "analytics", "most_popular_all_time.csv"), "utf-8").trim().split(/\r?\n/);
    const limitedAuthorRows = readFileSync(join(repoRoot, "analytics", "authors_by_total_downloads.csv"), "utf-8").trim().split(/\r?\n/);
    const limitedAuthorWindowRows = readFileSync(join(repoRoot, "analytics", "authors_last_30d.csv"), "utf-8").trim().split(/\r?\n/);
    assert.equal(limitedAllTimeRows.length, 2);
    assert.equal(limitedAuthorRows.length, 2);
    assert.equal(limitedAuthorWindowRows.length, 2);
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});

test("runGenerateAnalyticsCli splits author and project window download changes by asset type", () => {
  const repoRoot = mkdtempSync(join(tmpdir(), "railyard-window-type-split-"));
  mkdirSync(join(repoRoot, "history"), { recursive: true });
  mkdirSync(join(repoRoot, "maps", "sample-map"), { recursive: true });
  mkdirSync(join(repoRoot, "mods", "sample-mod"), { recursive: true });

  try {
    writeJson(join(repoRoot, "maps", "index.json"), {
      schema_version: 1,
      maps: ["sample-map"],
    });
    // Both listings share one author and one source repo, so the author row
    // and the project row each aggregate a map AND a mod.
    writeJson(join(repoRoot, "maps", "sample-map", "manifest.json"), {
      schema_version: 1,
      id: "sample-map",
      name: "Sample Map",
      author: "mapmaker",
      github_id: 1,
      source: "https://github.com/example/shared-project",
      city_code: "ABC",
      country: "US",
      population: 0,
      population_count: 0,
      points_count: 0,
    });
    writeJson(join(repoRoot, "mods", "sample-mod", "manifest.json"), {
      schema_version: 1,
      id: "sample-mod",
      name: "Sample Mod",
      author: "mapmaker",
      github_id: 1,
      source: "https://github.com/example/shared-project",
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_30.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_30",
      generated_at: "2026-03-30T00:00:00.000Z",
      maps: { downloads: { "sample-map": { "1.0.0": 10 } } },
      mods: { downloads: { "sample-mod": { "1.0.0": 5 } } },
    });
    writeJson(join(repoRoot, "history", "snapshot_2026_03_31.json"), {
      schema_version: 2,
      snapshot_date: "2026_03_31",
      generated_at: "2026-03-31T00:00:00.000Z",
      maps: { downloads: { "sample-map": { "1.0.0": 13 } } },
      mods: { downloads: { "sample-mod": { "1.0.0": 7 } } },
    });

    runGenerateAnalyticsCli([], repoRoot);

    const readRow = (fileName: string, matchColumn: string, matchValue: string) => {
      const lines = readFileSync(join(repoRoot, "analytics", fileName), "utf-8")
        .trim()
        .split(/\r?\n/);
      const headers = lines[0]!.split(",");
      const row = lines
        .slice(1)
        .map((line) => line.split(","))
        .find((values) => values[headers.indexOf(matchColumn)] === matchValue);
      assert.ok(row, `row with ${matchColumn}=${matchValue} in ${fileName}`);
      return (column: string) => {
        const index = headers.indexOf(column);
        assert.notEqual(index, -1, `column ${column} in ${fileName}`);
        return row[index];
      };
    };

    const authorRow = readRow("authors_last_1d.csv", "author", "mapmaker");
    assert.equal(authorRow("download_change"), "5");
    assert.equal(authorRow("map_download_change"), "3");
    assert.equal(authorRow("mod_download_change"), "2");
    assert.equal(authorRow("adjusted_map_download_change"), "3");
    assert.equal(authorRow("adjusted_mod_download_change"), "2");

    const projectRow = readRow(
      "projects_most_popular_last_1d.csv",
      "project_key",
      "example/shared-project",
    );
    assert.equal(projectRow("download_change"), "5");
    assert.equal(projectRow("map_download_change"), "3");
    assert.equal(projectRow("mod_download_change"), "2");
    assert.equal(projectRow("adjusted_map_download_change"), "3");
    assert.equal(projectRow("adjusted_mod_download_change"), "2");
  } finally {
    rmSync(repoRoot, { recursive: true, force: true });
  }
});
