import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  applyHourlySuppressions,
  computeListingDeltas,
  getHourlyShardMonth,
  getHourlyShardRelativePath,
  groupHourlyRowsByMonth,
  HOURLY_DOWNLOADS_BACKFILL_FLOOR,
  mergeHourlyRows,
  parseHourlyDownloadsCsv,
  parseHourlySuppressions,
  serializeHourlyDownloadsCsv,
  truncateToHourBucketUtc,
  type HourlyDownloadRow,
} from "../lib/hourly-downloads.js";

test("truncateToHourBucketUtc normalizes offsets to UTC hour keys", () => {
  assert.equal(truncateToHourBucketUtc("2026-08-13T04:23:45.678Z"), "2026-08-13T04:00Z");
  // JST offset: 05:10+09:00 = 20:10 UTC the previous day.
  assert.equal(truncateToHourBucketUtc("2026-08-13T05:10:00+09:00"), "2026-08-12T20:00Z");
  assert.throws(() => truncateToHourBucketUtc("not-a-date"), /Unparseable/);
});

test("computeListingDeltas sums versions, clamps drops, nets intra-listing movement", () => {
  const previous = {
    lyon: { "v1.0.0": 100, "v1.0.2": 40 },
    steady: { "1.0.0": 5 },
    corrected: { "1.0.0": 900 },
  };
  const next = {
    lyon: { "v1.0.0": 100, "v1.0.2": 47 },
    steady: { "1.0.0": 5 },
    corrected: { "1.0.0": 300 }, // attribution landed: adjusted dropped — clamp, no negative row
    "brand-new": { "1.0.0": 3 },
  };
  const deltas = computeListingDeltas("map", previous, next);
  assert.deepEqual(deltas, [
    { listing_type: "map", id: "lyon", downloads: 7 },
    { listing_type: "map", id: "brand-new", downloads: 3 },
  ]);
});

test("csv serialize/parse round-trips and sorts chronologically", () => {
  const rows: HourlyDownloadRow[] = [
    { bucket_utc: "2026-08-13T05:00Z", listing_type: "mod", id: "b-mod", downloads: 1 },
    { bucket_utc: "2026-08-13T04:00Z", listing_type: "map", id: "a-map", downloads: 12 },
  ];
  const csv = serializeHourlyDownloadsCsv(rows);
  assert.equal(
    csv,
    "bucket_utc,listing_type,id,downloads\n"
    + "2026-08-13T04:00Z,map,a-map,12\n"
    + "2026-08-13T05:00Z,mod,b-mod,1\n",
  );
  assert.deepEqual(parseHourlyDownloadsCsv(csv), [
    { bucket_utc: "2026-08-13T04:00Z", listing_type: "map", id: "a-map", downloads: 12 },
    { bucket_utc: "2026-08-13T05:00Z", listing_type: "mod", id: "b-mod", downloads: 1 },
  ]);
});

test("parseHourlyDownloadsCsv skips malformed lines", () => {
  const parsed = parseHourlyDownloadsCsv([
    "bucket_utc,listing_type,id,downloads",
    "2026-08-13T04:00Z,map,good,2",
    "not-a-bucket,map,bad,2",
    "2026-08-13T04:00Z,plugin,bad-type,2",
    "2026-08-13T04:00Z,map,zero,0",
    "garbage",
    "",
  ].join("\n"));
  assert.deepEqual(parsed.map((row) => row.id), ["good"]);
});

test("mergeHourlyRows sums same bucket+listing entries", () => {
  const merged = mergeHourlyRows(
    [{ bucket_utc: "2026-08-13T04:00Z", listing_type: "map", id: "lyon", downloads: 2 }],
    [
      { bucket_utc: "2026-08-13T04:00Z", listing_type: "map", id: "lyon", downloads: 3 },
      { bucket_utc: "2026-08-13T04:00Z", listing_type: "mod", id: "lyon", downloads: 1 },
    ],
  );
  const key = (row: HourlyDownloadRow): string => `${row.listing_type}:${row.id}`;
  const byKey = new Map(merged.map((row) => [key(row), row.downloads]));
  assert.equal(byKey.get("map:lyon"), 5);
  assert.equal(byKey.get("mod:lyon"), 1);
  assert.equal(merged.length, 2);
});

test("parseHourlySuppressions keeps valid entries and drops malformed ones", () => {
  const parsed = parseHourlySuppressions({
    schema_version: 1,
    suppressions: [
      { bucket_utc: "2026-08-06T01:00Z", listing_type: "mod", id: "gone-mod", reason: "restore burst" },
      { bucket_utc: "2026-08-06T01:00Z", listing_type: "mod", id: "partial", downloads: 40, reason: "partial raise" },
      { bucket_utc: "not-an-hour", listing_type: "mod", id: "bad-bucket", reason: "x" },
      { bucket_utc: "2026-08-06T01:00Z", listing_type: "widget", id: "bad-type", reason: "x" },
      { bucket_utc: "2026-08-06T01:00Z", listing_type: "mod", id: "no-reason" },
    ],
  });
  assert.deepEqual(parsed.map((entry) => entry.id), ["gone-mod", "partial"]);
  assert.equal(parsed[0]!.downloads, undefined);
  assert.equal(parsed[1]!.downloads, 40);
});

test("applyHourlySuppressions drops whole rows and subtracts partial amounts", () => {
  const rows: HourlyDownloadRow[] = [
    { bucket_utc: "2026-08-06T01:00Z", listing_type: "mod", id: "gone-mod", downloads: 2376 },
    { bucket_utc: "2026-08-06T01:00Z", listing_type: "mod", id: "partial", downloads: 50 },
    { bucket_utc: "2026-08-06T01:00Z", listing_type: "mod", id: "swallowed", downloads: 30 },
    { bucket_utc: "2026-08-06T01:00Z", listing_type: "map", id: "gone-mod", downloads: 7 },
    { bucket_utc: "2026-08-06T02:00Z", listing_type: "mod", id: "gone-mod", downloads: 5 },
  ];
  const { rows: result, suppressed } = applyHourlySuppressions(rows, [
    { bucket_utc: "2026-08-06T01:00Z", listing_type: "mod", id: "gone-mod", reason: "full drop" },
    { bucket_utc: "2026-08-06T01:00Z", listing_type: "mod", id: "partial", downloads: 40, reason: "subtract" },
    { bucket_utc: "2026-08-06T01:00Z", listing_type: "mod", id: "swallowed", downloads: 30, reason: "exact subtract drops" },
  ]);
  assert.equal(suppressed, 3);
  const key = (row: HourlyDownloadRow): string => `${row.bucket_utc} ${row.listing_type} ${row.id}`;
  const byKey = new Map(result.map((row) => [key(row), row.downloads]));
  assert.equal(byKey.has("2026-08-06T01:00Z mod gone-mod"), false);
  assert.equal(byKey.get("2026-08-06T01:00Z mod partial"), 10);
  assert.equal(byKey.has("2026-08-06T01:00Z mod swallowed"), false);
  // Same id under a different type or hour is untouched.
  assert.equal(byKey.get("2026-08-06T01:00Z map gone-mod"), 7);
  assert.equal(byKey.get("2026-08-06T02:00Z mod gone-mod"), 5);
});

test("the committed suppression spec matches the pruned restoration rows", () => {
  const spec = JSON.parse(
    readFileSync(resolve(import.meta.dirname, "..", "..", "..", "history", "hourly-suppressions.json"), "utf-8"),
  ) as unknown;
  const parsed = parseHourlySuppressions(spec);
  // Every committed entry must survive the parser — a silently dropped entry
  // would resurrect an administrative burst on the next backfill.
  const rawCount = (spec as { suppressions: unknown[] }).suppressions.length;
  assert.equal(parsed.length, rawCount, "parser dropped committed entries");
  // Suppressions exist only for the documented incidents (KNOWN_INCIDENTS.md).
  const countsByBucket = new Map<string, number>();
  for (const entry of parsed) {
    assert.equal(entry.downloads, undefined, "restoration rows are whole-row drops");
    countsByBucket.set(entry.bucket_utc, (countsByBucket.get(entry.bucket_utc) ?? 0) + 1);
  }
  assert.deepEqual(
    Object.fromEntries([...countsByBucket.entries()].sort()),
    {
      "2026-07-08T09:00Z": 103, // 429-cascade counter bounce
      "2026-07-09T02:00Z": 14, // 429-cascade restoration
      "2026-07-09T03:00Z": 10, // 429-cascade restoration
      "2026-08-06T01:00Z": 3, // private-repo-wipe restoration
      "2026-08-13T00:00Z": 50, // jp-maps wipe restoration
    },
  );
});

test("shard helpers key rows by UTC month and build shard paths", () => {
  assert.equal(getHourlyShardMonth("2026-07-31T23:00Z"), "2026-07");
  assert.equal(
    getHourlyShardRelativePath("2026-07"),
    "analytics/hourly/downloads-2026-07.csv",
  );
  assert.throws(() => getHourlyShardRelativePath("july"), /Invalid shard month/);

  const rows: HourlyDownloadRow[] = [
    { bucket_utc: "2026-07-31T23:00Z", listing_type: "map", id: "a", downloads: 1 },
    { bucket_utc: "2026-08-01T00:00Z", listing_type: "map", id: "a", downloads: 2 },
    { bucket_utc: "2026-08-01T01:00Z", listing_type: "mod", id: "b", downloads: 3 },
  ];
  const byMonth = groupHourlyRowsByMonth(rows);
  assert.deepEqual([...byMonth.keys()].sort(), ["2026-07", "2026-08"]);
  assert.equal(byMonth.get("2026-07")!.length, 1);
  assert.equal(byMonth.get("2026-08")!.length, 2);
});

test("the backfill floor sits on the first fully worker-scheduled day", () => {
  // 2026-06-30 was the first day of full hourly coverage; the floor starts the
  // series one day later so the first bucket's BASELINE commit is also from a
  // fully covered day (see KNOWN_INCIDENTS.md).
  assert.equal(HOURLY_DOWNLOADS_BACKFILL_FLOOR, "2026-07-01T00:00Z");
  assert.equal(getHourlyShardMonth(HOURLY_DOWNLOADS_BACKFILL_FLOOR), "2026-07");
});
