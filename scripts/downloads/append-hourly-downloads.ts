import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import {
  computeListingDeltas,
  getHourlyShardMonth,
  getHourlyShardRelativePath,
  HOURLY_DOWNLOADS_CSV_RELATIVE_PATH,
  mergeHourlyRows,
  parseHourlyDownloadsCsv,
  pruneHourlyRows,
  serializeHourlyDownloadsCsv,
  truncateToHourBucketUtc,
  type DownloadsFile,
  type HourlyDownloadRow,
  type HourlyListingType,
} from "../lib/hourly-downloads.js";
import { runGitCommand } from "../lib/git-history.js";
import { appendGitHubOutput, resolveRepoRoot, runAndExitOnError } from "../lib/script-runtime.js";

// Appends this run's per-listing download deltas to the hourly series
// (analytics/hourly/downloads-YYYY-MM.csv, sharded by UTC month). Runs in the
// hourly workflow's commit job AFTER the regenerated downloads.json artifacts
// are copied into the working tree: the previous state is read from git HEAD
// (main's last committed counters), the new state from disk. A missing HEAD
// baseline skips the listing type rather than booking the whole cumulative
// counter as one hour.
//
// Shards are never pruned — a month's file freezes once the month ends. The
// legacy un-suffixed downloads.csv (trailing 14-day view over the newest
// shards) is regenerated alongside for the deployed website until it reads
// the shards directly.

interface TypeSpec {
  listingType: HourlyListingType;
  relativePath: string;
}

const TYPE_SPECS: TypeSpec[] = [
  { listingType: "map", relativePath: "maps/downloads.json" },
  { listingType: "mod", relativePath: "mods/downloads.json" },
];

function readHeadDownloads(repoRoot: string, relativePath: string): DownloadsFile | null {
  const content = runGitCommand(repoRoot, ["show", `HEAD:${relativePath}`]);
  if (!content) return null;
  try {
    return JSON.parse(content) as DownloadsFile;
  } catch {
    return null;
  }
}

function readWorkingDownloads(repoRoot: string, relativePath: string): DownloadsFile | null {
  const path = resolve(repoRoot, ...relativePath.split("/"));
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf-8")) as DownloadsFile;
  } catch {
    return null;
  }
}

function readCsvRows(path: string): HourlyDownloadRow[] {
  return existsSync(path) ? parseHourlyDownloadsCsv(readFileSync(path, "utf-8")) : [];
}

/** Writes serialized rows unless the file already holds them; true when written. */
function writeCsvIfChanged(path: string, rows: HourlyDownloadRow[]): boolean {
  const serialized = serializeHourlyDownloadsCsv(rows);
  const previous = existsSync(path) ? readFileSync(path, "utf-8") : "";
  if (serialized === previous) return false;
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, serialized, "utf-8");
  return true;
}

/** The previous UTC month of a "YYYY-MM" key. */
function previousMonth(month: string): string {
  const year = Number.parseInt(month.slice(0, 4), 10);
  const monthIndex = Number.parseInt(month.slice(5, 7), 10) - 1;
  return new Date(Date.UTC(year, monthIndex - 1, 1)).toISOString().slice(0, 7);
}

async function run(): Promise<void> {
  const repoRoot = process.env.RAILYARD_REPO_ROOT ?? resolveRepoRoot(import.meta.dirname);
  const nowMs = Date.now();
  const bucket = truncateToHourBucketUtc(new Date(nowMs).toISOString());
  const month = getHourlyShardMonth(bucket);

  const additions: HourlyDownloadRow[] = [];
  for (const spec of TYPE_SPECS) {
    const previous = readHeadDownloads(repoRoot, spec.relativePath);
    const next = readWorkingDownloads(repoRoot, spec.relativePath);
    if (!previous || !next) {
      console.warn(`[hourly-downloads] missing ${!previous ? "HEAD" : "working"} state for ${spec.relativePath}; skipping ${spec.listingType}s`);
      continue;
    }
    for (const delta of computeListingDeltas(spec.listingType, previous, next)) {
      additions.push({ bucket_utc: bucket, ...delta });
    }
  }

  const shardPath = resolve(repoRoot, ...getHourlyShardRelativePath(month).split("/"));
  const mergedShard = mergeHourlyRows(readCsvRows(shardPath), additions);
  const shardChanged = writeCsvIfChanged(shardPath, mergedShard);

  // Legacy trailing-window view: the current + previous shard always cover it.
  const previousShardPath = resolve(
    repoRoot,
    ...getHourlyShardRelativePath(previousMonth(month)).split("/"),
  );
  const legacyRows = pruneHourlyRows(
    [...readCsvRows(previousShardPath), ...mergedShard],
    nowMs,
  );
  const legacyPath = resolve(repoRoot, ...HOURLY_DOWNLOADS_CSV_RELATIVE_PATH.split("/"));
  const legacyChanged = writeCsvIfChanged(legacyPath, legacyRows);

  if (!shardChanged && !legacyChanged) {
    console.log(`[hourly-downloads] bucket=${bucket} no changes (deltas=0, nothing pruned)`);
    appendGitHubOutput(["hourly_downloads_changed=false"]);
    return;
  }

  const totalNew = additions.reduce((sum, row) => sum + row.downloads, 0);
  console.log(
    `[hourly-downloads] bucket=${bucket} listings=${additions.length} downloads=${totalNew} shard=${month} rows=${mergedShard.length} legacy_rows=${legacyRows.length}`,
  );
  appendGitHubOutput([
    "hourly_downloads_changed=true",
    `hourly_downloads_new=${totalNew}`,
  ]);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  runAndExitOnError(run);
}
