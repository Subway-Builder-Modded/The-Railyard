import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import {
  applyHourlySuppressions,
  computeListingDeltas,
  getHourlyShardRelativePath,
  groupHourlyRowsByMonth,
  HOURLY_DOWNLOADS_BACKFILL_FLOOR,
  HOURLY_DOWNLOADS_DIR_RELATIVE_PATH,
  HOURLY_SUPPRESSIONS_RELATIVE_PATH,
  mergeHourlyRows,
  parseHourlySuppressions,
  serializeHourlyDownloadsCsv,
  truncateToHourBucketUtc,
  type DownloadsFile,
  type HourlyDownloadRow,
  type HourlyListingType,
} from "../lib/hourly-downloads.js";
import { runGitCommand } from "../lib/git-history.js";
import { getFlagValue } from "../lib/cli.js";
import { resolveRepoRoot, runAndExitOnError } from "../lib/script-runtime.js";

// Rebuilds the hourly download series from git history: every hourly bot run
// commits downloads.json, so pairwise deltas between consecutive commits ARE
// the hourly series. Deterministic and idempotent — the monthly shards
// (downloads-YYYY-MM.csv) are regenerated wholesale for the window, so this is
// both the initial backfill and the disaster-recovery path (re-run after any
// history rewrite).
//
//   pnpm --dir scripts run backfill-hourly-downloads [-- --days 30]
//
// Without --days the window runs from HOURLY_DOWNLOADS_BACKFILL_FLOOR
// (2026-07-01, the Cloudflare Worker scheduler cutoff — earlier commits are
// too sparse for hour grain; see KNOWN_INCIDENTS.md). --days shortens the
// window; it can never extend past the floor.
//
// Requires full local history for the window (a shallow clone will silently
// truncate the series; the commit-count sanity check below guards this).

interface TypeSpec {
  listingType: HourlyListingType;
  relativePath: string;
}

const TYPE_SPECS: TypeSpec[] = [
  { listingType: "map", relativePath: "maps/downloads.json" },
  { listingType: "mod", relativePath: "mods/downloads.json" },
];

// Approximate hourly cadence; used only for the shallow-clone sanity check.
const MIN_EXPECTED_COMMITS_PER_DAY = 12;
const DAY_MS = 24 * 60 * 60 * 1000;

function parseWindowStartMs(argv: string[], nowMs: number): number {
  const floorMs = Date.parse(
    `${HOURLY_DOWNLOADS_BACKFILL_FLOOR.slice(0, 13)}:00:00Z`,
  );
  const value = getFlagValue(argv, "days");
  if (value === undefined) return floorMs;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0 || parsed > 365) {
    throw new Error(`Invalid --days '${value}'; expected 1-365.`);
  }
  return Math.max(nowMs - parsed * DAY_MS, floorMs);
}

interface CommitRef {
  sha: string;
  committedAt: string;
}

// Commits ascending in time, extended one commit before the window so the
// first in-window commit has a delta baseline.
function listCommitsForPath(repoRoot: string, relativePath: string, sinceMs: number): CommitRef[] {
  const sinceIso = new Date(sinceMs).toISOString();
  const output = runGitCommand(repoRoot, [
    "log", "--since", sinceIso, "--format=%H %cI", "--reverse", "--", relativePath,
  ]);
  const commits: CommitRef[] = (output ?? "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line !== "")
    .map((line) => {
      const [sha, committedAt] = line.split(" ") as [string, string];
      return { sha, committedAt };
    });

  const baseline = runGitCommand(repoRoot, [
    "log", "-1", "--before", sinceIso, "--format=%H %cI", "--", relativePath,
  ]);
  if (baseline) {
    const [sha, committedAt] = baseline.trim().split(" ") as [string, string];
    commits.unshift({ sha, committedAt });
  }
  return commits;
}

function readDownloadsAtCommit(
  repoRoot: string,
  sha: string,
  relativePath: string,
): DownloadsFile | null {
  const content = runGitCommand(repoRoot, ["show", `${sha}:${relativePath}`]);
  if (!content) return null;
  try {
    return JSON.parse(content) as DownloadsFile;
  } catch {
    return null;
  }
}

async function run(): Promise<void> {
  const repoRoot = process.env.RAILYARD_REPO_ROOT ?? resolveRepoRoot(import.meta.dirname);
  const nowMs = Date.now();
  const sinceMs = parseWindowStartMs(process.argv.slice(2), nowMs);
  const windowDays = Math.max(1, Math.round((nowMs - sinceMs) / DAY_MS));
  const windowStart = truncateToHourBucketUtc(new Date(sinceMs).toISOString());

  let rows: HourlyDownloadRow[] = [];
  for (const spec of TYPE_SPECS) {
    const commits = listCommitsForPath(repoRoot, spec.relativePath, sinceMs);
    if (commits.length < windowDays * MIN_EXPECTED_COMMITS_PER_DAY) {
      throw new Error(
        `Only ${commits.length} commits found for ${spec.relativePath} over ${windowDays}d `
        + `(expected >= ${windowDays * MIN_EXPECTED_COMMITS_PER_DAY}); shallow clone or wrong branch?`,
      );
    }
    let previous = readDownloadsAtCommit(repoRoot, commits[0]!.sha, spec.relativePath);
    let pairs = 0;
    for (const commit of commits.slice(1)) {
      const next = readDownloadsAtCommit(repoRoot, commit.sha, spec.relativePath);
      if (!next) continue;
      const bucket = truncateToHourBucketUtc(commit.committedAt);
      if (previous && bucket >= windowStart) {
        const additions = computeListingDeltas(spec.listingType, previous, next).map(
          (delta) => ({ bucket_utc: bucket, ...delta }),
        );
        rows = mergeHourlyRows(rows, additions);
        pairs += 1;
      }
      previous = next;
    }
    console.log(`[backfill-hourly-downloads] ${spec.listingType}s: ${commits.length} commits, ${pairs} in-window deltas`);
  }

  const suppressionsPath = resolve(repoRoot, ...HOURLY_SUPPRESSIONS_RELATIVE_PATH.split("/"));
  if (existsSync(suppressionsPath)) {
    const suppressions = parseHourlySuppressions(JSON.parse(readFileSync(suppressionsPath, "utf-8")));
    const applied = applyHourlySuppressions(rows, suppressions);
    rows = applied.rows;
    console.log(`[backfill-hourly-downloads] applied ${applied.suppressed} committed suppression(s)`);
  }

  // Regenerate the shard set wholesale: write every month in the window and
  // remove stray shard files the window no longer produces.
  const hourlyDir = resolve(repoRoot, ...HOURLY_DOWNLOADS_DIR_RELATIVE_PATH.split("/"));
  mkdirSync(hourlyDir, { recursive: true });
  const byMonth = groupHourlyRowsByMonth(rows);
  const writtenFiles = new Set<string>();
  for (const [month, monthRows] of [...byMonth.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    const shardPath = resolve(repoRoot, ...getHourlyShardRelativePath(month).split("/"));
    writeFileSync(shardPath, serializeHourlyDownloadsCsv(monthRows), "utf-8");
    writtenFiles.add(`downloads-${month}.csv`);
    console.log(`[backfill-hourly-downloads] shard ${month}: ${monthRows.length} rows`);
  }
  for (const fileName of readdirSync(hourlyDir)) {
    if (/^downloads-\d{4}-\d{2}\.csv$/.test(fileName) && !writtenFiles.has(fileName)) {
      rmSync(resolve(hourlyDir, fileName));
      console.log(`[backfill-hourly-downloads] removed stale shard ${fileName}`);
    }
  }

  const total = rows.reduce((sum, row) => sum + row.downloads, 0);
  const buckets = new Set(rows.map((row) => row.bucket_utc)).size;
  console.log(
    `[backfill-hourly-downloads] wrote ${rows.length} rows across ${buckets} hour buckets in ${byMonth.size} shard(s) (${total} downloads, window ${windowDays}d from ${windowStart})`,
  );
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  runAndExitOnError(run);
}
