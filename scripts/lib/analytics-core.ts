import { existsSync, mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { loadAuthorAliasIndex } from "./author-aliases.js";
import { getFlagValue } from "./cli.js";
import { writeCsv } from "./csv.js";
import { readJsonFile } from "./json-utils.js";
import { resolveRepoRoot } from "./script-runtime.js";
import { compareStableSemverAsc } from "./semver.js";
import { isTestListing } from "./test-listings.js";
import {
  buildVersionCreditResolver,
  type CreditedPersonPresentation,
} from "./analytics/credited-person.js";
import {
  buildAssetsByDayRows,
  buildAuthorByDayRows,
  buildDailyDeltaSnapshotTotals,
  buildListingByDayRows,
  buildListingVersionByDayRows,
  buildProjectByDayRows,
  buildSignedDailyDeltaSnapshotTotals,
  toSnapshotDateLabel,
} from "./analytics/by-day.js";
import {
  filterOutTestListingTotals,
  loadListingProjectRow,
  loadManifestMeta,
  toListingLabel,
} from "./analytics/listing-meta.js";
import { loadMapStatisticsRows } from "./analytics/map-statistics.js";
import {
  buildListingIdsBySnapshot,
  buildListingVersionsBySnapshot,
  buildMonotonicSnapshotTotals,
  buildVersionGrainSnapshotTotals,
  listSnapshots,
  resolveBaselineSnapshot,
  toListingTotals,
} from "./analytics/snapshots.js";
import type {
  AssetByDayRow,
  AuthorCreditEntry,
  DailySeriesRow,
  ListingKey,
  ListingMeta,
  ListingProjectRow,
  ListingTotals,
  MapStatisticsRow,
  SnapshotData,
} from "./analytics/types.js";

interface ListingWindowRow {
  rank: number;
  listing_type: "map" | "mod";
  id: string;
  name: string;
  author: string;
  author_alias: string;
  attribution_link: string;
  download_change: number;
  adjusted_download_change: number;
  current_total: number;
  adjusted_current_total: number;
  baseline_total: number;
  adjusted_baseline_total: number;
  latest_snapshot: string;
  baseline_snapshot: string;
}

interface ListingAllTimeRow {
  rank: number;
  listing_type: "map" | "mod";
  id: string;
  name: string;
  author: string;
  author_alias: string;
  attribution_link: string;
  total_downloads: number;
  adjusted_total_downloads: number;
  latest_snapshot: string;
}

interface ProjectWindowRow {
  rank: number;
  project_key: string;
  project_name: string;
  author: string;
  author_alias: string;
  attribution_link: string;
  listing_count: number;
  download_change: number;
  adjusted_download_change: number;
  current_total: number;
  adjusted_current_total: number;
  baseline_total: number;
  adjusted_baseline_total: number;
  latest_snapshot: string;
  baseline_snapshot: string;
  // Per-asset-type splits of the window movement. Appended last so existing
  // positional consumers keep working.
  map_download_change: number;
  adjusted_map_download_change: number;
  mod_download_change: number;
  adjusted_mod_download_change: number;
}

interface ProjectAllTimeRow {
  rank: number;
  project_key: string;
  project_name: string;
  author: string;
  author_alias: string;
  attribution_link: string;
  listing_count: number;
  total_downloads: number;
  adjusted_total_downloads: number;
  latest_snapshot: string;
}

interface AuthorAssetCountRow {
  rank: number;
  author: string;
  author_alias: string;
  attribution_link: string;
  asset_count: number;
  map_count: number;
  mod_count: number;
  total_downloads: number;
  adjusted_total_downloads: number;
  // Number of listings where this person is the ACTIVE caretaker (no `until`).
  // Appended last so existing positional consumers keep working.
  caretaken_asset_count: number;
}

interface ListingVersionCreditRow {
  listing_type: "map" | "mod";
  listing_id: string;
  version: string;
  credited_author_id: string;
}

interface AuthorTotalDownloadsRow {
  rank: number;
  author: string;
  author_alias: string;
  attribution_link: string;
  total_downloads: number;
  adjusted_total_downloads: number;
  asset_count: number;
  map_count: number;
  mod_count: number;
}

interface AuthorWindowRow {
  rank: number;
  author: string;
  author_alias: string;
  attribution_link: string;
  asset_count: number;
  map_count: number;
  mod_count: number;
  download_change: number;
  adjusted_download_change: number;
  current_total: number;
  adjusted_current_total: number;
  baseline_total: number;
  adjusted_baseline_total: number;
  latest_snapshot: string;
  baseline_snapshot: string;
  // Per-asset-type splits of the window movement. Appended last so existing
  // positional consumers keep working.
  map_download_change: number;
  adjusted_map_download_change: number;
  mod_download_change: number;
  adjusted_mod_download_change: number;
}

const DEFAULT_TOP_LISTINGS: number | null = null;
const DEFAULT_TOP_AUTHORS: number | null = null;
const WINDOWS = [1, 3, 7, 14, 30] as const;

function validateArgs(argv: string[]): void {
  const valueFlags = new Set(["--top-k-listings", "--top-k-authors"]);
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]!;
    if (arg === "--") continue;
    if (arg.startsWith("--top-k-listings=") || arg.startsWith("--top-k-authors=")) continue;
    if (valueFlags.has(arg)) {
      index += 1;
      continue;
    }
    throw new Error(
      `Unknown argument '${arg}'. Supported flags: --top-k-listings <n>, --top-k-authors <n>.`,
    );
  }
}

function parseTopK(rawValue: string | undefined, fallback: number | null, label: string): number | null {
  if (!rawValue || rawValue.trim() === "") return fallback;
  if (!/^\d+$/.test(rawValue.trim())) {
    throw new Error(`Invalid ${label} value '${rawValue}'. Expected a non-negative integer.`);
  }
  const parsed = Number(rawValue);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid ${label} value '${rawValue}'. Expected a non-negative integer.`);
  }
  return parsed === 0 ? null : parsed;
}

function limitRows<T>(rows: T[], topK: number | null): T[] {
  return topK === null ? rows : rows.slice(0, topK);
}

export function runGenerateAnalyticsCli(
  argv = process.argv.slice(2),
  repoRoot?: string,
): void {
  const resolvedRepoRoot = repoRoot ?? process.env.RAILYARD_REPO_ROOT ?? resolveRepoRoot(import.meta.dirname);
  validateArgs(argv);
  const topListings = parseTopK(
    getFlagValue(argv, "top-k-listings")
      ?? process.env.ANALYTICS_TOP_K_LISTINGS
      ?? process.env.ANALYTICS_TOP_K,
    DEFAULT_TOP_LISTINGS,
    "top-k-listings",
  );
  const topAuthors = parseTopK(
    getFlagValue(argv, "top-k-authors") ?? process.env.ANALYTICS_TOP_K_AUTHORS,
    DEFAULT_TOP_AUTHORS,
    "top-k-authors",
  );
  const historyDir = join(resolvedRepoRoot, "history");
  const analyticsDir = join(resolvedRepoRoot, "analytics");
  mkdirSync(analyticsDir, { recursive: true });
  const authorAliases = loadAuthorAliasIndex(resolvedRepoRoot);

  const snapshots = listSnapshots(historyDir);
  if (snapshots.length === 0) {
    throw new Error(`No snapshots found in ${historyDir}`);
  }

  const latest = snapshots[snapshots.length - 1];
  const adjustedTotalsBySnapshot = new Map<string, ListingTotals>();
  const getAdjustedTotalsForSnapshot = (snapshotFile: string): ListingTotals => {
    const cached = adjustedTotalsBySnapshot.get(snapshotFile);
    if (cached) return cached;
    const totals = filterOutTestListingTotals(
      resolvedRepoRoot,
      toListingTotals(readJsonFile<SnapshotData>(join(historyDir, snapshotFile))),
    );
    adjustedTotalsBySnapshot.set(snapshotFile, totals);
    return totals;
  };
  const latestAdjustedTotals = getAdjustedTotalsForSnapshot(latest.file);
  for (const snapshot of snapshots) {
    getAdjustedTotalsForSnapshot(snapshot.file);
  }
  const monotonicTotalsBySnapshot = buildMonotonicSnapshotTotals(snapshots, historyDir);
  const dailyDeltasBySnapshot = buildDailyDeltaSnapshotTotals(snapshots, monotonicTotalsBySnapshot);
  const signedDailyDeltasBySnapshot = buildSignedDailyDeltaSnapshotTotals(snapshots, adjustedTotalsBySnapshot);
  const filteredDailyDeltasBySnapshot = new Map<string, ListingTotals>();
  const filteredSignedDailyDeltasBySnapshot = new Map<string, ListingTotals>();
  for (const snapshot of snapshots) {
    filteredDailyDeltasBySnapshot.set(
      snapshot.file,
      filterOutTestListingTotals(
        resolvedRepoRoot,
        dailyDeltasBySnapshot.get(snapshot.file) ?? new Map<ListingKey, number>(),
      ),
    );
    filteredSignedDailyDeltasBySnapshot.set(
      snapshot.file,
      filterOutTestListingTotals(
        resolvedRepoRoot,
        signedDailyDeltasBySnapshot.get(snapshot.file) ?? new Map<ListingKey, number>(),
      ),
    );
  }
  const listingIdsBySnapshot = buildListingIdsBySnapshot(snapshots, historyDir, resolvedRepoRoot);
  const listingVersionsBySnapshot = buildListingVersionsBySnapshot(
    snapshots,
    historyDir,
    resolvedRepoRoot,
  );
  const snapshotDates = snapshots.map((snapshot) => toSnapshotDateLabel(snapshot.file));
  const latestTotals = filterOutTestListingTotals(
    resolvedRepoRoot,
    monotonicTotalsBySnapshot.get(latest.file) ?? new Map<ListingKey, number>(),
  );

  const listingMeta = new Map<ListingKey, ListingMeta>();
  for (const key of latestAdjustedTotals.keys()) {
    const [listingType, id] = key.split(":") as ["maps" | "mods", string];
    listingMeta.set(key, loadManifestMeta(resolvedRepoRoot, listingType, id, authorAliases));
  }

  // --- Caretaker download crediting (author aggregations only) ---
  // Each listing is partitioned into "credit units": groups of versions credited
  // to the same person per the [since, until)/released_at rule. Listings whose
  // versions all credit one person (every listing without caretakers, and
  // caretaker-since-epoch listings) keep listing-grain totals; only listings
  // split between persons use (listing, version)-grain monotonic/delta math.
  const creditResolver = buildVersionCreditResolver({
    repoRoot: resolvedRepoRoot,
    authorAliases,
  });
  const UNKNOWN_PERSON: CreditedPersonPresentation = {
    author: "UNKNOWN",
    author_alias: "UNKNOWN",
    attribution_link: "https://github.com/UNKNOWN",
  };

  // Union of versions per listing across all history snapshots.
  const historyVersionsByListing = new Map<ListingKey, Set<string>>();
  for (const versionSets of listingVersionsBySnapshot.values()) {
    for (const listingType of ["maps", "mods"] as const) {
      for (const versionKey of versionSets[listingType]) {
        const separatorIndex = versionKey.indexOf("@@");
        if (separatorIndex === -1) continue;
        const key: ListingKey = `${listingType}:${versionKey.slice(0, separatorIndex)}`;
        let versions = historyVersionsByListing.get(key);
        if (!versions) {
          versions = new Set<string>();
          historyVersionsByListing.set(key, versions);
        }
        versions.add(versionKey.slice(separatorIndex + 2));
      }
    }
  }

  interface ListingCreditUnit {
    person: CreditedPersonPresentation;
    // null = all of the listing's versions (use listing-grain totals unchanged).
    versions: Set<string> | null;
  }
  const creditUnitsByListing = new Map<ListingKey, ListingCreditUnit[]>();
  for (const key of latestTotals.keys()) {
    const [listingType, id] = key.split(":") as ["maps" | "mods", string];
    const meta = listingMeta.get(key);
    const authorPerson: CreditedPersonPresentation = meta
      ? { author: meta.author, author_alias: meta.author_alias, attribution_link: meta.attribution_link }
      : UNKNOWN_PERSON;
    let units: ListingCreditUnit[] = [{ person: authorPerson, versions: null }];
    if (creditResolver.hasCaretakers(listingType, id)) {
      const versions = [...(historyVersionsByListing.get(key) ?? new Set<string>())]
        .sort(compareStableSemverAsc);
      const groups = new Map<string, { person: CreditedPersonPresentation; versions: Set<string> }>();
      for (const version of versions) {
        const person = creditResolver.resolvePresentation(listingType, id, version, authorPerson);
        const group = groups.get(person.author) ?? { person, versions: new Set<string>() };
        group.versions.add(version);
        groups.set(person.author, group);
      }
      if (groups.size === 1) {
        units = [{ person: [...groups.values()][0]!.person, versions: null }];
      } else if (groups.size > 1) {
        units = [...groups.values()].map((group) => ({ person: group.person, versions: group.versions }));
      }
    }
    creditUnitsByListing.set(key, units);
  }

  // Version grain across every listing: feeds asset_versions_by_day.csv, and
  // the caretaker credit split below sums the same totals for its split
  // listings (per-key math is independent, so widening the key set does not
  // change the split listings' values).
  const versionGrain = buildVersionGrainSnapshotTotals(
    snapshots,
    historyDir,
    new Set(latestTotals.keys()),
  );
  const sumOverVersionKeys = (
    totals: Map<string, number> | undefined,
    versionKeys: readonly string[],
  ): number => {
    if (!totals) return 0;
    let total = 0;
    for (const versionKey of versionKeys) {
      total += totals.get(versionKey) ?? 0;
    }
    return total;
  };

  const authorCreditEntries: AuthorCreditEntry[] = [];
  for (const [key, currentTotal] of latestTotals.entries()) {
    const [listingType] = key.split(":") as ["maps" | "mods", string];
    for (const unit of creditUnitsByListing.get(key) ?? []) {
      if (unit.versions === null) {
        authorCreditEntries.push({
          listingType,
          person: unit.person,
          currentTotal,
          currentAdjusted: latestAdjustedTotals.get(key) ?? 0,
          monotonicAt: (file) => monotonicTotalsBySnapshot.get(file)?.get(key) ?? 0,
          adjustedAt: (file) => getAdjustedTotalsForSnapshot(file).get(key) ?? 0,
          deltaAt: (file) => filteredDailyDeltasBySnapshot.get(file)?.get(key) ?? 0,
        });
      } else {
        const versionKeys = [...unit.versions].map((version) => `${key}@@${version}`);
        authorCreditEntries.push({
          listingType,
          person: unit.person,
          currentTotal: sumOverVersionKeys(versionGrain.monotonicBySnapshot.get(latest.file), versionKeys),
          currentAdjusted: sumOverVersionKeys(versionGrain.adjustedBySnapshot.get(latest.file), versionKeys),
          monotonicAt: (file) => sumOverVersionKeys(versionGrain.monotonicBySnapshot.get(file), versionKeys),
          adjustedAt: (file) => sumOverVersionKeys(versionGrain.adjustedBySnapshot.get(file), versionKeys),
          deltaAt: (file) => sumOverVersionKeys(versionGrain.dailyDeltasBySnapshot.get(file), versionKeys),
        });
      }
    }
  }

  const listingProjectRows: ListingProjectRow[] = [...latestAdjustedTotals.keys()]
    .map((key) => {
      const [listingType, id] = key.split(":") as ["maps" | "mods", string];
      return loadListingProjectRow(resolvedRepoRoot, listingType, id);
    })
    .sort((a, b) =>
      a.project_key.localeCompare(b.project_key)
      || a.listing_type.localeCompare(b.listing_type)
      || a.id.localeCompare(b.id));

  const listingProjectByKey = new Map<ListingKey, ListingProjectRow>();
  for (const row of listingProjectRows) {
    const listingType = row.listing_type === "map" ? "maps" : "mods";
    listingProjectByKey.set(`${listingType}:${row.id}`, row);
  }

  const rowsForWindow = (days: number): ListingWindowRow[] => {
    const baseline = resolveBaselineSnapshot(snapshots, latest.date, days);
    const baselineAdjustedTotals = getAdjustedTotalsForSnapshot(baseline.file);
    const baselineTotals = filterOutTestListingTotals(
      resolvedRepoRoot,
      monotonicTotalsBySnapshot.get(baseline.file) ?? new Map<ListingKey, number>(),
    );

    const rows: Omit<ListingWindowRow, "rank">[] = [];
    for (const [key, currentTotal] of latestTotals.entries()) {
      const currentAdjustedTotal = latestAdjustedTotals.get(key) ?? 0;
      const baselineTotal = baselineTotals.get(key) ?? 0;
      const baselineAdjustedTotal = baselineAdjustedTotals.get(key) ?? 0;
      const change = currentTotal - baselineTotal;
      const adjustedChange = currentAdjustedTotal - baselineAdjustedTotal;
      const [listingType, id] = key.split(":") as ["maps" | "mods", string];
      const meta = listingMeta.get(key) ?? {
        name: id,
        author: "UNKNOWN",
        author_alias: "UNKNOWN",
        attribution_link: "https://github.com/UNKNOWN",
        github_id: null,
      };
      rows.push({
        listing_type: toListingLabel(listingType),
        id,
        name: meta.name,
        author: meta.author,
        author_alias: meta.author_alias,
        attribution_link: meta.attribution_link,
        download_change: change,
        adjusted_download_change: adjustedChange,
        current_total: currentTotal,
        adjusted_current_total: currentAdjustedTotal,
        baseline_total: baselineTotal,
        adjusted_baseline_total: baselineAdjustedTotal,
        latest_snapshot: latest.file,
        baseline_snapshot: baseline.file,
      });
    }

    rows.sort((a, b) =>
      b.download_change - a.download_change
      || b.current_total - a.current_total
      || a.id.localeCompare(b.id));

    return limitRows(rows, topListings).map((row, index) => ({
      rank: index + 1,
      ...row,
    }));
  };

  const allTimeRows = (() => {
    const rows: Omit<ListingAllTimeRow, "rank">[] = [];
    for (const [key, total] of latestTotals.entries()) {
      const [listingType, id] = key.split(":") as ["maps" | "mods", string];
      const meta = listingMeta.get(key) ?? {
        name: id,
        author: "UNKNOWN",
        author_alias: "UNKNOWN",
        attribution_link: "https://github.com/UNKNOWN",
        github_id: null,
      };
      rows.push({
        listing_type: toListingLabel(listingType),
        id,
        name: meta.name,
        author: meta.author,
        author_alias: meta.author_alias,
        attribution_link: meta.attribution_link,
        total_downloads: total,
        adjusted_total_downloads: latestAdjustedTotals.get(key) ?? 0,
        latest_snapshot: latest.file,
      });
    }
    rows.sort((a, b) => b.total_downloads - a.total_downloads || a.id.localeCompare(b.id));
    return limitRows(rows, topListings).map((row, index) => ({
      rank: index + 1,
      ...row,
    }));
  })();

  const projectRowsForWindow = (days: number): ProjectWindowRow[] => {
    const baseline = resolveBaselineSnapshot(snapshots, latest.date, days);
    const baselineAdjustedTotals = getAdjustedTotalsForSnapshot(baseline.file);
    const baselineTotals = filterOutTestListingTotals(
      resolvedRepoRoot,
      monotonicTotalsBySnapshot.get(baseline.file) ?? new Map<ListingKey, number>(),
    );

    const projectStats = new Map<string, Omit<ProjectWindowRow, "rank">>();
    for (const [key, currentTotal] of latestTotals.entries()) {
      const listingProject = listingProjectByKey.get(key);
      if (!listingProject) continue;
      const baselineTotal = baselineTotals.get(key) ?? 0;
      const currentAdjustedTotal = latestAdjustedTotals.get(key) ?? 0;
      const baselineAdjustedTotal = baselineAdjustedTotals.get(key) ?? 0;
      const existing = projectStats.get(listingProject.project_key) ?? {
        project_key: listingProject.project_key,
        project_name: listingProject.project_name,
        author: "",
        author_alias: "",
        attribution_link: "",
        listing_count: 0,
        download_change: 0,
        adjusted_download_change: 0,
        current_total: 0,
        adjusted_current_total: 0,
        baseline_total: 0,
        adjusted_baseline_total: 0,
        latest_snapshot: latest.file,
        baseline_snapshot: baseline.file,
        map_download_change: 0,
        adjusted_map_download_change: 0,
        mod_download_change: 0,
        adjusted_mod_download_change: 0,
      };
      const meta = listingMeta.get(key) ?? {
        name: "",
        author: "UNKNOWN",
        author_alias: "UNKNOWN",
        attribution_link: "https://github.com/UNKNOWN",
        github_id: null,
      };
      const authors = new Set(existing.author.split("; ").filter(Boolean));
      const authorAliases = new Set(existing.author_alias.split("; ").filter(Boolean));
      const attributionLinks = new Set(existing.attribution_link.split("; ").filter(Boolean));
      authors.add(meta.author);
      authorAliases.add(meta.author_alias);
      attributionLinks.add(meta.attribution_link);
      existing.author = [...authors].sort().join("; ");
      existing.author_alias = [...authorAliases].sort().join("; ");
      existing.attribution_link = [...attributionLinks].sort().join("; ");
      existing.listing_count += 1;
      existing.current_total += currentTotal;
      existing.adjusted_current_total += currentAdjustedTotal;
      existing.baseline_total += baselineTotal;
      existing.adjusted_baseline_total += baselineAdjustedTotal;
      existing.download_change += currentTotal - baselineTotal;
      existing.adjusted_download_change += currentAdjustedTotal - baselineAdjustedTotal;
      if (key.startsWith("maps:")) {
        existing.map_download_change += currentTotal - baselineTotal;
        existing.adjusted_map_download_change += currentAdjustedTotal - baselineAdjustedTotal;
      } else {
        existing.mod_download_change += currentTotal - baselineTotal;
        existing.adjusted_mod_download_change += currentAdjustedTotal - baselineAdjustedTotal;
      }
      projectStats.set(listingProject.project_key, existing);
    }

    const rows = [...projectStats.values()]
      .sort((a, b) =>
        b.download_change - a.download_change
        || b.current_total - a.current_total
        || a.project_key.localeCompare(b.project_key));

    return limitRows(rows, topListings).map((row, index) => ({
      rank: index + 1,
      ...row,
    }));
  };

  const projectAllTimeRows = (() => {
    const projectStats = new Map<string, Omit<ProjectAllTimeRow, "rank">>();
    for (const [key, total] of latestTotals.entries()) {
      const listingProject = listingProjectByKey.get(key);
      if (!listingProject) continue;
      const existing = projectStats.get(listingProject.project_key) ?? {
        project_key: listingProject.project_key,
        project_name: listingProject.project_name,
        author: "",
        author_alias: "",
        attribution_link: "",
        listing_count: 0,
        total_downloads: 0,
        adjusted_total_downloads: 0,
        latest_snapshot: latest.file,
      };
      const meta = listingMeta.get(key) ?? {
        name: "",
        author: "UNKNOWN",
        author_alias: "UNKNOWN",
        attribution_link: "https://github.com/UNKNOWN",
        github_id: null,
      };
      const authors = new Set(existing.author.split("; ").filter(Boolean));
      const authorAliases = new Set(existing.author_alias.split("; ").filter(Boolean));
      const attributionLinks = new Set(existing.attribution_link.split("; ").filter(Boolean));
      authors.add(meta.author);
      authorAliases.add(meta.author_alias);
      attributionLinks.add(meta.attribution_link);
      existing.author = [...authors].sort().join("; ");
      existing.author_alias = [...authorAliases].sort().join("; ");
      existing.attribution_link = [...attributionLinks].sort().join("; ");
      existing.listing_count += 1;
      existing.total_downloads += total;
      existing.adjusted_total_downloads += latestAdjustedTotals.get(key) ?? 0;
      projectStats.set(listingProject.project_key, existing);
    }

    const rows = [...projectStats.values()]
      .sort((a, b) =>
        b.total_downloads - a.total_downloads
        || b.listing_count - a.listing_count
        || a.project_key.localeCompare(b.project_key));

    return limitRows(rows, topListings).map((row, index) => ({
      rank: index + 1,
      ...row,
    }));
  })();

  // authors_by_asset_count stays AUTHORSHIP-based (assets owned); caretaker
  // crediting only adds the caretaken_asset_count column.
  const authorStats = new Map<string, Omit<AuthorAssetCountRow, "rank">>();
  for (const [key, total] of latestTotals.entries()) {
    const [listingType] = key.split(":") as ["maps" | "mods", string];
    const meta = listingMeta.get(key) ?? {
      name: "",
      author: "UNKNOWN",
      author_alias: "UNKNOWN",
      attribution_link: "https://github.com/UNKNOWN",
      github_id: null,
    };
    const previous = authorStats.get(meta.author) ?? {
      author: meta.author,
      author_alias: meta.author_alias,
      attribution_link: meta.attribution_link,
      asset_count: 0,
      map_count: 0,
      mod_count: 0,
      total_downloads: 0,
      adjusted_total_downloads: 0,
      caretaken_asset_count: 0,
    };
    previous.asset_count += 1;
    if (listingType === "maps") previous.map_count += 1;
    if (listingType === "mods") previous.mod_count += 1;
    previous.total_downloads += total;
    previous.adjusted_total_downloads += latestAdjustedTotals.get(key) ?? 0;
    authorStats.set(meta.author, previous);
  }

  // caretaken_asset_count: listings whose manifest declares this person as the
  // ACTIVE caretaker (entry without `until`). Persons who caretake but author
  // nothing still get a row (zeros in the authorship columns).
  for (const key of latestTotals.keys()) {
    const [listingType, id] = key.split(":") as ["maps" | "mods", string];
    const active = creditResolver.activeCaretaker(listingType, id);
    if (!active) continue;
    const existing = authorStats.get(active.author) ?? {
      author: active.author,
      author_alias: active.author_alias,
      attribution_link: active.attribution_link,
      asset_count: 0,
      map_count: 0,
      mod_count: 0,
      total_downloads: 0,
      adjusted_total_downloads: 0,
      caretaken_asset_count: 0,
    };
    existing.caretaken_asset_count += 1;
    authorStats.set(active.author, existing);
  }

  // authors_by_total_downloads aggregates per CREDITED person (caretaker
  // crediting); identical to authorStats whenever no listing splits credit.
  const creditedAuthorStats = new Map<string, Omit<AuthorTotalDownloadsRow, "rank">>();
  for (const entry of authorCreditEntries) {
    const previous = creditedAuthorStats.get(entry.person.author) ?? {
      author: entry.person.author,
      author_alias: entry.person.author_alias,
      attribution_link: entry.person.attribution_link,
      total_downloads: 0,
      adjusted_total_downloads: 0,
      asset_count: 0,
      map_count: 0,
      mod_count: 0,
    };
    previous.asset_count += 1;
    if (entry.listingType === "maps") previous.map_count += 1;
    if (entry.listingType === "mods") previous.mod_count += 1;
    previous.total_downloads += entry.currentTotal;
    previous.adjusted_total_downloads += entry.currentAdjusted;
    creditedAuthorStats.set(entry.person.author, previous);
  }

  // Author windows aggregate per CREDITED person (caretaker crediting) at
  // credit-unit grain; identical to the old listing-author rollup whenever no
  // listing splits credit between persons.
  const authorRowsForWindow = (days: number): AuthorWindowRow[] => {
    const baseline = resolveBaselineSnapshot(snapshots, latest.date, days);
    const authorWindowStats = new Map<string, Omit<AuthorWindowRow, "rank">>();

    for (const entry of authorCreditEntries) {
      const currentTotal = entry.currentTotal;
      const baselineTotal = entry.monotonicAt(baseline.file);
      const currentAdjustedTotal = entry.currentAdjusted;
      const baselineAdjustedTotal = entry.adjustedAt(baseline.file);
      const existing = authorWindowStats.get(entry.person.author) ?? {
        author: entry.person.author,
        author_alias: entry.person.author_alias,
        attribution_link: entry.person.attribution_link,
        asset_count: 0,
        map_count: 0,
        mod_count: 0,
        download_change: 0,
        adjusted_download_change: 0,
        current_total: 0,
        adjusted_current_total: 0,
        baseline_total: 0,
        adjusted_baseline_total: 0,
        latest_snapshot: latest.file,
        baseline_snapshot: baseline.file,
        map_download_change: 0,
        adjusted_map_download_change: 0,
        mod_download_change: 0,
        adjusted_mod_download_change: 0,
      };
      existing.asset_count += 1;
      if (entry.listingType === "maps") existing.map_count += 1;
      if (entry.listingType === "mods") existing.mod_count += 1;
      existing.download_change += currentTotal - baselineTotal;
      existing.adjusted_download_change += currentAdjustedTotal - baselineAdjustedTotal;
      if (entry.listingType === "maps") {
        existing.map_download_change += currentTotal - baselineTotal;
        existing.adjusted_map_download_change += currentAdjustedTotal - baselineAdjustedTotal;
      }
      if (entry.listingType === "mods") {
        existing.mod_download_change += currentTotal - baselineTotal;
        existing.adjusted_mod_download_change += currentAdjustedTotal - baselineAdjustedTotal;
      }
      existing.current_total += currentTotal;
      existing.adjusted_current_total += currentAdjustedTotal;
      existing.baseline_total += baselineTotal;
      existing.adjusted_baseline_total += baselineAdjustedTotal;
      authorWindowStats.set(entry.person.author, existing);
    }

    const rows = [...authorWindowStats.values()]
      .sort((a, b) =>
        b.download_change - a.download_change
        || b.current_total - a.current_total
        || a.author.localeCompare(b.author));
    return limitRows(rows, topAuthors).map((row, index) => ({
      rank: index + 1,
      ...row,
    }));
  };

  const authorRowsByAssetCount: AuthorAssetCountRow[] = [...authorStats.values()]
    .sort((a, b) =>
      b.asset_count - a.asset_count
      || b.total_downloads - a.total_downloads
      || a.author.localeCompare(b.author))
    .slice(0, topAuthors ?? authorStats.size)
    .map((row, index) => ({ rank: index + 1, ...row }));

  const authorRowsByTotalDownloads: AuthorTotalDownloadsRow[] = [...creditedAuthorStats.values()]
    .sort((a, b) =>
      b.total_downloads - a.total_downloads
      || b.asset_count - a.asset_count
      || a.author.localeCompare(b.author))
    .slice(0, topAuthors ?? creditedAuthorStats.size)
    .map((row, index) => ({
      rank: index + 1,
      author: row.author,
      author_alias: row.author_alias,
      attribution_link: row.attribution_link,
      total_downloads: row.total_downloads,
      adjusted_total_downloads: row.adjusted_total_downloads,
      asset_count: row.asset_count,
      map_count: row.map_count,
      mod_count: row.mod_count,
    }));

  const listingByDayRows = buildListingByDayRows(
    snapshotDates,
    latestTotals,
    filteredDailyDeltasBySnapshot,
    listingMeta,
    listingProjectByKey,
  );
  const projectByDayRows = buildProjectByDayRows(
    snapshotDates,
    latestTotals,
    filteredDailyDeltasBySnapshot,
    listingMeta,
    listingProjectByKey,
  );
  const authorByDayRows = buildAuthorByDayRows(snapshotDates, authorCreditEntries);

  // Per-version credited person for every version in the current downloads.json
  // data (test listings excluded, like every other analytics artifact). Lets
  // consumers (website author pages) derive credited per-author totals without
  // recomputing caretaker windows.
  const listingVersionCreditRows: ListingVersionCreditRow[] = [];
  for (const listingType of ["maps", "mods"] as const) {
    const downloadsPath = join(resolvedRepoRoot, listingType, "downloads.json");
    if (!existsSync(downloadsPath)) continue;
    let downloadsByListing: Record<string, Record<string, unknown>>;
    try {
      downloadsByListing = readJsonFile<Record<string, Record<string, unknown>>>(downloadsPath);
    } catch {
      continue;
    }
    for (const [id, versions] of Object.entries(downloadsByListing)) {
      if (!versions || typeof versions !== "object") continue;
      if (isTestListing(resolvedRepoRoot, listingType, id)) continue;
      const key: ListingKey = `${listingType}:${id}`;
      const authorId = (listingMeta.get(key)
        ?? loadManifestMeta(resolvedRepoRoot, listingType, id, authorAliases)).author;
      for (const version of Object.keys(versions)) {
        listingVersionCreditRows.push({
          listing_type: toListingLabel(listingType),
          listing_id: id,
          version,
          credited_author_id: creditResolver.resolveAuthorId(listingType, id, version, authorId),
        });
      }
    }
  }
  listingVersionCreditRows.sort((a, b) =>
    a.listing_type.localeCompare(b.listing_type)
    || a.listing_id.localeCompare(b.listing_id)
    || compareStableSemverAsc(a.version, b.version));
  const listingVersionByDayRows = buildListingVersionByDayRows(
    snapshotDates,
    latestTotals,
    historyVersionsByListing,
    versionGrain,
    latest.file,
  );
  const assetsByDayRows = buildAssetsByDayRows(
    snapshotDates,
    filteredDailyDeltasBySnapshot,
    filteredSignedDailyDeltasBySnapshot,
    listingIdsBySnapshot,
    listingVersionsBySnapshot,
  );

  const listingWindowColumns = [
    "rank",
    "listing_type",
    "id",
    "name",
    "author",
    "author_alias",
    "attribution_link",
    "download_change",
    "adjusted_download_change",
    "current_total",
    "adjusted_current_total",
    "baseline_total",
    "adjusted_baseline_total",
    "latest_snapshot",
    "baseline_snapshot",
  ] as const;
  const projectWindowColumns = [
    "rank",
    "project_key",
    "project_name",
    "author",
    "author_alias",
    "attribution_link",
    "listing_count",
    "download_change",
    "adjusted_download_change",
    "current_total",
    "adjusted_current_total",
    "baseline_total",
    "adjusted_baseline_total",
    "latest_snapshot",
    "baseline_snapshot",
    "map_download_change",
    "adjusted_map_download_change",
    "mod_download_change",
    "adjusted_mod_download_change",
  ] as const;
  const authorWindowColumns = [
    "rank",
    "author",
    "author_alias",
    "attribution_link",
    "asset_count",
    "map_count",
    "mod_count",
    "download_change",
    "adjusted_download_change",
    "current_total",
    "adjusted_current_total",
    "baseline_total",
    "adjusted_baseline_total",
    "latest_snapshot",
    "baseline_snapshot",
    "map_download_change",
    "adjusted_map_download_change",
    "mod_download_change",
    "adjusted_mod_download_change",
  ] as const;

  for (const days of WINDOWS) {
    writeCsv<ListingWindowRow>(
      join(analyticsDir, `most_popular_last_${days}d.csv`),
      [...listingWindowColumns],
      rowsForWindow(days),
    );
  }

  for (const days of WINDOWS) {
    writeCsv<ProjectWindowRow>(
      join(analyticsDir, `projects_most_popular_last_${days}d.csv`),
      [...projectWindowColumns],
      projectRowsForWindow(days),
    );
  }

  for (const days of WINDOWS) {
    writeCsv<AuthorWindowRow>(
      join(analyticsDir, `authors_last_${days}d.csv`),
      [...authorWindowColumns],
      authorRowsForWindow(days),
    );
  }

  writeCsv<ListingAllTimeRow>(
    join(analyticsDir, "most_popular_all_time.csv"),
    [
      "rank",
      "listing_type",
      "id",
      "name",
      "author",
      "author_alias",
      "attribution_link",
      "total_downloads",
      "adjusted_total_downloads",
      "latest_snapshot",
    ],
    allTimeRows,
  );

  writeCsv<ProjectAllTimeRow>(
    join(analyticsDir, "projects_most_popular_all_time.csv"),
    [
      "rank",
      "project_key",
      "project_name",
      "author",
      "author_alias",
      "attribution_link",
      "listing_count",
      "total_downloads",
      "adjusted_total_downloads",
      "latest_snapshot",
    ],
    projectAllTimeRows,
  );

  writeCsv<AuthorAssetCountRow>(
    join(analyticsDir, "authors_by_asset_count.csv"),
    [
      "rank",
      "author",
      "author_alias",
      "attribution_link",
      "asset_count",
      "map_count",
      "mod_count",
      "total_downloads",
      "adjusted_total_downloads",
      "caretaken_asset_count",
    ],
    authorRowsByAssetCount,
  );

  writeCsv<AuthorTotalDownloadsRow>(
    join(analyticsDir, "authors_by_total_downloads.csv"),
    [
      "rank",
      "author",
      "author_alias",
      "attribution_link",
      "total_downloads",
      "adjusted_total_downloads",
      "asset_count",
      "map_count",
      "mod_count",
    ],
    authorRowsByTotalDownloads,
  );

  const legacyMapPopulationCsvPath = join(analyticsDir, "maps_by_population.csv");
  rmSync(legacyMapPopulationCsvPath, { force: true });

  const mapStatisticsRows = loadMapStatisticsRows(resolvedRepoRoot, authorAliases);
  writeCsv<MapStatisticsRow>(
    join(analyticsDir, "maps_statistics.csv"),
    [
      "rank",
      "id",
      "name",
      "author",
      "author_alias",
      "attribution_link",
      "city_code",
      "country",
      "population",
      "population_count",
      "points_count",
      "n_cells",
      "mean_point_density",
      "median_resident_weighted_nn_km",
      "mean_resident_weighted_nn_km",
      "median_worker_weighted_nn_km",
      "mean_worker_weighted_nn_km",
      "detail_radius_km",
      "detail_score",
      "playable_area_cells",
      "median_cell_resident_density",
      "mean_cell_resident_density",
      "pct_cells_with_residents",
      "median_cell_worker_density",
      "mean_cell_worker_density",
      "pct_cells_with_workers",
      "median_commute_distance",
      "mean_commute_distance",
      "detected_center_count",
      "polycentrism_score",
    ],
    mapStatisticsRows,
  );

  writeCsv<ListingProjectRow>(
    join(analyticsDir, "listing_projects.csv"),
    [
      "listing_type",
      "id",
      "name",
      "project_key",
      "project_name",
    ],
    listingProjectRows,
  );

  writeCsv<ListingVersionCreditRow>(
    join(analyticsDir, "listing_version_credits.csv"),
    [
      "listing_type",
      "listing_id",
      "version",
      "credited_author_id",
    ],
    listingVersionCreditRows,
  );

  writeCsv<DailySeriesRow>(
    join(analyticsDir, "asset_versions_by_day.csv"),
    [
      "listing_type",
      "id",
      "version",
      "total_downloads",
      ...snapshotDates,
    ],
    listingVersionByDayRows,
  );

  writeCsv<DailySeriesRow>(
    join(analyticsDir, "most_popular_by_day.csv"),
    [
      "listing_type",
      "id",
      "name",
      "author",
      "author_alias",
      "attribution_link",
      "project_key",
      "project_name",
      "total_downloads",
      ...snapshotDates,
    ],
    listingByDayRows,
  );

  writeCsv<DailySeriesRow>(
    join(analyticsDir, "projects_by_day.csv"),
    [
      "project_key",
      "project_name",
      "author",
      "author_alias",
      "attribution_link",
      "listing_count",
      "total_downloads",
      ...snapshotDates,
    ],
    projectByDayRows,
  );

  writeCsv<DailySeriesRow>(
    join(analyticsDir, "authors_by_day.csv"),
    [
      "author",
      "author_alias",
      "attribution_link",
      "asset_count",
      "map_count",
      "mod_count",
      "total_downloads",
      ...snapshotDates,
    ],
    authorByDayRows,
  );

  writeCsv<AssetByDayRow>(
    join(analyticsDir, "assets_by_day.csv"),
    [
      "snapshot_date",
      "total_downloads",
      "maps",
      "mods",
      "total_downloads_signed",
      "maps_signed",
      "mods_signed",
      "total_downloads_clamped",
      "maps_clamped",
      "mods_clamped",
      "cumulative_total",
      "cumulative_maps",
      "cumulative_mods",
      "total_new_assets",
      "new_maps",
      "new_mods",
      "cumulative_assets",
      "cumulative_maps_assets",
      "cumulative_mods_assets",
      "total_new_assets_versions",
      "new_maps_versions",
      "new_mods_versions",
      "cumulative_asset_versions",
      "cumulative_maps_versions",
      "cumulative_mods_versions",
    ],
    assetsByDayRows,
  );

  console.log(`Generated analytics CSVs in ${analyticsDir}`);
  console.log(`Latest snapshot: ${latest.file}`);
  console.log(`Top listings: ${topListings ?? "all"}`);
  console.log(`Top authors: ${topAuthors ?? "all"}`);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  runGenerateAnalyticsCli();
}
