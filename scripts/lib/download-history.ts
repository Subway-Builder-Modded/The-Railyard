import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import type { DownloadsByListing } from "./download-definitions.js";

type ListingKind = "maps" | "mods";

interface IndexFile {
  schema_version?: number;
  maps?: unknown;
  mods?: unknown;
  [key: string]: unknown;
}

interface DownloadHistorySection {
  downloads: DownloadsByListing;
  total_downloads: number;
  net_downloads: number;
  index: IndexFile;
  entries: number;
}

export interface DownloadHistorySnapshot {
  schema_version: 1;
  snapshot_date: string;
  generated_at: string;
  maps: DownloadHistorySection;
  mods: DownloadHistorySection;
}

export interface GenerateDownloadHistoryOptions {
  repoRoot: string;
  now?: Date;
}

export interface GenerateDownloadHistoryResult {
  snapshotFile: string;
  previousSnapshotFile: string | null;
  snapshot: DownloadHistorySnapshot;
  warnings: string[];
}

const SNAPSHOT_PATTERN = /^snapshot_(\d{4}_\d{2}_\d{2})\.json$/;

function readJsonFile<T>(path: string): T {
  return JSON.parse(readFileSync(path, "utf-8")) as T;
}

function toSnapshotDate(now: Date): string {
  return now.toISOString().slice(0, 10).replaceAll("-", "_");
}

function asFiniteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function normalizeDownloads(
  raw: unknown,
  listingKind: ListingKind,
  warnings: string[],
): DownloadsByListing {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new Error(`${listingKind}/downloads.json must be a JSON object`);
  }

  const input = raw as Record<string, unknown>;
  const result: DownloadsByListing = {};
  for (const listingId of Object.keys(input).sort()) {
    const versionsRaw = input[listingId];
    if (typeof versionsRaw !== "object" || versionsRaw === null || Array.isArray(versionsRaw)) {
      warnings.push(`${listingKind}: listing='${listingId}' has non-object versions payload; treating as empty`);
      result[listingId] = {};
      continue;
    }

    const versionsInput = versionsRaw as Record<string, unknown>;
    const versionsResult: Record<string, number> = {};
    for (const version of Object.keys(versionsInput).sort()) {
      const parsed = asFiniteNumber(versionsInput[version]);
      if (parsed === null) {
        warnings.push(
          `${listingKind}: listing='${listingId}' version='${version}' has non-numeric download count; skipping version`,
        );
        continue;
      }
      versionsResult[version] = parsed;
    }
    result[listingId] = versionsResult;
  }
  return result;
}

function computeTotalDownloads(downloads: DownloadsByListing): number {
  let total = 0;
  for (const versions of Object.values(downloads)) {
    for (const count of Object.values(versions)) {
      total += count;
    }
  }
  return total;
}

function readListingData(
  repoRoot: string,
  listingKind: ListingKind,
  warnings: string[],
): { downloads: DownloadsByListing; totalDownloads: number; index: IndexFile; entries: number } {
  const downloadsPath = resolve(repoRoot, listingKind, "downloads.json");
  const indexPath = resolve(repoRoot, listingKind, "index.json");
  const downloadsRaw = readJsonFile<unknown>(downloadsPath);
  const downloads = normalizeDownloads(downloadsRaw, listingKind, warnings);
  const totalDownloads = computeTotalDownloads(downloads);
  const index = readJsonFile<IndexFile>(indexPath);

  const rawEntries = index[listingKind];
  const entries = Array.isArray(rawEntries) ? rawEntries.length : 0;
  if (!Array.isArray(rawEntries)) {
    warnings.push(`${listingKind}: index.json field '${listingKind}' is not an array; entries set to 0`);
  }

  return {
    downloads,
    totalDownloads,
    index,
    entries,
  };
}

function getHistoryDir(repoRoot: string): string {
  return resolve(repoRoot, "history");
}

function readPreviousSnapshot(
  repoRoot: string,
  currentSnapshotFileName: string,
  warnings: string[],
): { fileName: string; snapshot: DownloadHistorySnapshot } | null {
  const historyDir = getHistoryDir(repoRoot);
  if (!existsSync(historyDir)) {
    return null;
  }

  const previousFiles = readdirSync(historyDir)
    .filter((name) => SNAPSHOT_PATTERN.test(name))
    .filter((name) => name < currentSnapshotFileName)
    .sort();
  if (previousFiles.length === 0) {
    return null;
  }

  const fileName = previousFiles[previousFiles.length - 1]!;
  try {
    const snapshot = readJsonFile<DownloadHistorySnapshot>(resolve(historyDir, fileName));
    return { fileName, snapshot };
  } catch {
    warnings.push(`history: failed to parse previous snapshot '${fileName}'; using first-run net calculation`);
    return null;
  }
}

function resolvePreviousTotal(
  previousSnapshot: DownloadHistorySnapshot | null,
  listingKind: ListingKind,
  warnings: string[],
): number | null {
  if (!previousSnapshot) return null;
  const section = previousSnapshot[listingKind];
  const total = section?.total_downloads;
  if (typeof total !== "number" || !Number.isFinite(total)) {
    warnings.push(`history: previous snapshot missing finite ${listingKind}.total_downloads; using first-run net calculation`);
    return null;
  }
  return total;
}

export function generateDownloadHistorySnapshot(
  options: GenerateDownloadHistoryOptions,
): GenerateDownloadHistoryResult {
  const now = options.now ?? new Date();
  const warnings: string[] = [];
  const snapshotDate = toSnapshotDate(now);
  const snapshotFileName = `snapshot_${snapshotDate}.json`;
  const previous = readPreviousSnapshot(options.repoRoot, snapshotFileName, warnings);

  const mapsData = readListingData(options.repoRoot, "maps", warnings);
  const modsData = readListingData(options.repoRoot, "mods", warnings);

  const previousMapsTotal = resolvePreviousTotal(previous?.snapshot ?? null, "maps", warnings);
  const previousModsTotal = resolvePreviousTotal(previous?.snapshot ?? null, "mods", warnings);

  const snapshot: DownloadHistorySnapshot = {
    schema_version: 1,
    snapshot_date: snapshotDate,
    generated_at: now.toISOString(),
    maps: {
      downloads: mapsData.downloads,
      total_downloads: mapsData.totalDownloads,
      net_downloads: previousMapsTotal === null
        ? mapsData.totalDownloads
        : mapsData.totalDownloads - previousMapsTotal,
      index: mapsData.index,
      entries: mapsData.entries,
    },
    mods: {
      downloads: modsData.downloads,
      total_downloads: modsData.totalDownloads,
      net_downloads: previousModsTotal === null
        ? modsData.totalDownloads
        : modsData.totalDownloads - previousModsTotal,
      index: modsData.index,
      entries: modsData.entries,
    },
  };

  const historyDir = getHistoryDir(options.repoRoot);
  mkdirSync(historyDir, { recursive: true });
  const snapshotPath = resolve(historyDir, snapshotFileName);
  writeFileSync(snapshotPath, `${JSON.stringify(snapshot, null, 2)}\n`, "utf-8");

  return {
    snapshotFile: `history/${snapshotFileName}`,
    previousSnapshotFile: previous ? `history/${previous.fileName}` : null,
    snapshot,
    warnings,
  };
}
