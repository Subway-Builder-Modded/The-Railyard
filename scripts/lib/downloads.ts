import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { ListingManifest, ManifestDirectory } from "./manifests.js";
import * as D from "./download-definitions.js";
import {
  createGraphqlUsageState,
  fetchRepoReleaseIndexes,
  graphqlUsageSnapshot,
  isSupportedReleaseTag,
  parseGitHubReleaseAssetDownloadUrl,
} from "./release-resolution.js";

export type {
  ParsedReleaseAssetUrl,
  DownloadsByListing,
  GenerateDownloadsOptions,
  GenerateDownloadsResult,
} from "./download-definitions.js";

function getDirectoryForType(
  listingType: D.GenerateDownloadsOptions["listingType"],
): ManifestDirectory {
  return listingType === "map" ? "maps" : "mods";
}

function readJsonFile<T>(path: string): T {
  return JSON.parse(readFileSync(path, "utf-8")) as T;
}

function normalizeWhitespace(value: string): string {
  return value.trim();
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim() !== "";
}

function warn(warnings: string[], message: string): void {
  warnings.push(message);
}

function warnListing(
  warnings: string[],
  listingId: string,
  message: string,
  version?: string,
): void {
  if (version) {
    warn(warnings, `listing=${listingId} version=${version}: ${message}`);
    return;
  }
  warn(warnings, `listing=${listingId}: ${message}`);
}

export { isSupportedReleaseTag, parseGitHubReleaseAssetDownloadUrl };

/**
 * Builds a per-tag download index from release payloads.
 *
 * Each tag stores:
 * - `zipTotal`: cumulative downloads across `.zip` assets only
 * - `assets`: lookup map of all asset names to raw download metadata
 */
export function aggregateZipDownloadCountsByTag(releases: Array<{
  tagName: string;
  assets: Array<{ name: string; downloadCount: number; downloadUrl?: string | null }>;
}>): Map<string, D.RepoReleaseTagData> {
  const byTag = new Map<string, D.RepoReleaseTagData>();
  for (const release of releases) {
    if (!isNonEmptyString(release.tagName)) continue;
    const assets = new Map<string, { downloadCount: number; downloadUrl: string | null }>();
    let zipTotal = 0;

    for (const asset of release.assets) {
      if (!isNonEmptyString(asset.name) || !Number.isFinite(asset.downloadCount)) continue;
      assets.set(asset.name, {
        downloadCount: asset.downloadCount,
        downloadUrl: asset.downloadUrl ?? null,
      });
      if (asset.name.toLowerCase().endsWith(".zip")) {
        zipTotal += asset.downloadCount;
      }
    }

    byTag.set(release.tagName, { zipTotal, assets });
  }

  return byTag;
}

function getIndexIds(repoRoot: string, dir: ManifestDirectory): string[] {
  const indexPath = resolve(repoRoot, dir, "index.json");
  const parsed = readJsonFile<{ [key: string]: unknown }>(indexPath);
  const list = parsed[dir];
  if (!Array.isArray(list)) {
    throw new Error(`Invalid index file at ${indexPath}: missing '${dir}' array`);
  }
  return list.filter((value): value is string => typeof value === "string");
}

function getManifest(repoRoot: string, dir: ManifestDirectory, id: string): ListingManifest {
  return readJsonFile<ListingManifest>(resolve(repoRoot, dir, id, "manifest.json"));
}

/**
 * Fetches a custom update JSON and extracts only resolvable GitHub release
 * zip download references. Invalid/malformed entries are skipped with warnings.
 */
async function fetchCustomVersions(
  listingId: string,
  updateUrl: string,
  fetchImpl: typeof fetch,
  warnings: string[],
): Promise<D.CustomVersionRef[]> {
  let response: Response;
  try {
    response = await fetchImpl(updateUrl, {
      headers: {
        Accept: "application/json",
      },
    });
  } catch (error) {
    warnListing(
      warnings,
      listingId,
      `custom update JSON fetch failed (${(error as Error).message})`,
    );
    return [];
  }

  if (!response.ok) {
    warnListing(warnings, listingId, `custom update JSON returned HTTP ${response.status}`);
    return [];
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    warnListing(warnings, listingId, "custom update JSON is not valid JSON");
    return [];
  }

  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    warnListing(warnings, listingId, "custom update JSON must be an object");
    return [];
  }

  const versions = (body as { versions?: unknown }).versions;
  if (!Array.isArray(versions)) {
    warnListing(warnings, listingId, "custom update JSON missing versions array");
    return [];
  }

  const refs: D.CustomVersionRef[] = [];
  for (const entry of versions) {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      warnListing(warnings, listingId, "skipped custom version entry (malformed object)");
      continue;
    }
    const rawVersion = (entry as { version?: unknown }).version;
    const rawDownload = (entry as { download?: unknown }).download;
    if (!isNonEmptyString(rawVersion)) {
      warnListing(warnings, listingId, "skipped custom version entry (missing version)");
      continue;
    }
    if (!isNonEmptyString(rawDownload)) {
      warnListing(warnings, listingId, "missing download URL", rawVersion);
      continue;
    }

    const parsed = parseGitHubReleaseAssetDownloadUrl(rawDownload);
    if (!parsed) {
      warnListing(warnings, listingId, "skipped non-GitHub release download URL", rawVersion);
      continue;
    }
    if (!isSupportedReleaseTag(parsed.tag)) {
      warnListing(
        warnings,
        listingId,
        `skipped non-semver release tag '${parsed.tag}'`,
        rawVersion,
      );
      continue;
    }
    if (!parsed.assetName.toLowerCase().endsWith(".zip")) {
      warnListing(
        warnings,
        listingId,
        `skipped non-zip asset '${parsed.assetName}'`,
        rawVersion,
      );
      continue;
    }

    refs.push({
      listingId,
      version: normalizeWhitespace(rawVersion),
      repo: parsed.repo,
      tag: parsed.tag,
      assetName: parsed.assetName,
    });
  }

  return refs;
}

function sortObjectByKeys<T>(value: Record<string, T>): Record<string, T> {
  const sorted: Record<string, T> = {};
  for (const key of Object.keys(value).sort()) {
    sorted[key] = value[key];
  }
  return sorted;
}

/**
 * Generates deterministic per-listing download counts for maps or mods.
 *
 * Data sources:
 * - `update.type=github`: release tags from the configured repo
 * - `update.type=custom`: version/download pairs from update.json mapped
 *   to GitHub release assets
 *
 * Rules:
 * - zip assets only are counted toward version totals
 * - unresolvable versions are skipped and reported in `warnings`
 * - partial failures are tolerated to keep output generation resilient
 */
export async function generateDownloadsData(
  options: D.GenerateDownloadsOptions,
): Promise<D.GenerateDownloadsResult> {
  const repoRoot = options.repoRoot;
  const listingType = options.listingType;
  const fetchImpl = options.fetchImpl ?? fetch;
  const token = options.token;
  const warnings: string[] = [];
  const dir = getDirectoryForType(listingType);
  const ids = getIndexIds(repoRoot, dir);

  const downloadsByListing: D.DownloadsByListing = {};
  const githubListings: Array<{ id: string; repo: string }> = [];
  const customVersionRefs: D.CustomVersionRef[] = [];

  for (const id of ids) {
    downloadsByListing[id] = {};
    let manifest: ListingManifest;
    try {
      manifest = getManifest(repoRoot, dir, id);
    } catch (error) {
      warnListing(warnings, id, `failed to read manifest (${(error as Error).message})`);
      continue;
    }

    if (manifest.update.type === "github") {
      githubListings.push({
        id,
        repo: manifest.update.repo.toLowerCase(),
      });
      continue;
    }

    const refs = await fetchCustomVersions(
      id,
      manifest.update.url,
      fetchImpl,
      warnings,
    );
    customVersionRefs.push(...refs);
  }

  const repoSet = new Set<string>();
  for (const listing of githubListings) repoSet.add(listing.repo);
  for (const version of customVersionRefs) repoSet.add(version.repo);

  const usageState = createGraphqlUsageState();
  const { repoIndexes } = await fetchRepoReleaseIndexes(repoSet, {
    fetchImpl,
    token,
    warnings,
    usageState,
  });

  for (const listing of githubListings) {
    const index = repoIndexes.get(listing.repo);
    if (!index) {
      warnListing(warnings, listing.id, "skipped all github-release versions (repo unavailable)");
      continue;
    }
    const releaseCounts: Record<string, number> = {};
    for (const [tag, data] of index.byTag.entries()) {
      if (!isSupportedReleaseTag(tag)) {
        warnListing(warnings, listing.id, `skipped non-semver release tag '${tag}'`);
        continue;
      }
      const hasZipAsset = Array.from(data.assets.keys())
        .some((assetName) => assetName.toLowerCase().endsWith(".zip"));
      if (!hasZipAsset) {
        continue;
      }
      releaseCounts[tag] = data.zipTotal;
    }
    downloadsByListing[listing.id] = sortObjectByKeys(releaseCounts);
  }

  for (const versionRef of customVersionRefs) {
    const index = repoIndexes.get(versionRef.repo);
    if (!index) {
      warnListing(
        warnings,
        versionRef.listingId,
        "skipped (repo unavailable)",
        versionRef.version,
      );
      continue;
    }
    const release = index.byTag.get(versionRef.tag);
    if (!release) {
      warnListing(
        warnings,
        versionRef.listingId,
        `skipped (tag '${versionRef.tag}' not found)`,
        versionRef.version,
      );
      continue;
    }
    const asset = release.assets.get(versionRef.assetName);
    if (asset === undefined) {
      warnListing(
        warnings,
        versionRef.listingId,
        `skipped (asset '${versionRef.assetName}' not found)`,
        versionRef.version,
      );
      continue;
    }
    downloadsByListing[versionRef.listingId][versionRef.version] = asset.downloadCount;
  }

  const sortedDownloads: D.DownloadsByListing = {};
  for (const id of [...ids].sort()) {
    sortedDownloads[id] = sortObjectByKeys(downloadsByListing[id] ?? {});
  }

  return {
    downloads: sortedDownloads,
    warnings,
    rateLimit: graphqlUsageSnapshot(usageState),
  };
}

