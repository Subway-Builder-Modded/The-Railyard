import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { ListingManifest, ManifestDirectory, ManifestType } from "./manifests.js";
import type { MapManifest } from "./manifests.js";
import * as D from "./download-definitions.js";
import type {
  IntegrityCache,
  IntegrityCacheEntry,
  IntegrityOutput,
  IntegritySource,
  IntegrityVersionEntry,
  ListingIntegrityEntry,
  ZipCompletenessResult,
} from "./integrity.js";
import { fetchWithTimeout, resolveTimeoutMsFromEnv } from "./http.js";
import { isSupportedReleaseTag, parseGitHubReleaseAssetDownloadUrl } from "./release-resolution.js";

export interface CustomVersionCandidate {
  version: string;
  semver: boolean;
  downloadUrl: string | null;
  sha256: string | null;
  parsed: D.ParsedReleaseAssetUrl | null;
  errors: string[];
}

export interface ListingContext {
  id: string;
  listingType: ManifestType;
  cityCode?: string;
  update:
    | { type: "github"; repo: string }
    | { type: "custom"; url: string; versions: CustomVersionCandidate[] };
}

const NON_SHA_RECHECK_WINDOW_MS = 9 * 60 * 60 * 1000;
const REMOTE_REQUEST_TIMEOUT_MS = resolveTimeoutMsFromEnv("REGISTRY_FETCH_TIMEOUT_MS", 45_000);

function readJsonFile<T>(path: string): T {
  return JSON.parse(readFileSync(path, "utf-8")) as T;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim() !== "";
}

function normalizeWhitespace(value: string): string {
  return value.trim();
}

function warn(warnings: string[], message: string): void {
  warnings.push(message);
}

export function warnListing(
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

export function getDirectoryForType(listingType: ManifestType): ManifestDirectory {
  return listingType === "map" ? "maps" : "mods";
}

export function sortObjectByKeys<T>(value: Record<string, T>): Record<string, T> {
  const sorted: Record<string, T> = {};
  for (const key of Object.keys(value).sort()) {
    sorted[key] = value[key];
  }
  return sorted;
}

export function getIndexIds(repoRoot: string, dir: ManifestDirectory): string[] {
  const indexPath = resolve(repoRoot, dir, "index.json");
  const parsed = readJsonFile<{ [key: string]: unknown }>(indexPath);
  const list = parsed[dir];
  if (!Array.isArray(list)) {
    throw new Error(`Invalid index file at ${indexPath}: missing '${dir}' array`);
  }
  return list.filter((value): value is string => typeof value === "string");
}

export function getManifest(repoRoot: string, dir: ManifestDirectory, id: string): ListingManifest {
  return readJsonFile<ListingManifest>(resolve(repoRoot, dir, id, "manifest.json"));
}

function getCachePath(repoRoot: string, dir: ManifestDirectory): string {
  return resolve(repoRoot, dir, "integrity-cache.json");
}

function getIntegrityPath(repoRoot: string, dir: ManifestDirectory): string {
  return resolve(repoRoot, dir, "integrity.json");
}

function getEmptyCache(): IntegrityCache {
  return {
    schema_version: 1,
    entries: {},
  };
}

export function loadIntegrityCache(repoRoot: string, dir: ManifestDirectory): IntegrityCache {
  const cachePath = getCachePath(repoRoot, dir);
  if (!existsSync(cachePath)) {
    return getEmptyCache();
  }
  try {
    const parsed = readJsonFile<unknown>(cachePath);
    if (
      typeof parsed !== "object"
      || parsed === null
      || Array.isArray(parsed)
      || (parsed as { schema_version?: unknown }).schema_version !== 1
    ) {
      return getEmptyCache();
    }
    const rawEntries = (parsed as { entries?: unknown }).entries;
    if (typeof rawEntries !== "object" || rawEntries === null || Array.isArray(rawEntries)) {
      return getEmptyCache();
    }

    const entries: Record<string, Record<string, IntegrityCacheEntry>> = {};
    for (const [listingId, listingValue] of Object.entries(rawEntries)) {
      if (typeof listingValue !== "object" || listingValue === null || Array.isArray(listingValue)) continue;
      const versionEntries: Record<string, IntegrityCacheEntry> = {};
      for (const [version, versionValue] of Object.entries(listingValue)) {
        if (typeof versionValue !== "object" || versionValue === null || Array.isArray(versionValue)) continue;
        const fingerprint = (versionValue as { fingerprint?: unknown }).fingerprint;
        const lastCheckedAt = (versionValue as { last_checked_at?: unknown }).last_checked_at;
        const result = (versionValue as { result?: unknown }).result;
        if (
          typeof fingerprint !== "string"
          || fingerprint.trim() === ""
          || typeof lastCheckedAt !== "string"
          || lastCheckedAt.trim() === ""
          || typeof result !== "object"
          || result === null
          || Array.isArray(result)
        ) {
          continue;
        }
        versionEntries[version] = {
          fingerprint,
          last_checked_at: lastCheckedAt,
          result: result as IntegrityVersionEntry,
        };
      }
      entries[listingId] = versionEntries;
    }

    return {
      schema_version: 1,
      entries,
    };
  } catch {
    return getEmptyCache();
  }
}

export function emptyIntegrity(nowIso: string): IntegrityOutput {
  return {
    schema_version: 1,
    generated_at: nowIso,
    listings: {},
  };
}

export function loadIntegritySnapshot(repoRoot: string, dir: ManifestDirectory): IntegrityOutput | null {
  const path = getIntegrityPath(repoRoot, dir);
  if (!existsSync(path)) return null;
  try {
    const parsed = readJsonFile<unknown>(path);
    if (
      typeof parsed !== "object"
      || parsed === null
      || Array.isArray(parsed)
      || (parsed as { schema_version?: unknown }).schema_version !== 1
      || typeof (parsed as { generated_at?: unknown }).generated_at !== "string"
    ) {
      return null;
    }
    const listings = (parsed as { listings?: unknown }).listings;
    if (typeof listings !== "object" || listings === null || Array.isArray(listings)) {
      return null;
    }
    return {
      schema_version: 1,
      generated_at: (parsed as { generated_at: string }).generated_at,
      listings: listings as Record<string, ListingIntegrityEntry>,
    };
  } catch {
    return null;
  }
}

export function shouldUseCachedIntegrity(
  cacheEntry: IntegrityCacheEntry | undefined,
  fingerprint: string,
  now: Date,
): boolean {
  if (!cacheEntry) return false;
  if (cacheEntry.fingerprint !== fingerprint) return false;
  if (fingerprint.startsWith("sha256:")) return true;
  const lastChecked = Date.parse(cacheEntry.last_checked_at);
  if (!Number.isFinite(lastChecked)) return false;
  return now.getTime() - lastChecked <= NON_SHA_RECHECK_WINDOW_MS;
}

function semverParts(value: string): [number, number, number] | null {
  const match = value.match(/^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/);
  if (!match) return null;
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function compareSemverDescending(a: string, b: string): number {
  const pa = semverParts(a);
  const pb = semverParts(b);
  if (!pa || !pb) return b.localeCompare(a);
  if (pa[0] !== pb[0]) return pb[0] - pa[0];
  if (pa[1] !== pb[1]) return pb[1] - pa[1];
  if (pa[2] !== pb[2]) return pb[2] - pa[2];
  return b.localeCompare(a);
}

export function buildIncompleteVersionEntry(
  source: IntegritySource,
  fingerprint: string,
  checkedAt: string,
  errors: string[],
  requiredChecks: Record<string, boolean> = {},
  matchedFiles: Record<string, string | null> = {},
): IntegrityVersionEntry {
  return {
    is_complete: false,
    errors,
    required_checks: requiredChecks,
    matched_files: matchedFiles,
    source,
    fingerprint,
    checked_at: checkedAt,
  };
}

export function withCheckResult(
  result: ZipCompletenessResult,
  source: IntegritySource,
  fingerprint: string,
  checkedAt: string,
): IntegrityVersionEntry {
  return {
    is_complete: result.isComplete,
    errors: result.errors,
    required_checks: result.requiredChecks,
    matched_files: result.matchedFiles,
    source,
    fingerprint,
    checked_at: checkedAt,
  };
}

export async function fetchZipBuffer(
  listingId: string,
  zipUrl: string,
  fetchImpl: typeof fetch,
  warnings: string[],
  version: string,
  assetName?: string,
): Promise<Buffer | null> {
  let response: Response;
  try {
    const heartbeatLabel = `fetch-zip listing=${listingId} version=${version}${assetName ? ` asset=${assetName}` : ""}`;
    response = await fetchWithTimeout(fetchImpl, zipUrl, undefined, {
      timeoutMs: REMOTE_REQUEST_TIMEOUT_MS,
      heartbeatPrefix: "[downloads]",
      heartbeatLabel,
    });
  } catch (error) {
    warnListing(
      warnings,
      listingId,
      `failed to fetch ZIP${assetName ? ` '${assetName}'` : ""} (${(error as Error).message})`,
      version,
    );
    return null;
  }
  if (!response.ok) {
    warnListing(
      warnings,
      listingId,
      `failed to fetch ZIP${assetName ? ` '${assetName}'` : ""} (HTTP ${response.status})`,
      version,
    );
    return null;
  }
  try {
    return Buffer.from(await response.arrayBuffer());
  } catch {
    warnListing(
      warnings,
      listingId,
      `failed to read ZIP response body${assetName ? ` for '${assetName}'` : ""}`,
      version,
    );
    return null;
  }
}

export async function fetchCustomVersions(
  listingId: string,
  updateUrl: string,
  fetchImpl: typeof fetch,
  warnings: string[],
): Promise<CustomVersionCandidate[]> {
  let response: Response;
  try {
    response = await fetchWithTimeout(
      fetchImpl,
      updateUrl,
      {
        headers: {
          Accept: "application/json",
        },
      },
      {
        timeoutMs: REMOTE_REQUEST_TIMEOUT_MS,
        heartbeatPrefix: "[downloads]",
        heartbeatLabel: `fetch-custom-update listing=${listingId}`,
      },
    );
  } catch (error) {
    warnListing(warnings, listingId, `custom update JSON fetch failed (${(error as Error).message})`);
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

  const candidates: CustomVersionCandidate[] = [];
  for (const entry of versions) {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      warnListing(warnings, listingId, "skipped custom version entry (malformed object)");
      continue;
    }
    const rawVersion = (entry as { version?: unknown }).version;
    if (!isNonEmptyString(rawVersion)) {
      warnListing(warnings, listingId, "skipped custom version entry (missing version)");
      continue;
    }

    const version = normalizeWhitespace(rawVersion);
    const semver = isSupportedReleaseTag(version);
    const rawDownload = (entry as { download?: unknown }).download;
    const downloadUrl = isNonEmptyString(rawDownload) ? normalizeWhitespace(rawDownload) : null;
    const sha256 = isNonEmptyString((entry as { sha256?: unknown }).sha256)
      ? normalizeWhitespace((entry as { sha256: string }).sha256)
      : null;

    const parsed = downloadUrl ? parseGitHubReleaseAssetDownloadUrl(downloadUrl) : null;
    const errors: string[] = [];
    if (!semver) {
      errors.push(`non-semver version '${version}'`);
    }
    if (!downloadUrl) {
      errors.push("missing download URL");
    } else if (!parsed) {
      errors.push("non-GitHub release download URL");
    }

    candidates.push({
      version,
      semver,
      downloadUrl,
      sha256,
      parsed,
      errors,
    });
  }

  return candidates;
}

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

export function createListingIntegrityEntry(
  versionEntries: Record<string, IntegrityVersionEntry>,
): ListingIntegrityEntry {
  const semverVersions = Object.keys(versionEntries).filter((version) => isSupportedReleaseTag(version));
  const completeVersions = semverVersions
    .filter((version) => versionEntries[version]?.is_complete === true)
    .sort(compareSemverDescending);
  const incompleteVersions = semverVersions
    .filter((version) => versionEntries[version]?.is_complete !== true)
    .sort(compareSemverDescending);
  const latestSemverVersion = semverVersions.length > 0
    ? [...semverVersions].sort(compareSemverDescending)[0]
    : null;
  const latestSemverComplete = latestSemverVersion
    ? versionEntries[latestSemverVersion]?.is_complete === true
    : null;

  return {
    has_complete_version: completeVersions.length > 0,
    latest_semver_version: latestSemverVersion,
    latest_semver_complete: latestSemverComplete,
    complete_versions: completeVersions,
    incomplete_versions: incompleteVersions,
    versions: sortObjectByKeys(versionEntries),
  };
}
