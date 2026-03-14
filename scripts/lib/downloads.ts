import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { ListingManifest, ManifestDirectory, ManifestType } from "./manifests.js";
import type { MapManifest } from "./manifests.js";
import * as D from "./download-definitions.js";
import {
  createGraphqlUsageState,
  fetchRepoReleaseIndexes,
  graphqlUsageSnapshot,
  isSupportedReleaseTag,
  parseGitHubReleaseAssetDownloadUrl,
} from "./release-resolution.js";
import type {
  IntegrityCache,
  IntegrityCacheEntry,
  IntegrityOutput,
  IntegritySource,
  IntegrityVersionEntry,
  ListingIntegrityEntry,
  ZipCompletenessResult,
} from "./integrity.js";
import { inspectZipCompleteness } from "./integrity.js";

export type {
  ParsedReleaseAssetUrl,
  DownloadsByListing,
  GenerateDownloadsOptions,
  GenerateDownloadsResult,
} from "./download-definitions.js";

interface CustomVersionCandidate {
  version: string;
  semver: boolean;
  downloadUrl: string | null;
  sha256: string | null;
  parsed: D.ParsedReleaseAssetUrl | null;
  errors: string[];
}

interface ListingContext {
  id: string;
  listingType: ManifestType;
  cityCode?: string;
  update:
    | { type: "github"; repo: string }
    | { type: "custom"; url: string; versions: CustomVersionCandidate[] };
}

const NON_SHA_RECHECK_WINDOW_MS = 9 * 60 * 60 * 1000;
const DEFAULT_REMOTE_REQUEST_TIMEOUT_MS = 45_000;

function parsePositiveInteger(value: string | undefined): number | null {
  if (typeof value !== "string") return null;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return parsed;
}

function resolveRemoteRequestTimeoutMs(): number {
  const fromEnv = parsePositiveInteger(process.env.REGISTRY_FETCH_TIMEOUT_MS);
  return fromEnv ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS;
}

const REMOTE_REQUEST_TIMEOUT_MS = resolveRemoteRequestTimeoutMs();

function getDirectoryForType(listingType: ManifestType): ManifestDirectory {
  return listingType === "map" ? "maps" : "mods";
}

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

async function fetchWithTimeout(
  fetchImpl: typeof fetch,
  url: string,
  init: RequestInit | undefined,
  heartbeatLabel: string,
): Promise<Response> {
  const startedAt = Date.now();
  console.log(`[downloads] heartbeat:start ${heartbeatLabel}`);

  const controller = new AbortController();
  const timeoutHandle = setTimeout(() => {
    controller.abort();
  }, REMOTE_REQUEST_TIMEOUT_MS);

  try {
    const response = await fetchImpl(url, {
      ...init,
      signal: controller.signal,
    });
    const durationMs = Date.now() - startedAt;
    console.log(
      `[downloads] heartbeat:end ${heartbeatLabel} status=${response.status} durationMs=${durationMs}`,
    );
    return response;
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    const message = error instanceof Error ? error.message : String(error);
    const timeoutHint = error instanceof Error && error.name === "AbortError"
      ? `timed out after ${REMOTE_REQUEST_TIMEOUT_MS}ms`
      : message;
    console.warn(
      `[downloads] heartbeat:error ${heartbeatLabel} durationMs=${durationMs} error=${timeoutHint}`,
    );
    throw new Error(timeoutHint);
  } finally {
    clearTimeout(timeoutHandle);
  }
}

function sortObjectByKeys<T>(value: Record<string, T>): Record<string, T> {
  const sorted: Record<string, T> = {};
  for (const key of Object.keys(value).sort()) {
    sorted[key] = value[key];
  }
  return sorted;
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

function loadIntegrityCache(repoRoot: string, dir: ManifestDirectory): IntegrityCache {
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

function emptyIntegrity(nowIso: string): IntegrityOutput {
  return {
    schema_version: 1,
    generated_at: nowIso,
    listings: {},
  };
}

function loadIntegritySnapshot(repoRoot: string, dir: ManifestDirectory): IntegrityOutput | null {
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

function shouldUseCachedIntegrity(
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

function buildIncompleteVersionEntry(
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

function withCheckResult(
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

async function fetchZipBuffer(
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
    response = await fetchWithTimeout(fetchImpl, zipUrl, undefined, heartbeatLabel);
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

async function fetchCustomVersions(
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
      `fetch-custom-update listing=${listingId}`,
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

function createListingIntegrityEntry(
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

async function generateDownloadsDataDownloadOnly(
  options: D.GenerateDownloadsOptions,
): Promise<D.GenerateDownloadsResult> {
  const repoRoot = options.repoRoot;
  const listingType = options.listingType;
  const fetchImpl = options.fetchImpl ?? fetch;
  const token = options.token;
  const warnings: string[] = [];
  const dir = getDirectoryForType(listingType);
  const ids = getIndexIds(repoRoot, dir);
  const nowIso = new Date().toISOString();

  const downloadsByListing: D.DownloadsByListing = {};
  const listingContexts = new Map<string, ListingContext>();
  const repoSet = new Set<string>();

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
      const repo = manifest.update.repo.toLowerCase();
      repoSet.add(repo);
      listingContexts.set(id, {
        id,
        listingType,
        cityCode: listingType === "map" ? (manifest as MapManifest).city_code : undefined,
        update: { type: "github", repo },
      });
      continue;
    }

    const customVersions = await fetchCustomVersions(id, manifest.update.url, fetchImpl, warnings);
    for (const version of customVersions) {
      if (version.parsed) {
        repoSet.add(version.parsed.repo);
      }
    }
    listingContexts.set(id, {
      id,
      listingType,
      cityCode: listingType === "map" ? (manifest as MapManifest).city_code : undefined,
      update: {
        type: "custom",
        url: manifest.update.url,
        versions: customVersions,
      },
    });
  }

  const usageState = createGraphqlUsageState();
  const { repoIndexes } = await fetchRepoReleaseIndexes(repoSet, {
    fetchImpl,
    token,
    warnings,
    usageState,
  });

  let versionsChecked = 0;
  let filteredVersions = 0;

  for (const id of [...ids].sort()) {
    console.log(`[downloads] heartbeat:listing mode=download-only listing=${id}`);
    const context = listingContexts.get(id);
    if (!context) continue;

    if (context.update.type === "github") {
      const repoIndex = repoIndexes.get(context.update.repo);
      if (!repoIndex) {
        warnListing(warnings, id, "skipped all github-release versions (repo unavailable)");
        continue;
      }

      for (const tag of [...repoIndex.byTag.keys()].sort()) {
        const releaseData = repoIndex.byTag.get(tag);
        if (!releaseData) continue;
        if (!isSupportedReleaseTag(tag)) continue;
        const hasZipAsset = Array.from(releaseData.assets.keys())
          .some((assetName) => assetName.toLowerCase().endsWith(".zip"));
        if (!hasZipAsset) continue;

        versionsChecked += 1;
        downloadsByListing[id][tag] = releaseData.zipTotal;
      }
      continue;
    }

    for (const candidate of context.update.versions) {
      if (!candidate.semver) continue;
      versionsChecked += 1;

      if (!candidate.parsed) {
        warnListing(
          warnings,
          id,
          "skipped non-GitHub release download URL",
          candidate.version,
        );
        continue;
      }

      const repoIndex = repoIndexes.get(candidate.parsed.repo);
      if (!repoIndex) {
        warnListing(warnings, id, "skipped (repo unavailable)", candidate.version);
        continue;
      }
      const release = repoIndex.byTag.get(candidate.parsed.tag);
      if (!release) {
        warnListing(
          warnings,
          id,
          `skipped (tag '${candidate.parsed.tag}' not found)`,
          candidate.version,
        );
        continue;
      }
      const asset = release.assets.get(candidate.parsed.assetName);
      if (!asset) {
        warnListing(
          warnings,
          id,
          `skipped (asset '${candidate.parsed.assetName}' not found)`,
          candidate.version,
        );
        continue;
      }

      downloadsByListing[id][candidate.version] = asset.downloadCount;
    }
  }

  const sortedDownloads: D.DownloadsByListing = {};
  for (const id of [...ids].sort()) {
    sortedDownloads[id] = sortObjectByKeys(downloadsByListing[id] ?? {});
  }

  const integrity = loadIntegritySnapshot(repoRoot, dir) ?? emptyIntegrity(nowIso);
  let completeVersions = 0;
  let incompleteVersions = 0;
  for (const listing of Object.values(integrity.listings)) {
    for (const version of Object.values(listing.versions)) {
      if (version.is_complete) {
        completeVersions += 1;
      } else {
        incompleteVersions += 1;
      }
    }
  }

  return {
    downloads: sortedDownloads,
    integrity,
    integrityCache: loadIntegrityCache(repoRoot, dir),
    stats: {
      listings: ids.length,
      versions_checked: versionsChecked,
      complete_versions: completeVersions,
      incomplete_versions: incompleteVersions,
      filtered_versions: filteredVersions,
      cache_hits: 0,
    },
    warnings,
    rateLimit: graphqlUsageSnapshot(usageState),
  };
}

/**
 * Generates deterministic per-listing download counts for maps or mods and
 * produces integrity metadata for each version.
 *
 * Data sources:
 * - `update.type=github`: release tags from the configured repo
 * - `update.type=custom`: version/download pairs from update.json mapped
 *   to GitHub release assets where possible
 *
 * Rules:
 * - zip assets only are counted toward version totals
 * - invalid/incomplete versions are hard-filtered from downloads
 * - integrity records include explicit invalid entries for non-semver versions
 * - partial failures are tolerated to keep output generation resilient
 */
export async function generateDownloadsData(
  options: D.GenerateDownloadsOptions,
): Promise<D.GenerateDownloadsResult> {
  const mode = options.mode ?? "full";
  if (mode === "download-only") {
    return generateDownloadsDataDownloadOnly(options);
  }

  const repoRoot = options.repoRoot;
  const listingType = options.listingType;
  const fetchImpl = options.fetchImpl ?? fetch;
  const token = options.token;
  const warnings: string[] = [];
  const dir = getDirectoryForType(listingType);
  const ids = getIndexIds(repoRoot, dir);
  const now = new Date();
  const nowIso = now.toISOString();

  const cache = loadIntegrityCache(repoRoot, dir);
  const nextCache: IntegrityCache = {
    schema_version: 1,
    entries: {},
  };

  const downloadsByListing: D.DownloadsByListing = {};
  const listingContexts = new Map<string, ListingContext>();
  const repoSet = new Set<string>();

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
      const repo = manifest.update.repo.toLowerCase();
      repoSet.add(repo);
      listingContexts.set(id, {
        id,
        listingType,
        cityCode: listingType === "map" ? (manifest as MapManifest).city_code : undefined,
        update: { type: "github", repo },
      });
      continue;
    }

    const customVersions = await fetchCustomVersions(id, manifest.update.url, fetchImpl, warnings);
    for (const version of customVersions) {
      if (version.parsed) {
        repoSet.add(version.parsed.repo);
      }
    }
    listingContexts.set(id, {
      id,
      listingType,
      cityCode: listingType === "map" ? (manifest as MapManifest).city_code : undefined,
      update: {
        type: "custom",
        url: manifest.update.url,
        versions: customVersions,
      },
    });
  }

  const usageState = createGraphqlUsageState();
  const { repoIndexes } = await fetchRepoReleaseIndexes(repoSet, {
    fetchImpl,
    token,
    warnings,
    usageState,
  });

  const integrityListings: Record<string, ListingIntegrityEntry> = {};
  let versionsChecked = 0;
  let completeVersions = 0;
  let incompleteVersions = 0;
  let filteredVersions = 0;
  let cacheHits = 0;

  for (const id of [...ids].sort()) {
    console.log(`[downloads] heartbeat:listing mode=full listing=${id}`);
    const context = listingContexts.get(id);
    if (!context) {
      integrityListings[id] = createListingIntegrityEntry({});
      continue;
    }

    const versionEntries: Record<string, IntegrityVersionEntry> = {};
    const listingCacheEntries = cache.entries[id] ?? {};
    const nextListingCacheEntries: Record<string, IntegrityCacheEntry> = {};

    if (context.update.type === "github") {
      const repo = context.update.repo;
      const repoIndex = repoIndexes.get(repo);
      if (!repoIndex) {
        warnListing(warnings, id, "skipped all github-release versions (repo unavailable)");
        integrityListings[id] = createListingIntegrityEntry(versionEntries);
        nextCache.entries[id] = nextListingCacheEntries;
        continue;
      }

      for (const tag of [...repoIndex.byTag.keys()].sort()) {
        const releaseData = repoIndex.byTag.get(tag);
        if (!releaseData) continue;
        versionsChecked += 1;

        const zipAssets = Array.from(releaseData.assets.entries())
          .filter(([assetName]) => assetName.toLowerCase().endsWith(".zip"));
        const zipAssetNames = zipAssets.map(([assetName]) => assetName).sort();
        const fingerprint = zipAssetNames.length > 0
          ? `github:${repo}:${tag}:${zipAssetNames.join("|")}`
          : `github:${repo}:${tag}:no-zip`;
        const cached = listingCacheEntries[tag];
        const sourceBase: IntegritySource = {
          update_type: "github",
          repo,
          tag,
        };

        if (shouldUseCachedIntegrity(cached, fingerprint, now)) {
          cacheHits += 1;
          versionEntries[tag] = cached.result;
          nextListingCacheEntries[tag] = cached;
        } else if (!isSupportedReleaseTag(tag)) {
          const result = buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            [`non-semver release tag '${tag}'`],
          );
          versionEntries[tag] = result;
          nextListingCacheEntries[tag] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        } else if (zipAssets.length === 0) {
          const result = buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            ["release has no .zip asset"],
          );
          versionEntries[tag] = result;
          nextListingCacheEntries[tag] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        } else {
          const hasReleaseManifestAsset = releaseData.assets.has("manifest.json");
          let selectedResult: IntegrityVersionEntry | null = null;
          const attemptedErrors: string[] = [];

          for (const [assetName, asset] of zipAssets.sort(([a], [b]) => a.localeCompare(b))) {
            if (!asset.downloadUrl) {
              attemptedErrors.push(`zip asset '${assetName}' is missing download URL`);
              continue;
            }
            const zipBuffer = await fetchZipBuffer(id, asset.downloadUrl, fetchImpl, warnings, tag, assetName);
            if (!zipBuffer) {
              attemptedErrors.push(`zip asset '${assetName}' could not be fetched`);
              continue;
            }
            const check = await inspectZipCompleteness(listingType, zipBuffer, {
              cityCode: context.cityCode,
              releaseHasManifestAsset: hasReleaseManifestAsset,
            });
            selectedResult = withCheckResult(
              check,
              { ...sourceBase, asset_name: assetName, download_url: asset.downloadUrl },
              fingerprint,
              nowIso,
            );
            if (check.isComplete) {
              break;
            }
            attemptedErrors.push(...check.errors.map((error) => `asset '${assetName}': ${error}`));
            selectedResult = null;
          }

          const result = selectedResult ?? buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            attemptedErrors.length > 0 ? attemptedErrors : ["all zip assets failed integrity checks"],
          );
          versionEntries[tag] = result;
          nextListingCacheEntries[tag] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        }

        if (isSupportedReleaseTag(tag)) {
          const result = versionEntries[tag];
          if (result.is_complete) {
            downloadsByListing[id][tag] = releaseData.zipTotal;
          } else {
            filteredVersions += 1;
            warnListing(
              warnings,
              id,
              `excluded by integrity validation (${result.errors.join("; ") || "unknown error"})`,
              tag,
            );
          }
        }
      }
    } else {
      for (const candidate of context.update.versions) {
        const versionKey = candidate.version;
        versionsChecked += 1;

        const fallbackFingerprint = candidate.sha256
          ? `sha256:${candidate.sha256}`
          : `custom:${versionKey}:${candidate.downloadUrl ?? "missing-download"}`;
        const fingerprint = candidate.sha256
          ? `sha256:${candidate.sha256}`
          : (
            candidate.parsed
              ? `custom:${candidate.parsed.repo}:${candidate.parsed.tag}:${candidate.parsed.assetName}:${candidate.downloadUrl ?? "missing-download"}`
              : fallbackFingerprint
          );
        const sourceBase: IntegritySource = {
          update_type: "custom",
          repo: candidate.parsed?.repo,
          tag: candidate.parsed?.tag,
          asset_name: candidate.parsed?.assetName,
          download_url: candidate.downloadUrl ?? undefined,
        };
        const cached = listingCacheEntries[versionKey];

        if (shouldUseCachedIntegrity(cached, fingerprint, now)) {
          cacheHits += 1;
          versionEntries[versionKey] = cached.result;
          nextListingCacheEntries[versionKey] = cached;
        } else if (candidate.errors.length > 0) {
          const result = buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            candidate.errors,
          );
          versionEntries[versionKey] = result;
          nextListingCacheEntries[versionKey] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        } else if (!candidate.parsed) {
          const result = buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            ["download URL could not be parsed as a GitHub release asset URL"],
          );
          versionEntries[versionKey] = result;
          nextListingCacheEntries[versionKey] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        } else if (!candidate.parsed.assetName.toLowerCase().endsWith(".zip")) {
          const result = buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            [`download asset '${candidate.parsed.assetName}' is not a .zip`],
          );
          versionEntries[versionKey] = result;
          nextListingCacheEntries[versionKey] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        } else {
          const repoIndex = repoIndexes.get(candidate.parsed.repo);
          if (!repoIndex) {
            const result = buildIncompleteVersionEntry(
              sourceBase,
              fingerprint,
              nowIso,
              ["repository is unavailable via GitHub GraphQL"],
            );
            versionEntries[versionKey] = result;
            nextListingCacheEntries[versionKey] = {
              fingerprint,
              last_checked_at: nowIso,
              result,
            };
          } else {
            const release = repoIndex.byTag.get(candidate.parsed.tag);
            if (!release) {
              const result = buildIncompleteVersionEntry(
                sourceBase,
                fingerprint,
                nowIso,
                [`release tag '${candidate.parsed.tag}' not found`],
              );
              versionEntries[versionKey] = result;
              nextListingCacheEntries[versionKey] = {
                fingerprint,
                last_checked_at: nowIso,
                result,
              };
            } else {
              const asset = release.assets.get(candidate.parsed.assetName);
              if (!asset) {
                const result = buildIncompleteVersionEntry(
                  sourceBase,
                  fingerprint,
                  nowIso,
                  [`release asset '${candidate.parsed.assetName}' not found`],
                );
                versionEntries[versionKey] = result;
                nextListingCacheEntries[versionKey] = {
                  fingerprint,
                  last_checked_at: nowIso,
                  result,
                };
              } else if (!asset.downloadUrl) {
                const result = buildIncompleteVersionEntry(
                  sourceBase,
                  fingerprint,
                  nowIso,
                  [`release asset '${candidate.parsed.assetName}' has no download URL`],
                );
                versionEntries[versionKey] = result;
                nextListingCacheEntries[versionKey] = {
                  fingerprint,
                  last_checked_at: nowIso,
                  result,
                };
              } else {
                const zipBuffer = await fetchZipBuffer(
                  id,
                  asset.downloadUrl,
                  fetchImpl,
                  warnings,
                  versionKey,
                  candidate.parsed.assetName,
                );
                if (!zipBuffer) {
                  const result = buildIncompleteVersionEntry(
                    sourceBase,
                    fingerprint,
                    nowIso,
                    [`failed to fetch ZIP asset '${candidate.parsed.assetName}'`],
                  );
                  versionEntries[versionKey] = result;
                  nextListingCacheEntries[versionKey] = {
                    fingerprint,
                    last_checked_at: nowIso,
                    result,
                  };
                } else {
                  const check = await inspectZipCompleteness(listingType, zipBuffer, {
                    cityCode: context.cityCode,
                    releaseHasManifestAsset: release.assets.has("manifest.json"),
                  });
                  const result = withCheckResult(
                    check,
                    {
                      ...sourceBase,
                      asset_name: candidate.parsed.assetName,
                      download_url: asset.downloadUrl,
                    },
                    fingerprint,
                    nowIso,
                  );
                  versionEntries[versionKey] = result;
                  nextListingCacheEntries[versionKey] = {
                    fingerprint,
                    last_checked_at: nowIso,
                    result,
                  };
                }
              }
            }
          }
        }

        if (candidate.semver) {
          const result = versionEntries[versionKey];
          if (result?.is_complete === true && candidate.parsed) {
            const repoIndex = repoIndexes.get(candidate.parsed.repo);
            const release = repoIndex?.byTag.get(candidate.parsed.tag);
            const asset = release?.assets.get(candidate.parsed.assetName);
            if (asset) {
              downloadsByListing[id][versionKey] = asset.downloadCount;
            }
          } else {
            filteredVersions += 1;
            warnListing(
              warnings,
              id,
              `excluded by integrity validation (${result?.errors.join("; ") || "unknown error"})`,
              versionKey,
            );
          }
        }
      }
    }

    for (const [version, result] of Object.entries(versionEntries)) {
      if (result.is_complete) {
        completeVersions += 1;
      } else {
        incompleteVersions += 1;
      }
    }

    integrityListings[id] = createListingIntegrityEntry(versionEntries);
    nextCache.entries[id] = sortObjectByKeys(nextListingCacheEntries);
  }

  const sortedDownloads: D.DownloadsByListing = {};
  for (const id of [...ids].sort()) {
    sortedDownloads[id] = sortObjectByKeys(downloadsByListing[id] ?? {});
  }

  const integrity: IntegrityOutput = {
    schema_version: 1,
    generated_at: nowIso,
    listings: sortObjectByKeys(integrityListings),
  };

  return {
    downloads: sortedDownloads,
    integrity,
    integrityCache: {
      schema_version: 1,
      entries: sortObjectByKeys(nextCache.entries),
    },
    stats: {
      listings: ids.length,
      versions_checked: versionsChecked,
      complete_versions: completeVersions,
      incomplete_versions: incompleteVersions,
      filtered_versions: filteredVersions,
      cache_hits: cacheHits,
    },
    warnings,
    rateLimit: graphqlUsageSnapshot(usageState),
  };
}
