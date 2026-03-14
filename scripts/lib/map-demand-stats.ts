import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { resolve } from "node:path";
import JSZip from "jszip";
import type { MapManifest } from "./manifests.js";
import { createGraphqlUsageState, fetchRepoReleaseIndexes, graphqlUsageSnapshot } from "./release-resolution.js";
import { fetchWithTimeout, resolveTimeoutMsFromEnv } from "./http.js";

export interface DemandStats {
  residents_total: number;
  points_count: number;
  population_count: number;
}

export interface GenerateMapDemandStatsOptions {
  repoRoot: string;
  token?: string;
  fetchImpl?: typeof fetch;
  force?: boolean;
  mapId?: string;
}

export interface GenerateMapDemandStatsResult {
  processedMaps: number;
  updatedMaps: number;
  skippedMaps: number;
  skippedUnchanged: number;
  extractionFailures: number;
  residentsDeltaTotal: number;
  warnings: string[];
  rateLimit: {
    queries: number;
    totalCost: number;
    firstRemaining: number | null;
    lastRemaining: number | null;
    estimatedConsumed: number | null;
    resetAt: string | null;
  };
}

type JsonObject = Record<string, unknown>;
type MapUpdateSource =
  | { type: "github"; repo: string }
  | { type: "custom"; url: string };

interface DemandStatsCacheEntry {
  source_fingerprint: string;
  last_checked_at: string;
  stats?: DemandStats;
}

type DemandStatsCache = Record<string, DemandStatsCacheEntry>;

interface ResolvedInstallTarget {
  zipUrl: string;
  sourceFingerprint: string;
}

const CACHE_FILE_NAME = "demand-stats-cache.json";
// For non-sha fingerprints (e.g. tag+asset name), recheck periodically because
// upstream ZIP content may change without a fingerprint change.
const UNCHANGED_SKIP_WINDOW_MS = 9 * 60 * 60 * 1000;
const MAP_DEMAND_FETCH_TIMEOUT_MS = resolveTimeoutMsFromEnv("REGISTRY_FETCH_TIMEOUT_MS", 45_000);

function readJsonFile<T>(path: string): T {
  return JSON.parse(readFileSync(path, "utf-8")) as T;
}

function warn(warnings: string[], message: string): void {
  warnings.push(message);
}

function warnListing(warnings: string[], listingId: string, message: string): void {
  warn(warnings, `listing=${listingId}: ${message}`);
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function getMapIds(repoRoot: string): string[] {
  const indexPath = resolve(repoRoot, "maps", "index.json");
  const parsed = readJsonFile<{ maps?: unknown }>(indexPath);
  if (!Array.isArray(parsed.maps)) {
    throw new Error(`Invalid index file at ${indexPath}: missing 'maps' array`);
  }
  return parsed.maps.filter((value): value is string => typeof value === "string");
}

function getMapManifest(repoRoot: string, id: string): MapManifest {
  return readJsonFile<MapManifest>(resolve(repoRoot, "maps", id, "manifest.json"));
}

async function fetchCustomInstallTargetZipUrl(
  listingId: string,
  updateUrl: string,
  fetchImpl: typeof fetch,
  warnings: string[],
): Promise<ResolvedInstallTarget | null> {
  let response: Response;
  try {
    response = await fetchWithTimeout(
      fetchImpl,
      updateUrl,
      { headers: { Accept: "application/json" } },
      {
        timeoutMs: MAP_DEMAND_FETCH_TIMEOUT_MS,
        heartbeatPrefix: "[map-demand-stats]",
        heartbeatLabel: `fetch-custom-update listing=${listingId}`,
      },
    );
  } catch (error) {
    warnListing(warnings, listingId, `custom update JSON fetch failed (${(error as Error).message})`);
    return null;
  }

  if (!response.ok) {
    warnListing(warnings, listingId, `custom update JSON returned HTTP ${response.status}`);
    return null;
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    warnListing(warnings, listingId, "custom update JSON is not valid JSON");
    return null;
  }

  if (!isObject(body)) {
    warnListing(warnings, listingId, "custom update JSON must be an object");
    return null;
  }

  const versions = body.versions;
  if (!Array.isArray(versions) || versions.length === 0) {
    warnListing(warnings, listingId, "custom update JSON missing non-empty versions array");
    return null;
  }

  const firstVersion = versions[0];
  if (!isObject(firstVersion) || typeof firstVersion.download !== "string" || firstVersion.download.trim() === "") {
    warnListing(warnings, listingId, "custom update JSON first version missing download URL");
    return null;
  }

  const download = firstVersion.download.trim();
  const sha256 = typeof firstVersion.sha256 === "string" && firstVersion.sha256.trim() !== ""
    ? firstVersion.sha256.trim()
    : null;
  const version = typeof firstVersion.version === "string" ? firstVersion.version.trim() : "";

  return {
    zipUrl: download,
    sourceFingerprint: sha256
      ? `sha256:${sha256}`
      : `custom:${version}|${download}`,
  };
}

function getLatestGithubZipUrl(
  listingId: string,
  repo: string,
  repoIndexes: Map<string, { byTag: Map<string, { assets: Map<string, { downloadCount: number; downloadUrl: string | null }> }> }>,
  warnings: string[],
): ResolvedInstallTarget | null {
  const index = repoIndexes.get(repo.toLowerCase());
  if (!index) {
    warnListing(warnings, listingId, `skipped map stats extraction (repo unavailable: ${repo})`);
    return null;
  }

  const firstTagEntry = index.byTag.entries().next();
  if (firstTagEntry.done) {
    warnListing(warnings, listingId, `skipped map stats extraction (no releases in repo: ${repo})`);
    return null;
  }

  const [tag, releaseData] = firstTagEntry.value;
  for (const [assetName, asset] of releaseData.assets.entries()) {
    if (!assetName.toLowerCase().endsWith(".zip")) continue;
    if (!asset.downloadUrl || asset.downloadUrl.trim() === "") {
      warnListing(warnings, listingId, `latest release '${tag}' zip asset '${assetName}' missing download URL`);
      return null;
    }
    return {
      zipUrl: asset.downloadUrl,
      sourceFingerprint: `github:${tag}|${assetName}`,
    };
  }

  warnListing(warnings, listingId, `latest release '${tag}' has no .zip asset`);
  return null;
}

async function fetchZipBuffer(
  listingId: string,
  zipUrl: string,
  fetchImpl: typeof fetch,
  warnings: string[],
): Promise<Buffer | null> {
  let response: Response;
  try {
    response = await fetchWithTimeout(
      fetchImpl,
      zipUrl,
      undefined,
      {
        timeoutMs: MAP_DEMAND_FETCH_TIMEOUT_MS,
        heartbeatPrefix: "[map-demand-stats]",
        heartbeatLabel: `fetch-zip listing=${listingId}`,
      },
    );
  } catch (error) {
    warnListing(warnings, listingId, `failed to fetch map ZIP (${(error as Error).message})`);
    return null;
  }

  if (!response.ok) {
    warnListing(warnings, listingId, `failed to fetch map ZIP (HTTP ${response.status})`);
    return null;
  }

  try {
    const bytes = await response.arrayBuffer();
    return Buffer.from(bytes);
  } catch {
    warnListing(warnings, listingId, "failed to read map ZIP response body");
    return null;
  }
}

function findDemandDataEntry(zip: JSZip): JSZip.JSZipObject | null {
  const allEntries = Object.values(zip.files).filter((entry) => !entry.dir);
  const exactJson = allEntries.find((entry) => entry.name === "demand_data.json");
  if (exactJson) return exactJson;
  const exactGz = allEntries.find((entry) => entry.name === "demand_data.json.gz");
  if (exactGz) return exactGz;

  const jsonByBasename = allEntries.find((entry) => entry.name.toLowerCase().endsWith("/demand_data.json"));
  if (jsonByBasename) return jsonByBasename;
  const gzByBasename = allEntries.find((entry) => entry.name.toLowerCase().endsWith("/demand_data.json.gz"));
  if (gzByBasename) return gzByBasename;

  return null;
}

function getDemandPointRef(pointValue: unknown, fallbackRef: string): string {
  if (isObject(pointValue)) {
    const idValue = pointValue.id;
    if (typeof idValue === "string" && idValue.trim() !== "") {
      return idValue.trim();
    }
    if (typeof idValue === "number" && Number.isFinite(idValue)) {
      return String(idValue);
    }
  }
  return fallbackRef;
}

function parseDemandDataPayload(payload: unknown): DemandStats {
  if (!isObject(payload)) {
    throw new Error("demand data payload must be an object");
  }

  const points = payload.points;
  const popsMap = isObject(payload.pops_map)
    ? payload.pops_map
    : payload.pops;
  if (!isObject(points) && !Array.isArray(points)) {
    throw new Error("demand data missing collection field 'points'");
  }
  if (!isObject(popsMap) && !Array.isArray(popsMap)) {
    throw new Error("demand data missing collection field 'pops_map' (or legacy 'pops')");
  }

  const popSizeById = new Map<string, number>();
  const popEntries = Array.isArray(popsMap)
    ? popsMap.map((popValue, index) => [String(index), popValue] as const)
    : Object.entries(popsMap);
  for (const [popKey, popValue] of popEntries) {
    if (!isObject(popValue)) continue;
    const popRef = getDemandPointRef(popValue, popKey);
    const popId = typeof popValue.id === "string" && popValue.id.trim() !== ""
      ? popValue.id
      : popKey;
    const size = typeof popValue.size === "number" && Number.isFinite(popValue.size)
      ? popValue.size
      : undefined;
    if (size !== undefined && size < 0) {
      throw new Error(`population entry '${popRef}' has negative size value`);
    }
    if (!popId || size === undefined) continue;
    popSizeById.set(popId, size);
  }

  let residentsTotal = 0;
  const pointEntries = Array.isArray(points)
    ? points.map((pointValue, index) => [`index ${index}`, pointValue] as const)
    : Object.entries(points);
  const hasAnyExplicitResidents = pointEntries.some(([, pointValue]) => (
    isObject(pointValue)
    && typeof pointValue.residents === "number"
    && Number.isFinite(pointValue.residents)
  ));
  for (const [pointKeyOrIndex, pointValue] of pointEntries) {
    const pointRef = getDemandPointRef(pointValue, pointKeyOrIndex);
    if (!isObject(pointValue)) {
      throw new Error(`demand point '${pointRef}' is malformed`);
    }

    let residents: number | null = null;
    if (typeof pointValue.residents === "number" && Number.isFinite(pointValue.residents)) {
      residents = pointValue.residents;
    } else if (hasAnyExplicitResidents) {
      // Mixed payloads may include non-residential points with popIds references.
      // In explicit-residents mode, missing residents should count as zero.
      residents = 0;
    } else {
      const popIdsRaw = Array.isArray(pointValue.popIds)
        ? pointValue.popIds
        : (Array.isArray(pointValue.pop_ids) ? pointValue.pop_ids : null);
      if (popIdsRaw && popIdsRaw.every((value) => typeof value === "string")) {
        residents = popIdsRaw.reduce((sum, popId) => sum + (popSizeById.get(popId) ?? 0), 0);
      }
    }

    if (residents === null) {
      throw new Error(`demand point '${pointRef}' missing numeric residents value`);
    }
    if (residents < 0) {
      throw new Error(`demand point '${pointRef}' has negative residents value`);
    }
    residentsTotal += residents;
  }

  return {
    residents_total: residentsTotal,
    points_count: Array.isArray(points) ? points.length : Object.keys(points).length,
    population_count: Array.isArray(popsMap) ? popsMap.length : Object.keys(popsMap).length,
  };
}

function getCachePath(repoRoot: string): string {
  return resolve(repoRoot, "maps", CACHE_FILE_NAME);
}

function loadDemandStatsCache(repoRoot: string): DemandStatsCache {
  const cachePath = getCachePath(repoRoot);
  if (!existsSync(cachePath)) {
    return {};
  }
  try {
    const parsed = readJsonFile<unknown>(cachePath);
    if (!isObject(parsed)) return {};
    const entries: DemandStatsCache = {};
    for (const [id, entry] of Object.entries(parsed)) {
      if (!isObject(entry)) continue;
      const sourceFingerprint = typeof entry.source_fingerprint === "string"
        ? entry.source_fingerprint
        : undefined;
      const lastCheckedAt = typeof entry.last_checked_at === "string"
        ? entry.last_checked_at
        : undefined;
      const stats = isObject(entry.stats)
        && typeof entry.stats.residents_total === "number"
        && Number.isFinite(entry.stats.residents_total)
        && entry.stats.residents_total >= 0
        && typeof entry.stats.points_count === "number"
        && Number.isFinite(entry.stats.points_count)
        && entry.stats.points_count >= 0
        && typeof entry.stats.population_count === "number"
        && Number.isFinite(entry.stats.population_count)
        && entry.stats.population_count >= 0
        ? {
          residents_total: entry.stats.residents_total,
          points_count: entry.stats.points_count,
          population_count: entry.stats.population_count,
        }
        : undefined;
      if (!sourceFingerprint || !lastCheckedAt) continue;
      entries[id] = {
        source_fingerprint: sourceFingerprint,
        last_checked_at: lastCheckedAt,
        stats,
      };
    }
    return entries;
  } catch {
    return {};
  }
}

function writeDemandStatsCache(repoRoot: string, cache: DemandStatsCache): void {
  const sorted: DemandStatsCache = {};
  for (const key of Object.keys(cache).sort()) {
    sorted[key] = cache[key];
  }
  writeFileSync(getCachePath(repoRoot), `${JSON.stringify(sorted, null, 2)}\n`, "utf-8");
}

function applyDerivedFieldDefaults(manifest: MapManifest): boolean {
  const fallbackResidents = Number.isFinite(manifest.population)
    ? manifest.population
    : 0;
  const nextResidentsTotal = Number.isFinite(manifest.residents_total)
    ? manifest.residents_total
    : fallbackResidents;
  const nextPointsCount = Number.isFinite(manifest.points_count)
    ? manifest.points_count
    : 0;
  const nextPopulationCount = Number.isFinite(manifest.population_count)
    ? manifest.population_count
    : 0;

  const changed = (
    manifest.residents_total !== nextResidentsTotal
    || manifest.points_count !== nextPointsCount
    || manifest.population_count !== nextPopulationCount
  );

  manifest.residents_total = nextResidentsTotal;
  manifest.points_count = nextPointsCount;
  manifest.population_count = nextPopulationCount;
  return changed;
}

function shouldSkipUnchanged(
  cacheEntry: DemandStatsCacheEntry | undefined,
  resolvedSource: ResolvedInstallTarget,
  now: Date,
): boolean {
  if (!cacheEntry) return false;
  if (!cacheEntry.stats) return false;
  if (cacheEntry.source_fingerprint !== resolvedSource.sourceFingerprint) return false;
  if (resolvedSource.sourceFingerprint.startsWith("sha256:")) {
    return true;
  }
  const lastChecked = Date.parse(cacheEntry.last_checked_at);
  if (!Number.isFinite(lastChecked)) return false;
  return now.getTime() - lastChecked <= UNCHANGED_SKIP_WINDOW_MS;
}

export async function extractDemandStatsFromZipBuffer(
  listingId: string,
  zipBuffer: Buffer,
): Promise<DemandStats> {
  let zip: JSZip;
  try {
    zip = await JSZip.loadAsync(zipBuffer);
  } catch {
    throw new Error(`listing=${listingId}: ZIP could not be opened`);
  }

  const entry = findDemandDataEntry(zip);
  if (!entry) {
    throw new Error(`listing=${listingId}: demand_data.json or demand_data.json.gz not found in ZIP`);
  }

  let rawText: string;
  try {
    if (entry.name.toLowerCase().endsWith(".gz")) {
      const compressed = await entry.async("nodebuffer");
      rawText = gunzipSync(compressed).toString("utf-8");
    } else {
      rawText = await entry.async("string");
    }
  } catch {
    throw new Error(`listing=${listingId}: failed to read demand data entry '${entry.name}'`);
  }

  let payload: unknown;
  try {
    payload = JSON.parse(rawText);
  } catch {
    throw new Error(`listing=${listingId}: demand data file is not valid JSON`);
  }

  return parseDemandDataPayload(payload);
}

async function resolveZipUrlForMapSource(
  listingId: string,
  update: MapUpdateSource,
  fetchImpl: typeof fetch,
  token: string | undefined,
  warnings: string[],
): Promise<ResolvedInstallTarget | null> {
  if (update.type === "custom") {
    return fetchCustomInstallTargetZipUrl(listingId, update.url, fetchImpl, warnings);
  }

  const usageState = createGraphqlUsageState();
  const { repoIndexes } = await fetchRepoReleaseIndexes([update.repo], {
    fetchImpl,
    token,
    warnings,
    usageState,
  });
  return getLatestGithubZipUrl(listingId, update.repo, repoIndexes, warnings);
}

export async function resolveAndExtractDemandStatsForMapSource(
  listingId: string,
  update: MapUpdateSource,
  options: {
    fetchImpl?: typeof fetch;
    token?: string;
  } = {},
): Promise<DemandStats> {
  const warnings: string[] = [];
  const fetchImpl = options.fetchImpl ?? fetch;
  const resolvedSource = await resolveZipUrlForMapSource(
    listingId,
    update,
    fetchImpl,
    options.token,
    warnings,
  );
  if (!resolvedSource) {
    throw new Error(warnings[0] ?? `listing=${listingId}: failed to resolve map ZIP URL`);
  }

  const zipBuffer = await fetchZipBuffer(listingId, resolvedSource.zipUrl, fetchImpl, warnings);
  if (!zipBuffer) {
    throw new Error(warnings[0] ?? `listing=${listingId}: failed to fetch map ZIP`);
  }

  return extractDemandStatsFromZipBuffer(listingId, zipBuffer);
}

export async function generateMapDemandStats(
  options: GenerateMapDemandStatsOptions,
): Promise<GenerateMapDemandStatsResult> {
  const repoRoot = options.repoRoot;
  const fetchImpl = options.fetchImpl ?? fetch;
  const token = options.token;
  const force = options.force === true;
  const mapId = typeof options.mapId === "string" && options.mapId.trim() !== ""
    ? options.mapId.trim()
    : undefined;
  const warnings: string[] = [];
  const allIds = getMapIds(repoRoot);
  if (mapId && !allIds.includes(mapId)) {
    throw new Error(`Map id '${mapId}' was not found in maps/index.json`);
  }
  const ids = mapId ? [mapId] : allIds;
  const cache = loadDemandStatsCache(repoRoot);
  const now = new Date();

  const manifests = new Map<string, MapManifest>();
  const githubRepos = new Set<string>();

  for (const id of ids) {
    try {
      const manifest = getMapManifest(repoRoot, id);
      manifests.set(id, manifest);
      if (manifest.update.type === "github") {
        githubRepos.add(manifest.update.repo.toLowerCase());
      }
    } catch (error) {
      warnListing(warnings, id, `failed to read map manifest (${(error as Error).message})`);
    }
  }

  const usageState = createGraphqlUsageState();
  const { repoIndexes } = await fetchRepoReleaseIndexes(githubRepos, {
    fetchImpl,
    token,
    warnings,
    usageState,
  });

  let processedMaps = 0;
  let updatedMaps = 0;
  let skippedMaps = 0;
  let skippedUnchanged = 0;
  let extractionFailures = 0;
  let residentsDeltaTotal = 0;

  for (const id of ids) {
    processedMaps += 1;
    const manifest = manifests.get(id);
    if (!manifest) {
      skippedMaps += 1;
      extractionFailures += 1;
      continue;
    }

    let resolvedSource: ResolvedInstallTarget | null = null;
    if (manifest.update.type === "github") {
      resolvedSource = getLatestGithubZipUrl(id, manifest.update.repo, repoIndexes, warnings);
    } else {
      resolvedSource = await fetchCustomInstallTargetZipUrl(id, manifest.update.url, fetchImpl, warnings);
    }

    if (!resolvedSource) {
      skippedMaps += 1;
      extractionFailures += 1;
      if (applyDerivedFieldDefaults(manifest)) {
        writeFileSync(
          resolve(repoRoot, "maps", id, "manifest.json"),
          `${JSON.stringify(manifest, null, 2)}\n`,
          "utf-8",
        );
        updatedMaps += 1;
      }
      continue;
    }

    if (!force && shouldSkipUnchanged(cache[id], resolvedSource, now)) {
      skippedMaps += 1;
      skippedUnchanged += 1;
      const cachedStats = cache[id]?.stats;
      if (cachedStats) {
        const oldResidents = Number.isFinite(manifest.residents_total)
          ? manifest.residents_total
          : (Number.isFinite(manifest.population) ? manifest.population : 0);
        const changed = (
          manifest.population !== cachedStats.residents_total
          || manifest.residents_total !== cachedStats.residents_total
          || manifest.points_count !== cachedStats.points_count
          || manifest.population_count !== cachedStats.population_count
        );
        if (changed) {
          manifest.population = cachedStats.residents_total;
          manifest.residents_total = cachedStats.residents_total;
          manifest.points_count = cachedStats.points_count;
          manifest.population_count = cachedStats.population_count;
          writeFileSync(
            resolve(repoRoot, "maps", id, "manifest.json"),
            `${JSON.stringify(manifest, null, 2)}\n`,
            "utf-8",
          );
          updatedMaps += 1;
          residentsDeltaTotal += cachedStats.residents_total - oldResidents;
        }
      }
      continue;
    }

    const zipBuffer = await fetchZipBuffer(id, resolvedSource.zipUrl, fetchImpl, warnings);
    if (!zipBuffer) {
      skippedMaps += 1;
      extractionFailures += 1;
      if (applyDerivedFieldDefaults(manifest)) {
        writeFileSync(
          resolve(repoRoot, "maps", id, "manifest.json"),
          `${JSON.stringify(manifest, null, 2)}\n`,
          "utf-8",
        );
        updatedMaps += 1;
      }
      continue;
    }

    let stats: DemandStats;
    try {
      stats = await extractDemandStatsFromZipBuffer(id, zipBuffer);
    } catch (error) {
      warnListing(warnings, id, String((error as Error).message));
      skippedMaps += 1;
      extractionFailures += 1;
      if (applyDerivedFieldDefaults(manifest)) {
        writeFileSync(
          resolve(repoRoot, "maps", id, "manifest.json"),
          `${JSON.stringify(manifest, null, 2)}\n`,
          "utf-8",
        );
        updatedMaps += 1;
      }
      continue;
    }

    const oldResidents = Number.isFinite(manifest.residents_total)
      ? manifest.residents_total
      : (Number.isFinite(manifest.population) ? manifest.population : 0);

    const changed = (
      manifest.population !== stats.residents_total
      || manifest.residents_total !== stats.residents_total
      || manifest.points_count !== stats.points_count
      || manifest.population_count !== stats.population_count
    );

    manifest.population = stats.residents_total;
    manifest.residents_total = stats.residents_total;
    manifest.points_count = stats.points_count;
    manifest.population_count = stats.population_count;
    cache[id] = {
      source_fingerprint: resolvedSource.sourceFingerprint,
      last_checked_at: now.toISOString(),
      stats,
    };

    if (changed) {
      writeFileSync(
        resolve(repoRoot, "maps", id, "manifest.json"),
        `${JSON.stringify(manifest, null, 2)}\n`,
        "utf-8",
      );
      updatedMaps += 1;
      residentsDeltaTotal += stats.residents_total - oldResidents;
    }
  }
  writeDemandStatsCache(repoRoot, cache);

  return {
    processedMaps,
    updatedMaps,
    skippedMaps,
    skippedUnchanged,
    extractionFailures,
    residentsDeltaTotal,
    warnings,
    rateLimit: graphqlUsageSnapshot(usageState),
  };
}
