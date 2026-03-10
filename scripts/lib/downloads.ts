import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { ListingManifest, ManifestDirectory } from "./manifests.js";
import * as D from "./download-definitions.js";

export type {
  ParsedReleaseAssetUrl,
  DownloadsByListing,
  GenerateDownloadsOptions,
  GenerateDownloadsResult,
} from "./download-definitions.js";

const GRAPHQL_RATE_LIMIT_WARN_THRESHOLD = D.GRAPHQL_RATE_LIMIT_WARN_THRESHOLD;
const GRAPHQL_ENDPOINT = D.GRAPHQL_ENDPOINT;
const REPO_RELEASES_QUERY = D.REPO_RELEASES_QUERY;
const SEMVER_RELEASE_TAG_REGEX = /^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;

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

export function isSupportedReleaseTag(tag: string): boolean {
  return SEMVER_RELEASE_TAG_REGEX.test(tag);
}

/**
 * Parses a GitHub release asset download URL of the form:
 * `https://github.com/<owner>/<repo>/releases/download/<tag>/<asset>`
 *
 * Returns normalized repo metadata, or `null` if URL is not a valid
 * GitHub release asset URL.
 */
export function parseGitHubReleaseAssetDownloadUrl(
  url: string,
): D.ParsedReleaseAssetUrl | null {
  if (!isNonEmptyString(url)) return null;
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }

  if (parsed.hostname.toLowerCase() !== "github.com") {
    return null;
  }

  const segments = parsed.pathname.split("/").filter(Boolean);
  // /owner/repo/releases/download/<tag>/<asset>
  if (segments.length < 6) return null;
  if (segments[2] !== "releases" || segments[3] !== "download") return null;

  const owner = decodeURIComponent(segments[0]).trim();
  const name = decodeURIComponent(segments[1]).trim();
  const tag = decodeURIComponent(segments[4]).trim();
  const assetName = decodeURIComponent(segments.slice(5).join("/")).trim();
  if (!owner || !name || !tag || !assetName) return null;

  return {
    repo: `${owner}/${name}`.toLowerCase(),
    owner,
    name,
    tag,
    assetName,
  };
}

/**
 * Builds a per-tag download index from release payloads.
 *
 * Each tag stores:
 * - `zipTotal`: cumulative downloads across `.zip` assets only
 * - `assets`: lookup map of all asset names to their raw download counts
 */
export function aggregateZipDownloadCountsByTag(releases: Array<{
  tagName: string;
  assets: Array<{ name: string; downloadCount: number }>;
}>): Map<string, D.RepoReleaseTagData> {
  const byTag = new Map<string, D.RepoReleaseTagData>();
  for (const release of releases) {
    if (!isNonEmptyString(release.tagName)) continue;
    const assets = new Map<string, number>();
    let zipTotal = 0;

    for (const asset of release.assets) {
      if (!isNonEmptyString(asset.name) || !Number.isFinite(asset.downloadCount)) continue;
      assets.set(asset.name, asset.downloadCount);
      if (asset.name.toLowerCase().endsWith(".zip")) {
        zipTotal += asset.downloadCount;
      }
    }

    byTag.set(release.tagName, { zipTotal, assets });
  }

  return byTag;
}

function splitRepo(repo: string): { owner: string; name: string } | null {
  const [owner, name] = repo.split("/");
  if (!owner || !name) return null;
  return { owner, name };
}

function buildGraphqlHeaders(token: string | undefined): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "application/json",
  };
  if (token) headers.Authorization = `Bearer ${token}`;
  return headers;
}

function updateGraphqlUsage(
  usageState: D.GraphqlUsageState,
  rateLimit: D.GraphqlRateLimitInfo | undefined,
): void {
  if (!rateLimit) return;
  usageState.queries += 1;
  usageState.totalCost += Number.isFinite(rateLimit.cost) ? rateLimit.cost : 0;
  if (usageState.firstRemaining === null && Number.isFinite(rateLimit.remaining)) {
    usageState.firstRemaining = rateLimit.remaining;
  }
  if (Number.isFinite(rateLimit.remaining)) {
    usageState.lastRemaining = rateLimit.remaining;
  }
  if (typeof rateLimit.resetAt === "string" && rateLimit.resetAt.trim() !== "") {
    usageState.resetAt = rateLimit.resetAt;
  }
}

function maybeWarnLowRateLimit(
  warnings: string[],
  rateLimit: D.GraphqlRateLimitInfo | undefined,
  rateLimitWarningState: D.RateLimitWarningState,
): void {
  if (
    rateLimit
    && typeof rateLimit.remaining === "number"
    && rateLimit.remaining <= GRAPHQL_RATE_LIMIT_WARN_THRESHOLD
    && !rateLimitWarningState.warned
  ) {
    warn(
      warnings,
      `GraphQL rate limit low: remaining=${rateLimit.remaining}, cost=${rateLimit.cost}, resetAt=${rateLimit.resetAt}`,
    );
    rateLimitWarningState.warned = true;
  }
}

async function requestRepoReleasesPage(
  repo: string,
  owner: string,
  name: string,
  cursor: string | null,
  fetchImpl: typeof fetch,
  token: string | undefined,
): Promise<D.RepoReleasesPageResult> {
  let response: Response;
  try {
    response = await fetchImpl(GRAPHQL_ENDPOINT, {
      method: "POST",
      headers: buildGraphqlHeaders(token),
      body: JSON.stringify({
        query: REPO_RELEASES_QUERY,
        variables: {
          owner,
          name,
          cursor,
        },
      }),
    });
  } catch (error) {
    return { ok: false, error: `repo=${repo}: GraphQL request failed (${(error as Error).message})` };
  }

  if (!response.ok) {
    return { ok: false, error: `repo=${repo}: GraphQL returned HTTP ${response.status}` };
  }

  let payload: D.GraphqlReleasesResponse;
  try {
    payload = await response.json() as D.GraphqlReleasesResponse;
  } catch {
    return { ok: false, error: `repo=${repo}: GraphQL returned non-JSON response` };
  }

  if (Array.isArray(payload.errors) && payload.errors.length > 0) {
    return {
      ok: false,
      error: `repo=${repo}: GraphQL errors: ${payload.errors.map((error) => error.message).join("; ")}`,
    };
  }

  const releases = payload.data?.repository?.releases;
  if (!releases) {
    return { ok: false, error: `repo=${repo}: repository not found or no releases access` };
  }

  return {
    ok: true,
    page: {
      releases,
      rateLimit: payload.data?.rateLimit,
    },
  };
}

/**
 * Queries GitHub GraphQL for all releases in a repository (paginated),
 * then builds a release/tag index used to resolve listing version counts.
 *
 * On any request or schema error, returns `null` and appends a warning
 * so callers can continue producing partial output.
 */
async function fetchGraphqlReleaseIndexForRepo(
  repo: string,
  fetchImpl: typeof fetch,
  token: string | undefined,
  warnings: string[],
  rateLimitWarningState: D.RateLimitWarningState,
  usageState: D.GraphqlUsageState,
): Promise<D.RepoReleaseIndex | null> {
  // TODO: Performance optimization for larger registries:
  // Batch multiple repositories into a single GraphQL operation using aliases
  // (e.g., r0: repository(...), r1: repository(...)) while tracking per-repo
  // pagination cursors. This reduces HTTP round-trips but still requires
  // iterative requests until each repo's releases.pageInfo.hasNextPage is false.
  const repoParts = splitRepo(repo);
  if (!repoParts) {
    warn(warnings, `repo=${repo}: invalid owner/repo format`);
    return null;
  }

  const byTag = new Map<string, D.RepoReleaseTagData>();
  let cursor: string | null = null;

  for (; ;) {
    const pageResult = await requestRepoReleasesPage(
      repo,
      repoParts.owner,
      repoParts.name,
      cursor,
      fetchImpl,
      token,
    );
    if (!pageResult.ok) {
      warn(warnings, pageResult.error);
      return null;
    }
    const { releases, rateLimit } = pageResult.page;

    updateGraphqlUsage(usageState, rateLimit);
    maybeWarnLowRateLimit(warnings, rateLimit, rateLimitWarningState);

    for (const release of releases.nodes) {
      const assets = release.releaseAssets.nodes.map((asset) => ({
        name: asset.name,
        downloadCount: asset.downloadCount,
      }));
      if (release.releaseAssets.pageInfo.hasNextPage) {
        warn(
          warnings,
          `repo=${repo} tag=${release.tagName}: release has >100 assets; only first 100 considered`,
        );
      }

      const entries = aggregateZipDownloadCountsByTag([
        { tagName: release.tagName, assets },
      ]);
      const data = entries.get(release.tagName);
      if (data) {
        byTag.set(release.tagName, data);
      }
    }

    if (!releases.pageInfo.hasNextPage) {
      break;
    }
    cursor = releases.pageInfo.endCursor;
    if (!cursor) break;
  }

  return { byTag };
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
  const rateLimitWarningState: D.RateLimitWarningState = { warned: false };
  const usageState: D.GraphqlUsageState = {
    queries: 0,
    totalCost: 0,
    firstRemaining: null,
    lastRemaining: null,
    resetAt: null,
  };

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

  const repoIndexes = new Map<string, D.RepoReleaseIndex>();
  for (const repo of Array.from(repoSet).sort()) {
    const index = await fetchGraphqlReleaseIndexForRepo(
      repo,
      fetchImpl,
      token,
      warnings,
      rateLimitWarningState,
      usageState,
    );
    if (index) {
      repoIndexes.set(repo, index);
    }
  }

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
      if (data.zipTotal > 0) {
        releaseCounts[tag] = data.zipTotal;
      }
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
    const count = release.assets.get(versionRef.assetName);
    if (count === undefined) {
      warnListing(
        warnings,
        versionRef.listingId,
        `skipped (asset '${versionRef.assetName}' not found)`,
        versionRef.version,
      );
      continue;
    }
    downloadsByListing[versionRef.listingId][versionRef.version] = count;
  }

  const sortedDownloads: D.DownloadsByListing = {};
  for (const id of [...ids].sort()) {
    sortedDownloads[id] = sortObjectByKeys(downloadsByListing[id] ?? {});
  }

  const estimatedConsumed = (
    usageState.firstRemaining !== null
    && usageState.lastRemaining !== null
  )
    ? (usageState.firstRemaining - usageState.lastRemaining)
    : null;

  return {
    downloads: sortedDownloads,
    warnings,
    rateLimit: {
      queries: usageState.queries,
      totalCost: usageState.totalCost,
      firstRemaining: usageState.firstRemaining,
      lastRemaining: usageState.lastRemaining,
      estimatedConsumed,
      resetAt: usageState.resetAt,
    },
  };
}
