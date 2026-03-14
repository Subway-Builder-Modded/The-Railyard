import * as D from "./download-definitions.js";
import { fetchWithTimeout, resolveTimeoutMsFromEnv } from "./http.js";

const GRAPHQL_RATE_LIMIT_WARN_THRESHOLD = D.GRAPHQL_RATE_LIMIT_WARN_THRESHOLD;
const GRAPHQL_ENDPOINT = D.GRAPHQL_ENDPOINT;
const REPO_RELEASES_QUERY = D.REPO_RELEASES_QUERY;
const SEMVER_RELEASE_TAG_REGEX = /^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const GRAPHQL_HEARTBEAT_PREFIX = "[graphql]";
const GRAPHQL_REQUEST_TIMEOUT_MS = resolveTimeoutMsFromEnv("GRAPHQL_REQUEST_TIMEOUT_MS", 45_000);

export interface ParsedReleaseAssetUrl extends D.ParsedReleaseAssetUrl {}

export interface GraphqlUsageSnapshot {
  queries: number;
  totalCost: number;
  firstRemaining: number | null;
  lastRemaining: number | null;
  estimatedConsumed: number | null;
  resetAt: string | null;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim() !== "";
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

function warn(warnings: string[], message: string): void {
  warnings.push(message);
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
  const cursorLabel = cursor ?? "start";

  let response: Response;
  try {
    response = await fetchWithTimeout(
      fetchImpl,
      GRAPHQL_ENDPOINT,
      {
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
      },
      {
        timeoutMs: GRAPHQL_REQUEST_TIMEOUT_MS,
        heartbeatPrefix: GRAPHQL_HEARTBEAT_PREFIX,
        heartbeatLabel: `repo=${repo} cursor=${cursorLabel}`,
      },
    );
  } catch (error) {
    const errorMessage = (error as Error).message;
    return { ok: false, error: `repo=${repo}: GraphQL request failed (${errorMessage})` };
  }

  if (!response.ok) {
    if (response.status === 401) {
      const authHint = token
        ? "token is set but appears invalid or lacks access"
        : "token is missing/empty";
      return { ok: false, error: `repo=${repo}: GraphQL returned HTTP 401 (${authHint})` };
    }
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

function aggregateReleaseDataByTag(releases: Array<{
  tagName: string;
  assets: Array<{ name: string; downloadCount: number; downloadUrl: string | null }>;
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
        downloadUrl: asset.downloadUrl,
      });
      if (asset.name.toLowerCase().endsWith(".zip")) {
        zipTotal += asset.downloadCount;
      }
    }

    byTag.set(release.tagName, { zipTotal, assets });
  }

  return byTag;
}

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
        downloadUrl: asset.downloadUrl,
      }));
      if (release.releaseAssets.pageInfo.hasNextPage) {
        warn(
          warnings,
          `repo=${repo} tag=${release.tagName}: release has >100 assets; only first 100 considered`,
        );
      }

      const entries = aggregateReleaseDataByTag([
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

export function createGraphqlUsageState(): D.GraphqlUsageState {
  return {
    queries: 0,
    totalCost: 0,
    firstRemaining: null,
    lastRemaining: null,
    resetAt: null,
  };
}

export function graphqlUsageSnapshot(usageState: D.GraphqlUsageState): GraphqlUsageSnapshot {
  const estimatedConsumed = (
    usageState.firstRemaining !== null
    && usageState.lastRemaining !== null
  )
    ? (usageState.firstRemaining - usageState.lastRemaining)
    : null;

  return {
    queries: usageState.queries,
    totalCost: usageState.totalCost,
    firstRemaining: usageState.firstRemaining,
    lastRemaining: usageState.lastRemaining,
    estimatedConsumed,
    resetAt: usageState.resetAt,
  };
}

export async function fetchRepoReleaseIndexes(
  repos: Iterable<string>,
  options: {
    fetchImpl?: typeof fetch;
    token?: string;
    warnings: string[];
    usageState?: D.GraphqlUsageState;
  },
): Promise<{
  repoIndexes: Map<string, D.RepoReleaseIndex>;
  usageState: D.GraphqlUsageState;
}> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const usageState = options.usageState ?? createGraphqlUsageState();
  const rateLimitWarningState: D.RateLimitWarningState = { warned: false };
  const repoIndexes = new Map<string, D.RepoReleaseIndex>();

  const repoList = Array.from(new Set(Array.from(repos)))
    .map((repo) => repo.toLowerCase())
    .sort();

  for (const repo of repoList) {
    const index = await fetchGraphqlReleaseIndexForRepo(
      repo,
      fetchImpl,
      options.token,
      options.warnings,
      rateLimitWarningState,
      usageState,
    );
    if (index) {
      repoIndexes.set(repo, index);
    }
  }

  return { repoIndexes, usageState };
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
