import type { ManifestType } from "./manifests.js";
import type { IntegrityOutput, IntegrityCache } from "./integrity.js";

export interface ParsedReleaseAssetUrl {
  repo: string;
  owner: string;
  name: string;
  tag: string;
  assetName: string;
}

export interface GraphqlReleaseAssetNode {
  name: string;
  downloadCount: number;
  downloadUrl: string;
}

export interface GraphqlReleaseNode {
  tagName: string;
  releaseAssets: {
    nodes: GraphqlReleaseAssetNode[];
    pageInfo: {
      hasNextPage: boolean;
      endCursor: string | null;
    };
  };
}

export interface GraphqlRateLimitInfo {
  remaining: number;
  cost: number;
  resetAt: string;
}

export interface GraphqlReleasesResponse {
  data?: {
    repository: {
      releases: {
        nodes: GraphqlReleaseNode[];
        pageInfo: {
          hasNextPage: boolean;
          endCursor: string | null;
        };
      };
    } | null;
    rateLimit?: GraphqlRateLimitInfo;
  };
  errors?: Array<{ message: string }>;
}

export interface RepoReleaseTagData {
  zipTotal: number;
  assets: Map<string, {
    downloadCount: number;
    downloadUrl: string | null;
  }>;
}

export interface RepoReleaseIndex {
  byTag: Map<string, RepoReleaseTagData>;
}

export interface RateLimitWarningState {
  warned: boolean;
}

export interface GraphqlUsageState {
  queries: number;
  totalCost: number;
  firstRemaining: number | null;
  lastRemaining: number | null;
  resetAt: string | null;
}

export interface CustomVersionRef {
  listingId: string;
  version: string;
  repo: string;
  tag: string;
  assetName: string;
}

export interface DownloadsByListing {
  [listingId: string]: {
    [version: string]: number;
  };
}

export interface GenerateDownloadsOptions {
  repoRoot: string;
  listingType: ManifestType;
  mode?: "full" | "download-only";
  token?: string;
  fetchImpl?: typeof fetch;
}

export interface GenerateDownloadsResult {
  downloads: DownloadsByListing;
  integrity: IntegrityOutput;
  integrityCache: IntegrityCache;
  stats: {
    listings: number;
    versions_checked: number;
    complete_versions: number;
    incomplete_versions: number;
    filtered_versions: number;
    cache_hits: number;
  };
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

export interface RepoReleasesPage {
  releases: {
    nodes: GraphqlReleaseNode[];
    pageInfo: {
      hasNextPage: boolean;
      endCursor: string | null;
    };
  };
  rateLimit?: GraphqlRateLimitInfo;
}

export type RepoReleasesPageResult =
  | { ok: true; page: RepoReleasesPage }
  | { ok: false; error: string };

export const GRAPHQL_RATE_LIMIT_WARN_THRESHOLD = 200;
export const GRAPHQL_ENDPOINT = "https://api.github.com/graphql";

export const REPO_RELEASES_QUERY = `
  query RepoReleases($owner: String!, $name: String!, $cursor: String) {
    repository(owner: $owner, name: $name) {
      releases(first: 100, after: $cursor, orderBy: {field: CREATED_AT, direction: DESC}) {
        nodes {
          tagName
          releaseAssets(first: 100) {
            nodes {
              name
              downloadCount
              downloadUrl
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    rateLimit {
      remaining
      cost
      resetAt
    }
  }
`;

