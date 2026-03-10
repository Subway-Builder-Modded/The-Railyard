import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { validateCustomUpdateUrl } from "./lib/custom-url.js";
import { validateGitHubRepo } from "./lib/github.js";
import {
  type ManifestType,
  type MapManifest,
  type ModManifest,
  resolveListingIdAndDir,
  resolveListingKind,
} from "./lib/manifests.js";
import {
  DEFAULT_MAP_DATA_SOURCE,
  LEVEL_OF_DETAIL_SET,
  LOCATION_TAG_SET,
  SOURCE_QUALITY_SET,
  SPECIAL_DEMAND_TAG_SET,
  isOsmDataSource,
} from "./lib/map-constants.js";

const REPO_ROOT = resolve(import.meta.dirname, "..");

function isPresent(value: unknown): value is string {
  return typeof value === "string"
    && value !== ""
    && value !== "_No response_"
    && value !== "None"
    && value !== "No change";
}

function getString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function parseCheckedBoxes(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw.map((tag) => String(tag).trim()).filter(Boolean);
  }
  if (typeof raw !== "string" || !raw || raw === "_No response_") return [];
  return raw
    .split("\n")
    .filter((line) => line.startsWith("- [X]") || line.startsWith("- [x]"))
    .map((line) => line.replace(/^- \[[Xx]\]\s*/, "").trim())
    .filter(Boolean);
}

function validateMapUpdateFields(
  manifest: MapManifest,
  data: Record<string, unknown>,
  errors: string[],
): void {
  const currentDataSource = String(manifest.data_source ?? DEFAULT_MAP_DATA_SOURCE);
  const currentSourceQuality = String(manifest.source_quality ?? "");
  const currentLevelOfDetail = String(manifest.level_of_detail ?? "");
  const currentLocation = String(manifest.location ?? "");
  const currentSpecialDemand = (Array.isArray(manifest.special_demand)
    ? manifest.special_demand
    : []).filter((tag): tag is string => typeof tag === "string");

  const nextDataSource = isPresent(data.data_source) ? data.data_source : currentDataSource;
  const nextSourceQuality = isPresent(data.source_quality)
    ? data.source_quality
    : currentSourceQuality;
  const nextLevelOfDetail = isPresent(data.level_of_detail)
    ? data.level_of_detail
    : currentLevelOfDetail;
  const nextLocation = isPresent(data.location) ? data.location : currentLocation;
  const nextSpecialDemand =
    data.special_demand !== undefined && data.special_demand !== "_No response_" && data.special_demand !== "None"
      ? parseCheckedBoxes(data.special_demand)
      : currentSpecialDemand;

  if (!SOURCE_QUALITY_SET.has(nextSourceQuality)) {
    errors.push("**source_quality**: Must be one of `low-quality`, `medium-quality`, `high-quality`.");
  }
  if (!LEVEL_OF_DETAIL_SET.has(nextLevelOfDetail)) {
    errors.push("**level_of_detail**: Must be one of `low-detail`, `medium-detail`, `high-detail`.");
  }
  if (!LOCATION_TAG_SET.has(nextLocation)) {
    errors.push("**location**: Must be one of the supported location tags.");
  }

  const invalidSpecialDemand = nextSpecialDemand.filter((tag) => !SPECIAL_DEMAND_TAG_SET.has(tag));
  if (invalidSpecialDemand.length > 0) {
    errors.push(`**special_demand**: Invalid tag(s): ${invalidSpecialDemand.join(", ")}`);
  }

  if (isOsmDataSource(nextDataSource) && nextSourceQuality === "high-quality") {
    errors.push("**source_quality**: OSM-based data sources cannot be marked `high-quality`.");
  }
}

function resolveSourceUrl(
  data: Record<string, unknown>,
  existingManifest: ModManifest | MapManifest | null,
): string | undefined {
  if (isPresent(data.source)) return data.source;
  if (existingManifest && isPresent(existingManifest.source)) return existingManifest.source;
  return undefined;
}

async function validateGitHubUpdate(
  updateType: string | undefined,
  githubRepo: string | undefined,
  sourceUrl: string | undefined,
  listingKind: ManifestType,
  errors: string[],
): Promise<void> {
  if (updateType === "GitHub Releases" && isPresent(githubRepo)) {
    if (!/^[^/]+\/[^/]+$/.test(githubRepo)) {
      errors.push("**github-repo**: Must provide a valid `owner/repo` when using GitHub Releases.");
      return;
    }
    const ghErrors = await validateGitHubRepo(githubRepo, sourceUrl, listingKind);
    errors.push(...ghErrors);
    return;
  }

  if (!updateType && isPresent(githubRepo)) {
    if (!/^[^/]+\/[^/]+$/.test(githubRepo)) {
      errors.push("**github-repo**: Must provide a valid `owner/repo` when using GitHub Releases.");
      return;
    }
    const ghErrors = await validateGitHubRepo(githubRepo, sourceUrl, listingKind);
    errors.push(...ghErrors);
  }
}

async function validateCustomUrlUpdate(
  updateType: string | undefined,
  customUpdateUrl: string | undefined,
  listingKind: ManifestType,
  errors: string[],
): Promise<void> {
  if (updateType === "Custom URL" && isPresent(customUpdateUrl)) {
    try {
      new URL(customUpdateUrl);
      const urlErrors = await validateCustomUpdateUrl(customUpdateUrl, listingKind);
      errors.push(...urlErrors);
    } catch {
      errors.push("**custom-update-url**: Must be a valid URL.");
    }
    return;
  }

  if (!updateType && isPresent(customUpdateUrl)) {
    try {
      new URL(customUpdateUrl);
      const urlErrors = await validateCustomUpdateUrl(customUpdateUrl, listingKind);
      errors.push(...urlErrors);
    } catch {
      errors.push("**custom-update-url**: Must be a valid URL.");
    }
  }
}

async function main() {
  const listingKind = resolveListingKind(process.env.LISTING_TYPE);
  const issueJson = process.env.ISSUE_JSON;
  const issueAuthorId = process.env.ISSUE_AUTHOR_ID;

  if (!issueJson || !issueAuthorId) {
    console.error("ISSUE_JSON and ISSUE_AUTHOR_ID environment variables are required");
    process.exit(1);
  }

  const data = JSON.parse(issueJson) as Record<string, unknown>;
  const { id, dir } = resolveListingIdAndDir(listingKind, data);
  const errors: string[] = [];
  let existingManifest: ModManifest | MapManifest | null = null;

  if (!id || typeof id !== "string") {
    errors.push(`**${listingKind}-id**: Must provide a valid ${listingKind} ID.`);
  } else {
    const manifestPath = resolve(REPO_ROOT, dir, id, "manifest.json");

    if (!existsSync(manifestPath)) {
      errors.push(`**${listingKind}-id**: No ${listingKind} with ID \`${id}\` exists in the registry.`);
    } else {
      const manifest = JSON.parse(readFileSync(manifestPath, "utf-8")) as
        | ModManifest
        | MapManifest;
      existingManifest = manifest;
      const ownerId = String(manifest.github_id);
      const authorId = String(issueAuthorId);

      if (ownerId !== authorId) {
        errors.push(
          `**Ownership check failed**: Your GitHub account does not match the original publisher of \`${id}\`. `
          + `Only the original publisher can update this listing.`,
        );
      }

      if (listingKind === "map") {
        validateMapUpdateFields(manifest as MapManifest, data, errors);
      }
    }
  }

  const sourceUrl = resolveSourceUrl(data, existingManifest);
  const githubRepo = getString(data["github-repo"]);
  const customUpdateUrl = getString(data["custom-update-url"]);
  const updateType = getString(data["update-type"]);

  await validateGitHubUpdate(updateType, githubRepo, sourceUrl, listingKind, errors);
  await validateCustomUrlUpdate(updateType, customUpdateUrl, listingKind, errors);

  if (errors.length > 0) {
    const errorMessage = [
      "Update validation failed:\n",
      ...errors.map((e) => `- ${e}`),
      "\nIf you believe this is an error, please contact a maintainer.",
    ].join("\n");

    writeFileSync(resolve(REPO_ROOT, "validation-error.md"), errorMessage);
    console.error(errorMessage);
    process.exit(1);
  }

  console.log("Update validation passed.");
}

main();
