import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { validateCustomUpdateUrl } from "./lib/custom-url.js";
import { validateGitHubRepo } from "./lib/github.js";
import {
  type ManifestType,
  type MapManifest,
  type ModManifest,
  resolveListingIdAndDir,
  resolveManifestType as resolveManifestType,
} from "./lib/manifests.js";
import { validateMapUpdateFields } from "./lib/map-update-logic.js";

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
  manifestType: ManifestType,
  errors: string[],
): Promise<void> {
  if (updateType === "GitHub Releases" && isPresent(githubRepo)) {
    if (!/^[^/]+\/[^/]+$/.test(githubRepo)) {
      errors.push("**github-repo**: Must provide a valid `owner/repo` when using GitHub Releases.");
      return;
    }
    const ghErrors = await validateGitHubRepo(githubRepo, sourceUrl, manifestType);
    errors.push(...ghErrors);
    return;
  }

  if (!updateType && isPresent(githubRepo)) {
    if (!/^[^/]+\/[^/]+$/.test(githubRepo)) {
      errors.push("**github-repo**: Must provide a valid `owner/repo` when using GitHub Releases.");
      return;
    }
    const ghErrors = await validateGitHubRepo(githubRepo, sourceUrl, manifestType);
    errors.push(...ghErrors);
  }
}

async function validateCustomUrlUpdate(
  updateType: string | undefined,
  customUpdateUrl: string | undefined,
  manifestType: ManifestType,
  errors: string[],
): Promise<void> {
  if (updateType === "Custom URL" && isPresent(customUpdateUrl)) {
    try {
      new URL(customUpdateUrl);
      const urlErrors = await validateCustomUpdateUrl(customUpdateUrl, manifestType);
      errors.push(...urlErrors);
    } catch {
      errors.push("**custom-update-url**: Must be a valid URL.");
    }
    return;
  }

  if (!updateType && isPresent(customUpdateUrl)) {
    try {
      new URL(customUpdateUrl);
      const urlErrors = await validateCustomUpdateUrl(customUpdateUrl, manifestType);
      errors.push(...urlErrors);
    } catch {
      errors.push("**custom-update-url**: Must be a valid URL.");
    }
  }
}

async function main() {
  const manifestType = resolveManifestType(process.env.LISTING_TYPE);
  const issueJson = process.env.ISSUE_JSON;
  const issueAuthorId = process.env.ISSUE_AUTHOR_ID;

  if (!issueJson || !issueAuthorId) {
    console.error("ISSUE_JSON and ISSUE_AUTHOR_ID environment variables are required");
    process.exit(1);
  }

  const data = JSON.parse(issueJson) as Record<string, unknown>;
  const { id, dir } = resolveListingIdAndDir(manifestType, data);
  const errors: string[] = [];
  let existingManifest: ModManifest | MapManifest | null = null;

  if (!id || typeof id !== "string") {
    errors.push(`**${manifestType}-id**: Must provide a valid ${manifestType} ID.`);
  } else {
    const manifestPath = resolve(REPO_ROOT, dir, id, "manifest.json");

    if (!existsSync(manifestPath)) {
      errors.push(`**${manifestType}-id**: No ${manifestType} with ID \`${id}\` exists in the registry.`);
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

      if (manifestType === "map") {
        validateMapUpdateFields(manifest as MapManifest, data, errors);
      }
    }
  }

  const sourceUrl = resolveSourceUrl(data, existingManifest);
  const githubRepo = getString(data["github-repo"]);
  const customUpdateUrl = getString(data["custom-update-url"]);
  const updateType = getString(data["update-type"]);

  await validateGitHubUpdate(updateType, githubRepo, sourceUrl, manifestType, errors);
  await validateCustomUrlUpdate(updateType, customUpdateUrl, manifestType, errors);

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
