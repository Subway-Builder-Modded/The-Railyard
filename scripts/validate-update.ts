import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { validateCustomUpdateUrl } from "./lib/custom-url.js";
import { validateGitHubRepo } from "./lib/github.js";

const REPO_ROOT = resolve(import.meta.dirname, "..");

async function main() {
  const type = process.env.LISTING_TYPE; // "mod" or "map"
  const issueJson = process.env.ISSUE_JSON;
  const issueAuthorId = process.env.ISSUE_AUTHOR_ID;

  if (!issueJson || !issueAuthorId) {
    console.error("ISSUE_JSON and ISSUE_AUTHOR_ID environment variables are required");
    process.exit(1);
  }

  const data = JSON.parse(issueJson);
  const id = type === "map" ? data["map-id"] : data["mod-id"];
  const errors: string[] = [];

  if (!id || typeof id !== "string") {
    errors.push(`**${type}-id**: Must provide a valid ${type} ID.`);
  } else {
    const dir = type === "map" ? "maps" : "mods";
    const manifestPath = resolve(REPO_ROOT, dir, id, "manifest.json");

    if (!existsSync(manifestPath)) {
      errors.push(`**${type}-id**: No ${type} with ID \`${id}\` exists in the registry.`);
    } else {
      const manifest = JSON.parse(readFileSync(manifestPath, "utf-8"));
      const ownerId = String(manifest.github_id);
      const authorId = String(issueAuthorId);

      if (ownerId !== authorId) {
        errors.push(
          `**Ownership check failed**: Your GitHub account does not match the original publisher of \`${id}\`. ` +
          `Only the original publisher can update this listing.`
        );
      }
    }
  }

  // Validate GitHub repo if update-type is being changed to GitHub Releases or github-repo is being updated
  const githubRepo = data["github-repo"];
  const customUpdateUrl = data["custom-update-url"];
  const updateType = data["update-type"];

  // Resolve source URL for monorepo detection: use updated source if provided, otherwise fall back to existing manifest
  const sourceUrl = data.source || (id ? (() => {
    const dir = type === "map" ? "maps" : "mods";
    const mp = resolve(REPO_ROOT, dir, id, "manifest.json");
    try { return JSON.parse(readFileSync(mp, "utf-8")).source; } catch { return undefined; }
  })() : undefined);

  if (updateType === "GitHub Releases" && githubRepo) {
    if (!/^[^/]+\/[^/]+$/.test(githubRepo)) {
      errors.push("**github-repo**: Must provide a valid `owner/repo` when using GitHub Releases.");
    } else {
      const ghErrors = await validateGitHubRepo(githubRepo, sourceUrl);
      errors.push(...ghErrors);
    }
  } else if (!updateType && githubRepo && githubRepo.trim() !== "") {
    // github-repo is being updated without changing update-type — still validate it
    if (!/^[^/]+\/[^/]+$/.test(githubRepo)) {
      errors.push("**github-repo**: Must provide a valid `owner/repo` when using GitHub Releases.");
    } else {
      const ghErrors = await validateGitHubRepo(githubRepo, sourceUrl);
      errors.push(...ghErrors);
    }
  }

  // Validate custom update URL if update-type is being changed to Custom URL or custom-update-url is being updated
  if (updateType === "Custom URL" && customUpdateUrl) {
    try {
      new URL(customUpdateUrl);
      const urlErrors = await validateCustomUpdateUrl(customUpdateUrl);
      errors.push(...urlErrors);
    } catch {
      errors.push("**custom-update-url**: Must be a valid URL.");
    }
  } else if (!updateType && customUpdateUrl && customUpdateUrl.trim() !== "") {
    try {
      new URL(customUpdateUrl);
      const urlErrors = await validateCustomUpdateUrl(customUpdateUrl);
      errors.push(...urlErrors);
    } catch {
      errors.push("**custom-update-url**: Must be a valid URL.");
    }
  }

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
