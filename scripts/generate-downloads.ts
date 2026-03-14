import { writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { generateDownloadsData } from "./lib/downloads.js";
import type { ManifestType } from "./lib/manifests.js";

const FALLBACK_REPO_ROOT = basename(import.meta.dirname) === "dist"
  ? resolve(import.meta.dirname, "..", "..")
  : resolve(import.meta.dirname, "..");

function getNonEmptyEnv(name: string): string | undefined {
  const value = process.env[name];
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
}

function getArgValue(name: string): string | undefined {
  const exact = `--${name}=`;
  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith(exact)) {
      return arg.slice(exact.length);
    }
  }

  const args = process.argv.slice(2);
  for (let index = 0; index < args.length; index += 1) {
    if (args[index] === `--${name}`) {
      return args[index + 1];
    }
  }
  return undefined;
}

function resolveListingType(rawValue: string | undefined): ManifestType {
  if (rawValue === "map" || rawValue === "mod") {
    return rawValue;
  }
  throw new Error("Missing or invalid --type. Expected one of: map, mod");
}

function resolveMode(rawValue: string | undefined): "full" | "download-only" {
  if (!rawValue || rawValue.trim() === "") return "full";
  if (rawValue === "full" || rawValue === "download-only") {
    return rawValue;
  }
  throw new Error("Missing or invalid --mode. Expected one of: full, download-only");
}

function toWarningsOutputJson(listingType: ManifestType, warnings: string[]): string {
  const MAX_WARNINGS = 30;
  const normalized = warnings
    .map((warning) => warning.trim())
    .filter((warning) => warning !== "")
    .map((warning) => `${listingType}: ${warning}`);
  const displayed = normalized.slice(0, MAX_WARNINGS);
  if (normalized.length > displayed.length) {
    displayed.push(`...and ${normalized.length - displayed.length} more warnings`);
  }
  return JSON.stringify(displayed);
}

async function run(): Promise<void> {
  const listingType = resolveListingType(
    getArgValue("type") ?? process.env.LISTING_TYPE,
  );
  const mode = resolveMode(getArgValue("mode") ?? process.env.DOWNLOADS_MODE);
  const repoRoot = process.env.RAILYARD_REPO_ROOT ?? FALLBACK_REPO_ROOT;
  const ghDownloadsToken = getNonEmptyEnv("GH_DOWNLOADS_TOKEN");
  const githubToken = getNonEmptyEnv("GITHUB_TOKEN");
  const token = ghDownloadsToken ?? githubToken;
  const tokenSource = ghDownloadsToken
    ? "GH_DOWNLOADS_TOKEN"
    : (githubToken ? "GITHUB_TOKEN" : "none");
  console.log(`[downloads] Auth token source: ${tokenSource}`);
  if (!token) {
    console.warn(
      "[downloads] No non-empty GitHub token configured (GH_DOWNLOADS_TOKEN/GITHUB_TOKEN). GraphQL requests are likely to fail with 401.",
    );
  }

  const {
    downloads,
    integrity,
    integrityCache,
    stats,
    warnings,
    rateLimit,
  } = await generateDownloadsData({
    repoRoot,
    listingType,
    mode,
    token,
  });

  const outputDir = listingType === "map" ? "maps" : "mods";
  const outputPath = resolve(repoRoot, outputDir, "downloads.json");
  const integrityPath = resolve(repoRoot, outputDir, "integrity.json");
  const integrityCachePath = resolve(repoRoot, outputDir, "integrity-cache.json");
  writeFileSync(outputPath, `${JSON.stringify(downloads, null, 2)}\n`, "utf-8");
  if (mode === "full") {
    writeFileSync(integrityPath, `${JSON.stringify(integrity, null, 2)}\n`, "utf-8");
    writeFileSync(integrityCachePath, `${JSON.stringify(integrityCache, null, 2)}\n`, "utf-8");
  }

  for (const warning of warnings) {
    console.warn(`[downloads] ${warning}`);
  }

  console.log(
    `[downloads] Mode: ${mode}`,
  );
  console.log(
    `[downloads] GraphQL usage: queries=${rateLimit.queries}, totalCost=${rateLimit.totalCost}, firstRemaining=${rateLimit.firstRemaining ?? "n/a"}, lastRemaining=${rateLimit.lastRemaining ?? "n/a"}, estimatedConsumed=${rateLimit.estimatedConsumed ?? "n/a"}, resetAt=${rateLimit.resetAt ?? "n/a"}`,
  );
  console.log(
    `[downloads] Integrity stats: listings=${stats.listings}, versionsChecked=${stats.versions_checked}, completeVersions=${stats.complete_versions}, incompleteVersions=${stats.incomplete_versions}, filteredVersions=${stats.filtered_versions}, cacheHits=${stats.cache_hits}`,
  );

  const zeroValidSemverListings = Object.entries(downloads)
    .filter(([, versions]) => Object.keys(versions).length === 0)
    .map(([id]) => id)
    .sort();
  if (zeroValidSemverListings.length > 0) {
    console.warn(
      `[downloads] Listings with zero valid semver tags (${zeroValidSemverListings.length}): ${zeroValidSemverListings.join(", ")}`,
    );
  } else {
    console.log("[downloads] Listings with zero valid semver tags: none");
  }

  console.log(
    mode === "full"
      ? `Generated ${outputDir}/downloads.json and ${outputDir}/integrity.json for ${Object.keys(downloads).length} listings`
      : `Generated ${outputDir}/downloads.json for ${Object.keys(downloads).length} listings (download-only mode)`,
  );

  if (process.env.GITHUB_OUTPUT) {
    const { appendFileSync } = await import("node:fs");
    const outputLines = [
      `warning_count=${warnings.length}`,
      `warnings_json=${toWarningsOutputJson(listingType, warnings)}`,
      `integrity_listings=${stats.listings}`,
      `integrity_versions_checked=${stats.versions_checked}`,
      `integrity_complete_versions=${stats.complete_versions}`,
      `integrity_incomplete_versions=${stats.incomplete_versions}`,
      `integrity_filtered_versions=${stats.filtered_versions}`,
      `integrity_cache_hits=${stats.cache_hits}`,
    ];
    appendFileSync(process.env.GITHUB_OUTPUT, `${outputLines.join("\n")}\n`);
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  run().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
