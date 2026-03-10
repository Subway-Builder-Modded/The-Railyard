import { writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { generateDownloadsData } from "./lib/downloads.js";
import type { ManifestType } from "./lib/manifests.js";

const FALLBACK_REPO_ROOT = resolve(import.meta.dirname, "..");

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

async function run(): Promise<void> {
  const listingType = resolveListingType(
    getArgValue("type") ?? process.env.LISTING_TYPE,
  );
  const repoRoot = process.env.RAILYARD_REPO_ROOT ?? FALLBACK_REPO_ROOT;
  const token = process.env.GH_DOWNLOADS_TOKEN ?? process.env.GITHUB_TOKEN;

  const { downloads, warnings, rateLimit } = await generateDownloadsData({
    repoRoot,
    listingType,
    token,
  });

  const outputDir = listingType === "map" ? "maps" : "mods";
  const outputPath = resolve(repoRoot, outputDir, "downloads.json");
  writeFileSync(outputPath, `${JSON.stringify(downloads, null, 2)}\n`, "utf-8");

  for (const warning of warnings) {
    console.warn(`[downloads] ${warning}`);
  }

  console.log(
    `[downloads] GraphQL usage: queries=${rateLimit.queries}, totalCost=${rateLimit.totalCost}, firstRemaining=${rateLimit.firstRemaining ?? "n/a"}, lastRemaining=${rateLimit.lastRemaining ?? "n/a"}, estimatedConsumed=${rateLimit.estimatedConsumed ?? "n/a"}, resetAt=${rateLimit.resetAt ?? "n/a"}`,
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
    `Generated ${outputDir}/downloads.json for ${Object.keys(downloads).length} listings`,
  );
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  run().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
