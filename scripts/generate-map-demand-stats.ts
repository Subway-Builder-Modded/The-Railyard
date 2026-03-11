import { basename, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { generateMapDemandStats } from "./lib/map-demand-stats.js";

const FALLBACK_REPO_ROOT = basename(import.meta.dirname) === "dist"
  ? resolve(import.meta.dirname, "..", "..")
  : resolve(import.meta.dirname, "..");

function getNonEmptyEnv(name: string): string | undefined {
  const value = process.env[name];
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
}

function parseCliArgs(argv: string[]): { force: boolean; mapId?: string } {
  let force = false;
  let mapId: string | undefined;

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--force") {
      force = true;
      continue;
    }
    if (arg === "--id" || arg === "-id") {
      const value = argv[index + 1];
      if (!value || value.startsWith("-")) {
        throw new Error(`Missing map id value after '${arg}'`);
      }
      mapId = value.trim();
      index += 1;
      continue;
    }
    if (arg.startsWith("--id=") || arg.startsWith("-id=")) {
      const value = arg.slice(arg.indexOf("=") + 1).trim();
      if (value === "") {
        throw new Error(`Missing map id value in '${arg}'`);
      }
      mapId = value;
      continue;
    }
    throw new Error(`Unknown argument '${arg}'. Supported flags: --force, --id <map-id>, -id <map-id>.`);
  }

  return { force, mapId };
}

async function run(): Promise<void> {
  const cli = parseCliArgs(process.argv.slice(2));
  const repoRoot = process.env.RAILYARD_REPO_ROOT ?? FALLBACK_REPO_ROOT;
  const token = getNonEmptyEnv("GH_DOWNLOADS_TOKEN") ?? getNonEmptyEnv("GITHUB_TOKEN");
  const tokenSource = getNonEmptyEnv("GH_DOWNLOADS_TOKEN")
    ? "GH_DOWNLOADS_TOKEN"
    : (getNonEmptyEnv("GITHUB_TOKEN") ? "GITHUB_TOKEN" : "none");

  console.log(`[map-demand-stats] Auth token source: ${tokenSource}`);
  if (!token) {
    console.warn(
      "[map-demand-stats] No non-empty GitHub token configured (GH_DOWNLOADS_TOKEN/GITHUB_TOKEN). GraphQL requests are likely to fail with 401.",
    );
  }

  console.log(
    `[map-demand-stats] Run mode: ${cli.force ? "force" : "cached"}${cli.mapId ? `, mapId=${cli.mapId}` : ", mapId=all"}`,
  );

  const result = await generateMapDemandStats({
    repoRoot,
    token,
    force: cli.force,
    mapId: cli.mapId,
  });

  for (const warning of result.warnings) {
    console.warn(`[map-demand-stats] ${warning}`);
  }

  console.log(
    `[map-demand-stats] GraphQL usage: queries=${result.rateLimit.queries}, totalCost=${result.rateLimit.totalCost}, firstRemaining=${result.rateLimit.firstRemaining ?? "n/a"}, lastRemaining=${result.rateLimit.lastRemaining ?? "n/a"}, estimatedConsumed=${result.rateLimit.estimatedConsumed ?? "n/a"}, resetAt=${result.rateLimit.resetAt ?? "n/a"}`,
  );
  console.log(
    `[map-demand-stats] Summary: processedMaps=${result.processedMaps}, updatedMaps=${result.updatedMaps}, skippedMaps=${result.skippedMaps}, skippedUnchanged=${result.skippedUnchanged}, extractionFailures=${result.extractionFailures}, residentsDeltaTotal=${result.residentsDeltaTotal}`,
  );

  if (process.env.GITHUB_OUTPUT) {
    const outputLines = [
      `processed_maps=${result.processedMaps}`,
      `updated_maps=${result.updatedMaps}`,
      `skipped_maps=${result.skippedMaps}`,
      `skipped_unchanged=${result.skippedUnchanged}`,
      `extraction_failures=${result.extractionFailures}`,
      `residents_delta_total=${result.residentsDeltaTotal}`,
      `graphql_queries=${result.rateLimit.queries}`,
      `graphql_total_cost=${result.rateLimit.totalCost}`,
    ];
    const { appendFileSync } = await import("node:fs");
    appendFileSync(process.env.GITHUB_OUTPUT, `${outputLines.join("\n")}\n`);
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  run().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
