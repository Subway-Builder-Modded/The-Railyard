import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  parseGalleryImages,
  resolveGalleryUrls,
  downloadGalleryImages,
} from "./lib/gallery.js";
import {
  type MapManifest,
  type ModManifest,
  resolveListingIdAndDir,
  resolveManifestType,
} from "./lib/manifests.js";
import {
  getOptionalIssueValue,
  isPresentIssueValue,
} from "./lib/map-field-utils.js";
import { applyMapManifestUpdates } from "./lib/map-update-logic.js";
import { resolveAndExtractDemandStatsForMapSource } from "./lib/map-demand-stats.js";
import { assertValidRegistryManifest } from "./lib/registry-manifest.js";

const REPO_ROOT = resolve(import.meta.dirname, "..");

function parseCheckedBoxes(raw: unknown): string[] | null {
  if (!raw) return null;
  if (Array.isArray(raw)) {
    const selected = raw.map((tag) => String(tag).trim()).filter(Boolean);
    return selected.length > 0 ? selected : null;
  }
  if (typeof raw !== "string") return null;
  const checked = raw
    .split("\n")
    .filter((line) => line.startsWith("- [X]") || line.startsWith("- [x]"))
    .map((line) => line.replace(/^- \[[Xx]\]\s*/, "").trim())
    .filter(Boolean);
  // Return null if nothing was checked (user wants to keep current tags)
  return checked.length > 0 ? checked : null;
}

function applyCommonMetadataUpdates(
  manifest: ModManifest,
  data: Record<string, unknown>,
): void {
  const description = getOptionalIssueValue(data.description);

  if (isPresentIssueValue(data.name)) manifest.name = data.name;
  if (description) manifest.description = description;
  if (isPresentIssueValue(data.source)) manifest.source = data.source;
}

function applyModTagUpdates(
  manifest: ModManifest,
  data: Record<string, unknown>,
): void {
  const newTags = parseCheckedBoxes(data.tags);
  if (newTags) manifest.tags = newTags;
}

function applyUpdateTypeChanges(
  manifest: ModManifest,
  data: Record<string, unknown>,
): void {
  const update = manifest.update;

  if (isPresentIssueValue(data["update-type"])) {
    if (data["update-type"] === "GitHub Releases" && isPresentIssueValue(data["github-repo"])) {
      manifest.update = { type: "github", repo: data["github-repo"] };
    } else if (data["update-type"] === "Custom URL" && isPresentIssueValue(data["custom-update-url"])) {
      manifest.update = { type: "custom", url: data["custom-update-url"] };
    }
    return;
  }

  // Update type not changing, but repo/url might be updated
  if (update.type === "github" && isPresentIssueValue(data["github-repo"])) {
    update.repo = data["github-repo"];
  }
  if (update.type === "custom" && isPresentIssueValue(data["custom-update-url"])) {
    update.url = data["custom-update-url"];
  }
}

async function main() {
  const manifestType = resolveManifestType(process.env.LISTING_TYPE);
  const issueJson = process.env.ISSUE_JSON;

  if (!issueJson) {
    console.error("ISSUE_JSON environment variable is required");
    process.exit(1);
  }

  const data = JSON.parse(issueJson) as Record<string, unknown>;
  const { id, dir } = resolveListingIdAndDir(manifestType, data);
  const manifestPath = resolve(REPO_ROOT, dir, id, "manifest.json");

  const manifest = JSON.parse(readFileSync(manifestPath, "utf-8")) as
    | ModManifest
    | MapManifest;

  applyCommonMetadataUpdates(manifest, data);

  if (manifestType === "mod") {
    applyModTagUpdates(manifest as ModManifest, data);
  }

  applyUpdateTypeChanges(manifest as ModManifest, data);

  if (manifestType === "map") {
    applyMapManifestUpdates(manifest as MapManifest, data);
    const mapManifest = manifest as MapManifest;
    const demandStats = await resolveAndExtractDemandStatsForMapSource(
      mapManifest.id,
      mapManifest.update,
      { token: process.env.GH_DOWNLOADS_TOKEN ?? process.env.GITHUB_TOKEN },
    );
    mapManifest.population = demandStats.residents_total;
    mapManifest.residents_total = demandStats.residents_total;
    mapManifest.points_count = demandStats.points_count;
    mapManifest.population_count = demandStats.population_count;
  }

  // Gallery images — resolve URLs via GitHub API (same as create-listing)
  const galleryUrls = parseGalleryImages(
    typeof data.gallery === "string" ? data.gallery : undefined,
  );
  if (galleryUrls.length > 0) {
    const galleryDir = resolve(REPO_ROOT, dir, id, "gallery");
    const resolvedUrls = await resolveGalleryUrls(galleryUrls);
    manifest.gallery = await downloadGalleryImages(resolvedUrls, galleryDir);
  }

  assertValidRegistryManifest(
    manifest,
    `Updated ${dir}/${id}/manifest.json`,
  );

  writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
  console.log(`Updated ${dir}/${id}/manifest.json`);
}

main();
