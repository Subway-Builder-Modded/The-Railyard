import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  parseGalleryImages,
  resolveGalleryUrls,
  downloadGalleryImages,
} from "./lib/gallery.js";
import {
  DEFAULT_LEVEL_OF_DETAIL,
  DEFAULT_MAP_DATA_SOURCE,
  DEFAULT_SOURCE_QUALITY,
  MAX_OSM_SOURCE_QUALITY,
  isOsmDataSource,
} from "./lib/map-constants.js";
import {
  type MapManifest,
  type ModManifest,
  resolveListingIdAndDir,
  resolveListingKind,
} from "./lib/manifests.js";
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

function isPresent(value: unknown): value is string {
  return typeof value === "string"
    && value !== ""
    && value !== "_No response_"
    && value !== "None"
    && value !== "No change";
}

function combineMapTags(location: string, specialDemand: string[]): string[] {
  return Array.from(new Set([location, ...specialDemand]));
}

function applyCommonMetadataUpdates(
  manifest: ModManifest,
  data: Record<string, unknown>,
): void {
  if (isPresent(data.name)) manifest.name = data.name;
  if (isPresent(data.description)) manifest.description = data.description;
  if (isPresent(data.source)) manifest.source = data.source;
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

  if (isPresent(data["update-type"])) {
    if (data["update-type"] === "GitHub Releases" && isPresent(data["github-repo"])) {
      manifest.update = { type: "github", repo: data["github-repo"] };
    } else if (data["update-type"] === "Custom URL" && isPresent(data["custom-update-url"])) {
      manifest.update = { type: "custom", url: data["custom-update-url"] };
    }
    return;
  }

  // Update type not changing, but repo/url might be updated
  if (update.type === "github" && isPresent(data["github-repo"])) {
    update.repo = data["github-repo"];
  }
  if (update.type === "custom" && isPresent(data["custom-update-url"])) {
    update.url = data["custom-update-url"];
  }
}

function applyMapManifestUpdates(
  manifest: MapManifest,
  data: Record<string, unknown>,
): void {
  const existingSpecialDemand = Array.isArray(manifest.special_demand)
    ? manifest.special_demand.filter((tag): tag is string => typeof tag === "string")
    : [];
  manifest.special_demand = existingSpecialDemand;

  if (isPresent(data["city-code"])) manifest.city_code = data["city-code"];
  if (isPresent(data.country)) manifest.country = data.country;
  if (isPresent(data.population)) manifest.population = parseInt(data.population, 10);

  if (isPresent(data.level_of_detail)) {
    manifest.level_of_detail = data.level_of_detail;
  } else if (!isPresent(manifest.level_of_detail)) {
    manifest.level_of_detail = DEFAULT_LEVEL_OF_DETAIL;
  }

  if (isPresent(data.source_quality)) {
    manifest.source_quality = data.source_quality;
  } else if (!isPresent(manifest.source_quality)) {
    manifest.source_quality = DEFAULT_SOURCE_QUALITY;
  }

  if (isPresent(data.data_source)) {
    manifest.data_source = data.data_source;
  } else if (!isPresent(manifest.data_source)) {
    manifest.data_source = DEFAULT_MAP_DATA_SOURCE;
  }

  if (isPresent(data.location)) {
    manifest.location = data.location;
  }
  if (
    data.special_demand !== undefined
    && data.special_demand !== "_No response_"
    && data.special_demand !== "None"
  ) {
    manifest.special_demand = parseCheckedBoxes(data.special_demand) ?? [];
  }

  if (
    isOsmDataSource(manifest.data_source)
    && manifest.source_quality === "high-quality"
  ) {
    manifest.source_quality = MAX_OSM_SOURCE_QUALITY;
  }

  if (isPresent(manifest.location)) {
    const specialDemand = (Array.isArray(manifest.special_demand)
      ? manifest.special_demand
      : []).filter(
        (tag: unknown): tag is string => typeof tag === "string",
      );
    manifest.special_demand = specialDemand;
    manifest.tags = combineMapTags(manifest.location, specialDemand);
  }
}

async function main() {
  const listingKind = resolveListingKind(process.env.LISTING_TYPE);
  const issueJson = process.env.ISSUE_JSON;

  if (!issueJson) {
    console.error("ISSUE_JSON environment variable is required");
    process.exit(1);
  }

  const data = JSON.parse(issueJson) as Record<string, unknown>;
  const { id, dir } = resolveListingIdAndDir(listingKind, data);
  const manifestPath = resolve(REPO_ROOT, dir, id, "manifest.json");

  const manifest = JSON.parse(readFileSync(manifestPath, "utf-8")) as
    | ModManifest
    | MapManifest;

  applyCommonMetadataUpdates(manifest, data);

  if (listingKind === "mod") {
    applyModTagUpdates(manifest as ModManifest, data);
  }

  applyUpdateTypeChanges(manifest as ModManifest, data);

  if (listingKind === "map") {
    applyMapManifestUpdates(manifest as MapManifest, data);
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

