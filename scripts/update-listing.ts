import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  parseGalleryImages,
  resolveGalleryUrls,
  downloadGalleryImages,
} from "./lib/gallery.js";

const REPO_ROOT = resolve(import.meta.dirname, "..");

function parseCheckedBoxes(raw: string | undefined): string[] | null {
  if (!raw) return null;
  const checked = raw
    .split("\n")
    .filter((line) => line.startsWith("- [X]") || line.startsWith("- [x]"))
    .map((line) => line.replace(/^- \[[Xx]\]\s*/, "").trim())
    .filter(Boolean);
  // Return null if nothing was checked (user wants to keep current tags)
  return checked.length > 0 ? checked : null;
}

function isPresent(value: string | undefined): value is string {
  return !!value && value !== "_No response_" && value !== "None" && value !== "No change";
}

function isOsmDataSource(value: string): boolean {
  return /osm/i.test(value);
}

async function main() {
  const type = process.env.LISTING_TYPE; // "mod" or "map"
  const issueJson = process.env.ISSUE_JSON;

  if (!issueJson) {
    console.error("ISSUE_JSON environment variable is required");
    process.exit(1);
  }

  const data = JSON.parse(issueJson);
  const id = type === "map" ? data["map-id"] : data["mod-id"];
  const dir = type === "map" ? "maps" : "mods";
  const manifestPath = resolve(REPO_ROOT, dir, id, "manifest.json");

  const manifest = JSON.parse(readFileSync(manifestPath, "utf-8"));

  // Update only provided fields
  if (isPresent(data.name)) manifest.name = data.name;
  if (isPresent(data.description)) manifest.description = data.description;
  if (isPresent(data.source)) manifest.source = data.source;

  const newTags = parseCheckedBoxes(data.tags);
  if (newTags) manifest.tags = newTags;

  // Update type
  if (isPresent(data["update-type"])) {
    if (data["update-type"] === "GitHub Releases" && isPresent(data["github-repo"])) {
      manifest.update = { type: "github", repo: data["github-repo"] };
    } else if (data["update-type"] === "Custom URL" && isPresent(data["custom-update-url"])) {
      manifest.update = { type: "custom", url: data["custom-update-url"] };
    }
  } else {
    // Update type not changing, but repo/url might be updated
    if (manifest.update.type === "github" && isPresent(data["github-repo"])) {
      manifest.update.repo = data["github-repo"];
    }
    if (manifest.update.type === "custom" && isPresent(data["custom-update-url"])) {
      manifest.update.url = data["custom-update-url"];
    }
  }

  // Map-specific fields
  if (type === "map") {
    if (isPresent(data["city-code"])) manifest.city_code = data["city-code"];
    if (isPresent(data.country)) manifest.country = data.country;
    if (isPresent(data.population)) manifest.population = parseInt(data.population, 10);

    if (isPresent(data.level_of_detail)) {
      manifest.level_of_detail = data.level_of_detail;
    } else if (!isPresent(manifest.level_of_detail)) {
      manifest.level_of_detail = "low-detail";
    }

    if (isPresent(data.source_quality)) {
      manifest.source_quality = data.source_quality;
    } else if (!isPresent(manifest.source_quality)) {
      manifest.source_quality = "low-quality";
    }

    if (isPresent(data.data_source)) {
      manifest.data_source = data.data_source;
    } else if (!isPresent(manifest.data_source)) {
      manifest.data_source = "OSM";
    }

    if (isPresent(manifest.data_source) && isOsmDataSource(manifest.data_source) && manifest.source_quality === "high-quality") {
      manifest.source_quality = "medium-quality";
    }
  }

  // Gallery images — resolve URLs via GitHub API (same as create-listing)
  const galleryUrls = parseGalleryImages(data.gallery);
  if (galleryUrls.length > 0) {
    const galleryDir = resolve(REPO_ROOT, dir, id, "gallery");
    const resolvedUrls = await resolveGalleryUrls(galleryUrls);
    manifest.gallery = await downloadGalleryImages(resolvedUrls, galleryDir);
  }

  writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
  console.log(`Updated ${dir}/${id}/manifest.json`);
}

main();
