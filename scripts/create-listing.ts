import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  parseGalleryImages,
  resolveGalleryUrls,
  downloadGalleryImages,
} from "./lib/gallery.js";
import {
  getMapDataSource,
  getRequiredIssueValue,
  normalizeSourceQualityForDataSource,
} from "./lib/map-field-utils.js";
import {
  type MapManifest,
  type ModManifest,
  resolveListingIdAndDir,
  resolveManifestType,
} from "./lib/manifests.js";
import { assertValidRegistryManifest } from "./lib/registry-manifest.js";

const REPO_ROOT = resolve(import.meta.dirname, "..");

function parseTags(raw: unknown): string[] {
  if (!raw) return [];
  // Issue parser may return an array of strings or comma-separated string
  if (Array.isArray(raw)) return raw.map((t) => String(t).trim()).filter(Boolean);
  if (typeof raw !== "string") return [];
  // Handle checkbox markdown format: "- [X] tag\n- [x] tag"
  if (raw.includes("- [")) {
    return raw
      .split("\n")
      .filter((line) => line.startsWith("- [X]") || line.startsWith("- [x]"))
      .map((line) => line.replace(/^- \[[Xx]\]\s*/, "").trim())
      .filter(Boolean);
  }
  // Handle comma-separated: "tag1, tag2, tag3"
  return raw.split(",").map((t) => t.trim()).filter(Boolean);
}

function buildUpdate(data: Record<string, unknown>): ModManifest["update"] {
  if (data["update-type"] === "GitHub Releases") {
    return { type: "github", repo: String(data["github-repo"]) };
  }
  return { type: "custom", url: String(data["custom-update-url"]) };
}

function combineMapTags(location: string, specialDemand: string[]): string[] {
  return Array.from(new Set([location, ...specialDemand]));
}

function buildMapManifestData(data: Record<string, unknown>): {
  tags: string[];
  mapFields: Omit<MapManifest, keyof ModManifest>;
} {
  const levelOfDetail = getRequiredIssueValue(
    "level_of_detail",
    data.level_of_detail,
  );
  const dataSource = getMapDataSource(data.data_source);
  const location = getRequiredIssueValue("location", data.location);
  const specialDemand = parseTags(data.special_demand);
  const sourceQuality = normalizeSourceQualityForDataSource(
    dataSource,
    getRequiredIssueValue("source_quality", data.source_quality),
  );

  return {
    tags: combineMapTags(location, specialDemand),
    mapFields: {
      city_code: String(data["city-code"]),
      country: String(data.country),
      population: parseInt(String(data.population), 10),
      data_source: dataSource,
      source_quality: sourceQuality,
      level_of_detail: levelOfDetail,
      location,
      special_demand: specialDemand,
    },
  };
}

async function main() {
  const manifestType = resolveManifestType(process.env.LISTING_TYPE);
  const issueJson = process.env.ISSUE_JSON;
  const issueAuthorId = process.env.ISSUE_AUTHOR_ID;
  const issueAuthorLogin = process.env.ISSUE_AUTHOR_LOGIN;

  if (!issueJson || !issueAuthorId || !issueAuthorLogin) {
    console.error("ISSUE_JSON, ISSUE_AUTHOR_ID, and ISSUE_AUTHOR_LOGIN are required");
    process.exit(1);
  }

  const data = JSON.parse(issueJson) as Record<string, unknown>;
  const { id, dir } = resolveListingIdAndDir(manifestType, data);
  const listingDir = resolve(REPO_ROOT, dir, id);
  const galleryDir = resolve(listingDir, "gallery");

  mkdirSync(galleryDir, { recursive: true });

  // Download gallery images — resolve markdown URLs to JWT-signed URLs
  // via the GitHub API HTML body (required for private repo attachments)
  const imageUrls = parseGalleryImages(
    typeof data.gallery === "string" ? data.gallery : undefined,
  );
  const resolvedUrls = await resolveGalleryUrls(imageUrls);
  const galleryPaths = await downloadGalleryImages(resolvedUrls, galleryDir);

  const rawTags = parseTags(data.tags);
  const mapData = manifestType === "map" ? buildMapManifestData(data) : undefined;
  const tags = mapData ? mapData.tags : rawTags;

  const manifest: ModManifest | MapManifest = {
    schema_version: 1,
    id,
    name: String(data.name),
    author: issueAuthorLogin,
    github_id: parseInt(issueAuthorId, 10),
    description: String(data.description),
    tags,
    gallery: galleryPaths,
    source: String(data.source),
    update: buildUpdate(data),
    ...(mapData ? mapData.mapFields : {}),
  };

  assertValidRegistryManifest(
    manifest,
    `Generated ${dir}/${id}/manifest.json`,
  );

  writeFileSync(
    resolve(listingDir, "manifest.json"),
    JSON.stringify(manifest, null, 2) + "\n"
  );

  console.log(`Created ${dir}/${id}/manifest.json`);
}

main();

