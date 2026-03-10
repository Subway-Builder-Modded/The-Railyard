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
import { assertValidRegistryManifest } from "./lib/registry-manifest.js";

const REPO_ROOT = resolve(import.meta.dirname, "..");

interface ModManifest {
  schema_version: number;
  id: string;
  name: string;
  author: string;
  github_id: number;
  description: string;
  tags: string[];
  gallery: string[];
  source: string;
  update: { type: "github"; repo: string } | { type: "custom"; url: string };
}

interface MapManifest extends ModManifest {
  city_code: string;
  country: string;
  population: number;
  data_source: string;
  source_quality: string;
  level_of_detail: string;
  location: string;
  special_demand: string[];
}

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

function buildUpdate(data: Record<string, string>): ModManifest["update"] {
  if (data["update-type"] === "GitHub Releases") {
    return { type: "github", repo: data["github-repo"]! };
  }
  return { type: "custom", url: data["custom-update-url"]! };
}

function combineMapTags(location: string, specialDemand: string[]): string[] {
  return Array.from(new Set([location, ...specialDemand]));
}

async function main() {
  const type = process.env.LISTING_TYPE; // "mod" or "map"
  const issueJson = process.env.ISSUE_JSON;
  const issueAuthorId = process.env.ISSUE_AUTHOR_ID;
  const issueAuthorLogin = process.env.ISSUE_AUTHOR_LOGIN;

  if (!issueJson || !issueAuthorId || !issueAuthorLogin) {
    console.error("ISSUE_JSON, ISSUE_AUTHOR_ID, and ISSUE_AUTHOR_LOGIN are required");
    process.exit(1);
  }

  const data = JSON.parse(issueJson);
  const id = type === "map" ? data["map-id"] : data["mod-id"];
  const dir = type === "map" ? "maps" : "mods";
  const listingDir = resolve(REPO_ROOT, dir, id);
  const galleryDir = resolve(listingDir, "gallery");

  mkdirSync(galleryDir, { recursive: true });

  // Download gallery images — resolve markdown URLs to JWT-signed URLs
  // via the GitHub API HTML body (required for private repo attachments)
  const imageUrls = parseGalleryImages(data.gallery);
  const resolvedUrls = await resolveGalleryUrls(imageUrls);
  const galleryPaths = await downloadGalleryImages(resolvedUrls, galleryDir);

  const rawTags = parseTags(data.tags);
  const levelOfDetail = type === "map"
    ? getRequiredIssueValue("level_of_detail", data.level_of_detail)
    : "";
  let sourceQuality = type === "map"
    ? getRequiredIssueValue("source_quality", data.source_quality)
    : "";
  const dataSource = type === "map"
    ? getMapDataSource(data.data_source)
    : "";
  const location = type === "map" ? getRequiredIssueValue("location", data.location) : "";
  const specialDemand = type === "map" ? parseTags(data.special_demand) : [];
  const tags = type === "map"
    ? combineMapTags(location, specialDemand)
    : rawTags;
  if (type === "map") {
    sourceQuality = normalizeSourceQualityForDataSource(dataSource, sourceQuality);
  }

  const manifest: ModManifest | MapManifest = {
    schema_version: 1,
    id,
    name: data.name,
    author: issueAuthorLogin,
    github_id: parseInt(issueAuthorId, 10),
    description: data.description,
    tags,
    gallery: galleryPaths,
    source: data.source,
    update: buildUpdate(data),
    ...(type === "map"
      ? {
          city_code: data["city-code"],
          country: data.country,
          population: parseInt(data.population, 10),
          data_source: dataSource,
          source_quality: sourceQuality,
          level_of_detail: levelOfDetail,
          location,
          special_demand: specialDemand,
        }
      : {}),
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

