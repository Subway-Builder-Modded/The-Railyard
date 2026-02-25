import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

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
}

function parseCheckedBoxes(raw: string | undefined): string[] {
  if (!raw) return [];
  return raw
    .split("\n")
    .filter((line) => line.startsWith("- [X]") || line.startsWith("- [x]"))
    .map((line) => line.replace(/^- \[[Xx]\]\s*/, "").trim())
    .filter(Boolean);
}

function parseGalleryImages(raw: string | undefined): string[] {
  if (!raw || raw === "_No response_") return [];
  // Extract image URLs from: ![alt](url), <img src="url">, or plain URLs
  const urls: string[] = [];
  for (const line of raw.split("\n")) {
    const mdMatch = line.match(/!\[.*?\]\((.*?)\)/);
    if (mdMatch) {
      urls.push(mdMatch[1]);
      continue;
    }
    const imgMatch = line.match(/<img\s[^>]*src=["']([^"']+)["'][^>]*>/i);
    if (imgMatch) {
      urls.push(imgMatch[1]);
      continue;
    }
    const trimmed = line.trim();
    if (trimmed.startsWith("http")) {
      urls.push(trimmed);
    }
  }
  return urls;
}

function buildUpdate(data: Record<string, string>): ModManifest["update"] {
  if (data["update-type"] === "GitHub Releases") {
    return { type: "github", repo: data["github-repo"]! };
  }
  return { type: "custom", url: data["custom-update-url"]! };
}

async function downloadGalleryImages(urls: string[], galleryDir: string): Promise<string[]> {
  const paths: string[] = [];
  for (let i = 0; i < urls.length; i++) {
    const url = urls[i];
    const ext = url.match(/\.(png|jpg|jpeg|gif|webp|svg)/i)?.[1] ?? "png";
    const filename = `screenshot${i + 1}.${ext}`;
    const filePath = resolve(galleryDir, filename);

    try {
      const response = await fetch(url);
      if (!response.ok) {
        console.warn(`Failed to download ${url}: ${response.status}`);
        continue;
      }
      const buffer = Buffer.from(await response.arrayBuffer());
      writeFileSync(filePath, buffer);
      paths.push(`gallery/${filename}`);
    } catch (err) {
      console.warn(`Failed to download ${url}: ${err}`);
    }
  }
  return paths;
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

  // Download gallery images
  const imageUrls = parseGalleryImages(data.gallery);
  const galleryPaths = await downloadGalleryImages(imageUrls, galleryDir);

  const tags = parseCheckedBoxes(data.tags);

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
        }
      : {}),
  };

  writeFileSync(
    resolve(listingDir, "manifest.json"),
    JSON.stringify(manifest, null, 2) + "\n"
  );

  console.log(`Created ${dir}/${id}/manifest.json`);
}

main();
