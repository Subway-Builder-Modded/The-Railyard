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

async function fetchWithAuth(url: string, token: string | undefined): Promise<Response> {
  const isGitHub = url.includes("github.com") || url.includes("githubusercontent.com");
  const headers: Record<string, string> = {};
  if (token && isGitHub) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  // Use manual redirects for GitHub URLs — the Authorization header gets
  // stripped on cross-origin redirects (github.com → githubusercontent.com),
  // but the redirect URL contains a JWT in the query string so we can
  // follow it without auth.
  const response = await fetch(url, {
    headers,
    redirect: isGitHub ? "manual" : "follow",
  });

  if (response.status >= 300 && response.status < 400) {
    const location = response.headers.get("location");
    if (location) {
      return fetch(location);
    }
  }

  return response;
}

async function downloadGalleryImages(urls: string[], galleryDir: string): Promise<string[]> {
  const paths: string[] = [];
  const token = process.env.GITHUB_TOKEN;

  for (let i = 0; i < urls.length; i++) {
    const url = urls[i];
    const ext = url.match(/\.(png|jpg|jpeg|gif|webp|svg)/i)?.[1] ?? "png";
    const filename = `screenshot${i + 1}.${ext}`;
    const filePath = resolve(galleryDir, filename);

    try {
      const response = await fetchWithAuth(url, token);
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

  const tags = parseTags(data.tags);

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
