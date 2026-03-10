import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { z } from "zod";
import { validateCustomUpdateUrl } from "./lib/custom-url.js";
import { validateGitHubRepo } from "./lib/github.js";
import { parseGalleryImages } from "./lib/gallery.js";
import {
  DEFAULT_MAP_DATA_SOURCE,
  LEVEL_OF_DETAIL_VALUES,
  LOCATION_TAGS,
  SOURCE_QUALITY_VALUES,
  SPECIAL_DEMAND_TAG_SET,
  VANILLA_CITY_CODE_SET,
  isOsmDataSource,
} from "./lib/map-constants.js";


const REPO_ROOT = resolve(import.meta.dirname, "..");

const PublishModInput = z.object({
  "mod-id": z.string().regex(/^[a-z0-9]+(-[a-z0-9]+)*$/, "Mod ID must be kebab-case (lowercase letters, numbers, hyphens)"),
  name: z.string().min(1, "Display name is required"),
  description: z.string().min(1, "Description is required"),
  source: z.string().url("Source must be a valid URL"),
  "update-type": z.enum(["GitHub Releases", "Custom URL"]),
  "github-repo": z.string().optional(),
  "custom-update-url": z.string().optional(),
});

const PublishMapInput = PublishModInput.omit({ "mod-id": true }).extend({
  "map-id": z.string().regex(/^[a-z0-9]+(-[a-z0-9]+)*$/, "Map ID must be kebab-case (lowercase letters, numbers, hyphens)"),
  "city-code": z.string().min(2).max(4).regex(/^[A-Z0-9]+$/, "City code must be 2-4 uppercase letters/numbers"),
  country: z.string().length(2).regex(/^[A-Z]{2}$/, "Country must be a 2-letter ISO 3166-1 alpha-2 code"),
  population: z.string().regex(/^\d+$/, "Population must be a number"),
  gallery: z.string().min(1, "At least one gallery image is required"),
  data_source: z.string().optional(),
  source_quality: z.enum(SOURCE_QUALITY_VALUES),
  level_of_detail: z.enum(LEVEL_OF_DETAIL_VALUES),
  location: z.enum(LOCATION_TAGS),
  special_demand: z.union([z.string(), z.array(z.string())]).optional(),
});

interface ValidationResult {
  success: boolean;
  errors: string[];
}

function isPresent(value: string | undefined): value is string {
  return !!value && value !== "_No response_" && value !== "None" && value !== "No change";
}

function parseCheckedBoxes(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw.map((tag) => String(tag).trim()).filter(Boolean);
  }
  if (typeof raw !== "string" || !raw || raw === "_No response_") return [];
  return raw
    .split("\n")
    .filter((line) => line.startsWith("- [X]") || line.startsWith("- [x]"))
    .map((line) => line.replace(/^- \[[Xx]\]\s*/, "").trim())
    .filter(Boolean);
}

async function validateMod(data: Record<string, string>): Promise<ValidationResult> {
  const errors: string[] = [];

  const parsed = PublishModInput.safeParse(data);
  if (!parsed.success) {
    for (const issue of parsed.error.issues) {
      errors.push(`**${issue.path.join(".")}**: ${issue.message}`);
    }
    return { success: false, errors };
  }

  const id = parsed.data["mod-id"];
  const modDir = resolve(REPO_ROOT, "mods", id);
  if (existsSync(modDir)) {
    errors.push(`**mod-id**: A mod with ID \`${id}\` already exists.`);
  }

  if (parsed.data["update-type"] === "GitHub Releases") {
    if (!parsed.data["github-repo"] || !/^[^/]+\/[^/]+$/.test(parsed.data["github-repo"])) {
      errors.push("**github-repo**: Must provide a valid `owner/repo` when using GitHub Releases.");
    } else {
      const ghErrors = await validateGitHubRepo(parsed.data["github-repo"], parsed.data.source, "mod", id);
      errors.push(...ghErrors);
    }
  } else {
    if (!parsed.data["custom-update-url"]) {
      errors.push("**custom-update-url**: Must provide a URL when using Custom URL.");
    } else {
      try {
        new URL(parsed.data["custom-update-url"]);
      } catch {
        errors.push("**custom-update-url**: Must be a valid URL.");
      }
      if (!errors.some((e) => e.includes("custom-update-url"))) {
        const urlErrors = await validateCustomUpdateUrl(parsed.data["custom-update-url"], "mod", id);
        errors.push(...urlErrors);
      }
    }
  }

  return { success: errors.length === 0, errors };
}

async function validateMap(data: Record<string, string>): Promise<ValidationResult> {
  const errors: string[] = [];

  const parsed = PublishMapInput.safeParse(data);
  if (!parsed.success) {
    for (const issue of parsed.error.issues) {
      errors.push(`**${issue.path.join(".")}**: ${issue.message}`);
    }
    return { success: false, errors };
  }

  const id = parsed.data["map-id"];
  const mapDir = resolve(REPO_ROOT, "maps", id);
  if (existsSync(mapDir)) {
    errors.push(`**map-id**: A map with ID \`${id}\` already exists.`);
  }

  if (VANILLA_CITY_CODE_SET.has(parsed.data["city-code"])) {
    errors.push(`**city-code**: \`${parsed.data["city-code"]}\` clashes with a vanilla city code.`);
  }

  if (parseGalleryImages(data.gallery).length === 0) {
    errors.push("**gallery**: At least one gallery image is required.");
  }
  const specialDemand = parseCheckedBoxes(data.special_demand);
  const invalidSpecialDemand = specialDemand.filter((tag) => !SPECIAL_DEMAND_TAG_SET.has(tag));
  if (invalidSpecialDemand.length > 0) {
    errors.push(`**special_demand**: Invalid tag(s): ${invalidSpecialDemand.join(", ")}`);
  }

  const dataSource = isPresent(parsed.data.data_source) ? parsed.data.data_source : DEFAULT_MAP_DATA_SOURCE;
  if (isOsmDataSource(dataSource) && parsed.data.source_quality === "high-quality") {
    errors.push("**source_quality**: OSM-based data sources cannot be marked `high-quality`.");
  }

  if (parsed.data["update-type"] === "GitHub Releases") {
    if (!parsed.data["github-repo"] || !/^[^/]+\/[^/]+$/.test(parsed.data["github-repo"])) {
      errors.push("**github-repo**: Must provide a valid `owner/repo` when using GitHub Releases.");
    } else {
      const ghErrors = await validateGitHubRepo(parsed.data["github-repo"], parsed.data.source, "map");
      errors.push(...ghErrors);
    }
  } else {
    if (!parsed.data["custom-update-url"]) {
      errors.push("**custom-update-url**: Must provide a URL when using Custom URL.");
    } else {
      try {
        new URL(parsed.data["custom-update-url"]);
      } catch {
        errors.push("**custom-update-url**: Must be a valid URL.");
      }
      if (!errors.some((e) => e.includes("custom-update-url"))) {
        const urlErrors = await validateCustomUpdateUrl(parsed.data["custom-update-url"]);
        errors.push(...urlErrors);
      }
    }
  }

  return { success: errors.length === 0, errors };
}

async function main() {
  const type = process.env.LISTING_TYPE;
  const issueJson = process.env.ISSUE_JSON;

  if (!issueJson) {
    console.error("ISSUE_JSON environment variable is required");
    process.exit(1);
  }

  const data = JSON.parse(issueJson);
  const result = type === "map" ? await validateMap(data) : await validateMod(data);

  if (!result.success) {
    const errorMessage = [
      "Validation failed with the following errors:\n",
      ...result.errors.map((e) => `- ${e}`),
    ].join("\n");

    // Write error for the workflow to pick up
    const { writeFileSync } = await import("node:fs");
    writeFileSync(resolve(REPO_ROOT, "validation-error.md"), errorMessage);
    console.error(errorMessage);
    process.exit(1);
  }

  console.log("Validation passed.");
}

main();

