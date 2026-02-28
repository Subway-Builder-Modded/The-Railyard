import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { z } from "zod";
import { validateCustomUpdateUrl } from "./lib/custom-url.js";
import { validateGitHubRepo } from "./lib/github.js";

const REPO_ROOT = resolve(import.meta.dirname, "..");

const VANILLA_CITY_CODES = new Set([
  "NYC", "DAL", "CHI", "SFO", "WAS", "PHX", "HOU", "ATL", "MIA", "SEA",
  "PHL", "DEN", "DET", "SAN", "MSP", "BOS", "AUS", "PDX", "STL", "SLC",
  "IND", "CMH", "CLE", "CIN", "MKE", "BAL", "PIT", "CLT", "HNL",
  "LON", "BHM", "MAN", "LIV", "NCL",
]);

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
});

interface ValidationResult {
  success: boolean;
  errors: string[];
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
      const ghErrors = await validateGitHubRepo(parsed.data["github-repo"], parsed.data.source);
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

  if (VANILLA_CITY_CODES.has(parsed.data["city-code"])) {
    errors.push(`**city-code**: \`${parsed.data["city-code"]}\` clashes with a vanilla city code.`);
  }

  if (parsed.data["update-type"] === "GitHub Releases") {
    if (!parsed.data["github-repo"] || !/^[^/]+\/[^/]+$/.test(parsed.data["github-repo"])) {
      errors.push("**github-repo**: Must provide a valid `owner/repo` when using GitHub Releases.");
    } else {
      const ghErrors = await validateGitHubRepo(parsed.data["github-repo"], parsed.data.source);
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
    // Write error for the workflow to pick up
    const { writeFileSync } = await import("node:fs");
    writeFileSync(resolve(REPO_ROOT, "validation-error.md"), errorMessage);
    console.error(errorMessage);
    process.exit(1);
  }

  console.log("Validation passed.");
}

main();
