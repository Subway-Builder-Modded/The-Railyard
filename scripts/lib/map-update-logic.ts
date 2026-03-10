import type { MapManifest } from "./manifests.js";
import {
  DEFAULT_LEVEL_OF_DETAIL,
  DEFAULT_MAP_DATA_SOURCE,
  DEFAULT_SOURCE_QUALITY,
  LEVEL_OF_DETAIL_SET,
  LOCATION_TAG_SET,
  MAX_OSM_SOURCE_QUALITY,
  SOURCE_QUALITY_SET,
  SPECIAL_DEMAND_TAG_SET,
  isOsmDataSource,
} from "./map-constants.js";

function isPresent(value: unknown): value is string {
  return typeof value === "string"
    && value !== ""
    && value !== "_No response_"
    && value !== "None"
    && value !== "No change";
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

function combineMapTags(location: string, specialDemand: string[]): string[] {
  return Array.from(new Set([location, ...specialDemand]));
}

function requireManifestString(
  fieldName: string,
  value: unknown,
  errors: string[],
): string | null {
  if (typeof value !== "string" || value.trim() === "") {
    errors.push(`**manifest.${fieldName}**: Missing required map field.`);
    return null;
  }
  return value;
}

function requireManifestStringArray(
  fieldName: string,
  value: unknown,
  errors: string[],
): string[] | null {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    errors.push(`**manifest.${fieldName}**: Must be an array of strings.`);
    return null;
  }
  return value.filter((item): item is string => typeof item === "string");
}

export function applyMapManifestUpdates(
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
    manifest.special_demand = parseCheckedBoxes(data.special_demand);
  }

  // Cap OSM quality to be medium quality since high-quality OSM data is generally not available
  if (
    isOsmDataSource(manifest.data_source)
    && manifest.source_quality === "high-quality"
  ) {
    manifest.source_quality = MAX_OSM_SOURCE_QUALITY;
  }

  if (isPresent(manifest.location)) {
    const specialDemand = (Array.isArray(manifest.special_demand)
      ? manifest.special_demand
      : []).filter((tag: unknown): tag is string => typeof tag === "string");
    manifest.special_demand = specialDemand;
    manifest.tags = combineMapTags(manifest.location, specialDemand);
  }
}

export function validateMapUpdateFields(
  manifest: MapManifest,
  data: Record<string, unknown>,
  errors: string[],
): void {
  const currentDataSource = requireManifestString(
    "data_source",
    manifest.data_source,
    errors,
  );
  const currentSourceQuality = requireManifestString(
    "source_quality",
    manifest.source_quality,
    errors,
  );
  const currentLevelOfDetail = requireManifestString(
    "level_of_detail",
    manifest.level_of_detail,
    errors,
  );
  const currentLocation = requireManifestString(
    "location",
    manifest.location,
    errors,
  );
  const currentSpecialDemand = requireManifestStringArray(
    "special_demand",
    manifest.special_demand,
    errors,
  );

  if (
    currentDataSource === null
    || currentSourceQuality === null
    || currentLevelOfDetail === null
    || currentLocation === null
    || currentSpecialDemand === null
  ) {
    return;
  }

  const nextDataSource = isPresent(data.data_source) ? data.data_source : currentDataSource;
  const nextSourceQuality = isPresent(data.source_quality)
    ? data.source_quality
    : currentSourceQuality;
  const nextLevelOfDetail = isPresent(data.level_of_detail)
    ? data.level_of_detail
    : currentLevelOfDetail;
  const nextLocation = isPresent(data.location) ? data.location : currentLocation;
  const nextSpecialDemand =
    data.special_demand !== undefined && data.special_demand !== "_No response_" && data.special_demand !== "None"
      ? parseCheckedBoxes(data.special_demand)
      : currentSpecialDemand;

  if (!SOURCE_QUALITY_SET.has(nextSourceQuality)) {
    errors.push("**source_quality**: Must be one of `low-quality`, `medium-quality`, `high-quality`.");
  }
  if (!LEVEL_OF_DETAIL_SET.has(nextLevelOfDetail)) {
    errors.push("**level_of_detail**: Must be one of `low-detail`, `medium-detail`, `high-detail`.");
  }
  if (!LOCATION_TAG_SET.has(nextLocation)) {
    errors.push("**location**: Must be one of the supported location tags.");
  }

  const invalidSpecialDemand = nextSpecialDemand.filter((tag) => !SPECIAL_DEMAND_TAG_SET.has(tag));
  if (invalidSpecialDemand.length > 0) {
    errors.push(`**special_demand**: Invalid tag(s): ${invalidSpecialDemand.join(", ")}`);
  }

  if (isOsmDataSource(nextDataSource) && nextSourceQuality === "high-quality") {
    errors.push("**source_quality**: OSM-based data sources cannot be marked `high-quality`.");
  }
}
