import {
  DEFAULT_LEVEL_OF_DETAIL,
  DEFAULT_MAP_DATA_SOURCE,
  DEFAULT_SOURCE_QUALITY,
  MAX_OSM_SOURCE_QUALITY,
  isOsmDataSource,
} from "./map-constants.js";

const EMPTY_ISSUE_VALUES = new Set(["_No response_", "None", "No change"]);

export function getOptionalIssueValue(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  if (!trimmed || EMPTY_ISSUE_VALUES.has(trimmed)) return undefined;
  return trimmed;
}

export function getRequiredIssueValue(fieldName: string, value: unknown): string {
  const resolved = getOptionalIssueValue(value);
  if (!resolved) {
    throw new Error(`${fieldName} is required`);
  }
  return resolved;
}

export function getMapDataSource(value: unknown): string {
  return getOptionalIssueValue(value) ?? DEFAULT_MAP_DATA_SOURCE;
}

export function getMapSourceQuality(value: unknown): string {
  return getOptionalIssueValue(value) ?? DEFAULT_SOURCE_QUALITY;
}

export function getMapLevelOfDetail(value: unknown): string {
  return getOptionalIssueValue(value) ?? DEFAULT_LEVEL_OF_DETAIL;
}

export function normalizeSourceQualityForDataSource(
  dataSource: string,
  sourceQuality: string,
): string {
  if (isOsmDataSource(dataSource) && sourceQuality === "high-quality") {
    return MAX_OSM_SOURCE_QUALITY;
  }
  return sourceQuality;
}
