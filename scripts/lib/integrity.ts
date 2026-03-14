import JSZip from "jszip";
import type { ManifestType } from "./manifests.js";

export interface IntegritySource {
  update_type: "github" | "custom";
  repo?: string;
  tag?: string;
  asset_name?: string;
  download_url?: string;
}

export interface IntegrityVersionEntry {
  is_complete: boolean;
  errors: string[];
  required_checks: Record<string, boolean>;
  matched_files: Record<string, string | null>;
  source: IntegritySource;
  fingerprint: string;
  checked_at: string;
}

export interface ListingIntegrityEntry {
  has_complete_version: boolean;
  latest_semver_version: string | null;
  latest_semver_complete: boolean | null;
  complete_versions: string[];
  incomplete_versions: string[];
  versions: Record<string, IntegrityVersionEntry>;
}

export interface IntegrityOutput {
  schema_version: 1;
  generated_at: string;
  listings: Record<string, ListingIntegrityEntry>;
}

export interface IntegrityCacheEntry {
  fingerprint: string;
  last_checked_at: string;
  result: IntegrityVersionEntry;
}

export interface IntegrityCache {
  schema_version: 1;
  entries: Record<string, Record<string, IntegrityCacheEntry>>;
}

export interface ZipCompletenessResult {
  isComplete: boolean;
  errors: string[];
  warnings: string[];
  requiredChecks: Record<string, boolean>;
  matchedFiles: Record<string, string | null>;
}

interface InspectZipOptions {
  cityCode?: string;
  releaseHasManifestAsset?: boolean;
}

function listTopLevelFileNames(zip: JSZip): Set<string> {
  const names = new Set<string>();
  for (const entry of Object.values(zip.files)) {
    if (entry.dir) continue;
    if (entry.name.includes("/")) continue;
    names.add(entry.name);
  }
  return names;
}

function firstMatch(files: Set<string>, names: string[]): string | null {
  for (const name of names) {
    if (files.has(name)) {
      return name;
    }
  }
  return null;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function findConfigCode(value: unknown): string | null {
  const queue: unknown[] = [value];
  const visited = new Set<unknown>();

  while (queue.length > 0) {
    const current = queue.shift();
    if (!isObject(current) || visited.has(current)) continue;
    visited.add(current);

    const direct = current.code;
    if (typeof direct === "string" && direct.trim() !== "") {
      return direct.trim();
    }

    for (const nestedValue of Object.values(current)) {
      if (isObject(nestedValue)) {
        queue.push(nestedValue);
      }
    }
  }

  return null;
}

async function parseConfigCode(zip: JSZip): Promise<{
  code: string | null;
  parseError: string | null;
}> {
  const configEntry = zip.files["config.json"];
  if (!configEntry || configEntry.dir) {
    return { code: null, parseError: null };
  }

  let rawConfig: string;
  try {
    rawConfig = await configEntry.async("string");
  } catch {
    return { code: null, parseError: "failed to read top-level config.json" };
  }

  let parsedConfig: unknown;
  try {
    parsedConfig = JSON.parse(rawConfig);
  } catch {
    return { code: null, parseError: "top-level config.json is not valid JSON" };
  }

  return {
    code: findConfigCode(parsedConfig),
    parseError: null,
  };
}

function inspectMapZip(
  files: Set<string>,
  registryCityCode: string | null,
  configCode: string | null,
  parseError: string | null,
  configCodeMismatchWarning: string | null,
): ZipCompletenessResult {
  const requiredChecks: Record<string, boolean> = {};
  const matchedFiles: Record<string, string | null> = {};
  const errors: string[] = [];
  const warnings: string[] = [];

  const configFile = firstMatch(files, ["config.json"]);
  requiredChecks.config_json = configFile !== null;
  matchedFiles.config_json = configFile;
  if (!configFile) {
    errors.push("missing top-level config.json");
  } else if (parseError) {
    errors.push(parseError);
  }

  const demandData = firstMatch(files, ["demand_data.json", "demand_data.json.gz"]);
  requiredChecks.demand_data = demandData !== null;
  matchedFiles.demand_data = demandData;
  if (!demandData) {
    errors.push("missing top-level demand_data.json or demand_data.json.gz");
  }

  const buildingsIndex = firstMatch(files, ["buildings_index.json", "buildings_index.json.gz"]);
  requiredChecks.buildings_index = buildingsIndex !== null;
  matchedFiles.buildings_index = buildingsIndex;
  if (!buildingsIndex) {
    errors.push("missing top-level buildings_index.json or buildings_index.json.gz");
  }

  const roads = firstMatch(files, ["roads.geojson", "roads.geojson.gz"]);
  requiredChecks.roads_geojson = roads !== null;
  matchedFiles.roads_geojson = roads;
  if (!roads) {
    errors.push("missing top-level roads.geojson or roads.geojson.gz");
  }

  const runwaysTaxiways = firstMatch(files, ["runways_taxiways.geojson", "runways_taxiways.geojson.gz"]);
  requiredChecks.runways_taxiways_geojson = runwaysTaxiways !== null;
  matchedFiles.runways_taxiways_geojson = runwaysTaxiways;
  if (!runwaysTaxiways) {
    errors.push("missing top-level runways_taxiways.geojson or runways_taxiways.geojson.gz");
  }

  if (configCodeMismatchWarning) {
    warnings.push(configCodeMismatchWarning);
  }

  if (!configCode) {
    requiredChecks.city_pmtiles = false;
    matchedFiles.city_pmtiles = null;
    if (registryCityCode) {
      errors.push(
        `missing code in config.json for PMTiles validation (registry city_code '${registryCityCode}')`,
      );
    } else {
      errors.push("missing code in config.json for PMTiles validation");
    }
  } else {
    const pmtilesName = `${configCode}.pmtiles`;
    const pmtiles = firstMatch(files, [pmtilesName]);
    requiredChecks.city_pmtiles = pmtiles !== null;
    matchedFiles.city_pmtiles = pmtiles;
    if (!pmtiles) {
      if (registryCityCode) {
        errors.push(
          `missing top-level ${pmtilesName} (config code '${configCode}', registry city_code '${registryCityCode}')`,
        );
      } else {
        errors.push(`missing top-level ${pmtilesName} (config code '${configCode}')`);
      }
    }
  }

  return {
    isComplete: errors.length === 0,
    errors,
    warnings,
    requiredChecks,
    matchedFiles,
  };
}

function inspectModZip(files: Set<string>, releaseHasManifestAsset: boolean): ZipCompletenessResult {
  const requiredChecks: Record<string, boolean> = {};
  const matchedFiles: Record<string, string | null> = {};
  const errors: string[] = [];
  const warnings: string[] = [];

  requiredChecks.release_manifest_asset = releaseHasManifestAsset;
  matchedFiles.release_manifest_asset = releaseHasManifestAsset ? "manifest.json" : null;
  if (!releaseHasManifestAsset) {
    errors.push("release asset manifest.json is missing");
  }

  const manifestInZip = firstMatch(files, ["manifest.json"]);
  requiredChecks.zip_manifest_json = manifestInZip !== null;
  matchedFiles.zip_manifest_json = manifestInZip;
  if (!manifestInZip) {
    errors.push("missing top-level manifest.json in ZIP");
  }

  return {
    isComplete: errors.length === 0,
    errors,
    warnings,
    requiredChecks,
    matchedFiles,
  };
}

export async function inspectZipCompleteness(
  listingType: ManifestType,
  zipBuffer: Buffer,
  options: InspectZipOptions = {},
): Promise<ZipCompletenessResult> {
  let zip: JSZip;
  try {
    zip = await JSZip.loadAsync(zipBuffer);
  } catch {
    return {
      isComplete: false,
      errors: ["ZIP could not be opened"],
      warnings: [],
      requiredChecks: {},
      matchedFiles: {},
    };
  }

  const topLevelFiles = listTopLevelFileNames(zip);
  if (listingType === "map") {
    const registryCityCode = options.cityCode?.trim() || null;
    const configCodeResult = await parseConfigCode(zip);
    const configCode = configCodeResult.code;
    const mismatchWarning = (
      registryCityCode
      && configCode
      && registryCityCode !== configCode
    )
      ? (
        `registry city_code '${registryCityCode}' differs from config code '${configCode}' `
        + `(registry expects '${registryCityCode}.pmtiles', config expects '${configCode}.pmtiles')`
      )
      : null;

    return inspectMapZip(
      topLevelFiles,
      registryCityCode,
      configCode,
      configCodeResult.parseError,
      mismatchWarning,
    );
  }

  return inspectModZip(topLevelFiles, options.releaseHasManifestAsset === true);
}
