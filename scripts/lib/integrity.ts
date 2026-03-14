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

function inspectMapZip(files: Set<string>, cityCode: string): ZipCompletenessResult {
  const requiredChecks: Record<string, boolean> = {};
  const matchedFiles: Record<string, string | null> = {};
  const errors: string[] = [];

  const configFile = firstMatch(files, ["config.json"]);
  requiredChecks.config_json = configFile !== null;
  matchedFiles.config_json = configFile;
  if (!configFile) {
    errors.push("missing top-level config.json");
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

  const pmtilesName = `${cityCode}.pmtiles`;
  const pmtiles = firstMatch(files, [pmtilesName]);
  requiredChecks.city_pmtiles = pmtiles !== null;
  matchedFiles.city_pmtiles = pmtiles;
  if (!pmtiles) {
    errors.push(`missing top-level ${pmtilesName}`);
  }

  return {
    isComplete: errors.length === 0,
    errors,
    requiredChecks,
    matchedFiles,
  };
}

function inspectModZip(files: Set<string>, releaseHasManifestAsset: boolean): ZipCompletenessResult {
  const requiredChecks: Record<string, boolean> = {};
  const matchedFiles: Record<string, string | null> = {};
  const errors: string[] = [];

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
      requiredChecks: {},
      matchedFiles: {},
    };
  }

  const topLevelFiles = listTopLevelFileNames(zip);
  if (listingType === "map") {
    const cityCode = options.cityCode;
    if (!cityCode) {
      return {
        isComplete: false,
        errors: ["missing city_code for map integrity validation"],
        requiredChecks: {},
        matchedFiles: {},
      };
    }
    return inspectMapZip(topLevelFiles, cityCode);
  }

  return inspectModZip(topLevelFiles, options.releaseHasManifestAsset === true);
}
