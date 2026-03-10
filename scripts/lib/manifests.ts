export type ManifestType = "map" | "mod";
export type ManifestDirectory = "maps" | "mods";

export type UpdateType =
  | { type: "github"; repo: string }
  | { type: "custom"; url: string };

export interface ModManifest {
  schema_version: number;
  id: string;
  name: string;
  author: string;
  github_id: number;
  description: string;
  tags: string[];
  gallery: string[];
  source: string;
  update: UpdateType;
}

export interface MapManifest extends ModManifest {
  city_code: string;
  country: string;
  population: number;
  data_source: string;
  source_quality: string;
  level_of_detail: string;
  location: string;
  special_demand: string[];
}

export type ListingManifest = ModManifest | MapManifest;

export function resolveManifestType(value: string | undefined): ManifestType {
  return value === "map" ? "map" : "mod";
}

export function resolveListingIdAndDir(
  kind: ManifestType,
  data: Record<string, unknown>,
): { id: string; dir: ManifestDirectory } {
  if (kind === "map") {
    return { id: String(data["map-id"]), dir: "maps" };
  }
  return { id: String(data["mod-id"]), dir: "mods" };
}
