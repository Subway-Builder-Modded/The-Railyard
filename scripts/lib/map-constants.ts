export const VANILLA_CITY_CODES = [
  "NYC",
  "DAL",
  "CHI",
  "SFO",
  "WAS",
  "PHX",
  "HOU",
  "ATL",
  "MIA",
  "SEA",
  "PHL",
  "DEN",
  "DET",
  "SAN",
  "MSP",
  "BOS",
  "AUS",
  "PDX",
  "STL",
  "SLC",
  "IND",
  "CMH",
  "CLE",
  "CIN",
  "MKE",
  "BAL",
  "PIT",
  "CLT",
  "HNL",
  "LON",
  "BHM",
  "MAN",
  "LIV",
  "NCL",
] as const;

export const LOCATION_TAGS = [
  "caribbean",
  "central-america",
  "central-asia",
  "east-africa",
  "east-asia",
  "europe",
  "middle-east",
  "north-africa",
  "north-america",
  "oceania",
  "south-america",
  "south-asia",
  "southeast-asia",
  "southern-africa",
  "west-africa",
] as const;

export const SPECIAL_DEMAND_TAGS = [
  "airports",
  "entertainment",
  "ferries",
  "hospitals",
  "parks",
  "schools",
  "universities",
] as const;

export const SOURCE_QUALITY_VALUES = [
  "low-quality",
  "medium-quality",
  "high-quality",
] as const;

export const LEVEL_OF_DETAIL_VALUES = [
  "low-detail",
  "medium-detail",
  "high-detail",
] as const;

export const DEFAULT_MAP_DATA_SOURCE = "OSM" as const;
export const DEFAULT_SOURCE_QUALITY = "low-quality" as const;
export const DEFAULT_LEVEL_OF_DETAIL = "low-detail" as const;
export const MAX_OSM_SOURCE_QUALITY = "medium-quality" as const;

export const VANILLA_CITY_CODE_SET = new Set<string>(VANILLA_CITY_CODES);
export const LOCATION_TAG_SET = new Set<string>(LOCATION_TAGS);
export const SPECIAL_DEMAND_TAG_SET = new Set<string>(SPECIAL_DEMAND_TAGS);
export const SOURCE_QUALITY_SET = new Set<string>(SOURCE_QUALITY_VALUES);
export const LEVEL_OF_DETAIL_SET = new Set<string>(LEVEL_OF_DETAIL_VALUES);

export function isOsmDataSource(value: string): boolean {
  return /osm/i.test(value);
}
