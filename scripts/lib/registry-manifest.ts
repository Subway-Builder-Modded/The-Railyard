import Ajv, { type ErrorObject } from "ajv";
import addFormats from "ajv-formats";
import {
  LEVEL_OF_DETAIL_VALUES,
  LOCATION_TAGS,
  SOURCE_QUALITY_VALUES,
  SPECIAL_DEMAND_TAGS,
} from "./map-constants.js";

const BASE_MANIFEST_PROPERTIES = {
  schema_version: { type: "integer", const: 1 },
  id: { type: "string", pattern: "^[a-z0-9]+(-[a-z0-9]+)*$" },
  name: { type: "string", minLength: 1 },
  author: { type: "string", minLength: 1 },
  github_id: { type: "integer", minimum: 1 },
  description: { type: "string", minLength: 1 },
  tags: {
    type: "array",
    items: { type: "string", minLength: 1 },
    uniqueItems: true,
  },
  gallery: {
    type: "array",
    items: { type: "string", minLength: 1 },
  },
  source: { type: "string", format: "uri" },
  update: {
    oneOf: [
      {
        type: "object",
        required: ["type", "repo"],
        properties: {
          type: { const: "github" },
          repo: { type: "string", pattern: "^[^/]+/[^/]+$" },
        },
        additionalProperties: false,
      },
      {
        type: "object",
        required: ["type", "url"],
        properties: {
          type: { const: "custom" },
          url: { type: "string", format: "uri" },
        },
        additionalProperties: false,
      },
    ],
  },
} as const;

const BASE_REQUIRED_FIELDS = [
  "schema_version",
  "id",
  "name",
  "author",
  "github_id",
  "description",
  "tags",
  "gallery",
  "source",
  "update",
] as const;

const MAP_MANIFEST_PROPERTIES = {
  ...BASE_MANIFEST_PROPERTIES,
  gallery: {
    type: "array",
    items: { type: "string", minLength: 1 },
    minItems: 1,
  },
  city_code: { type: "string", pattern: "^[A-Z0-9]{2,4}$" },
  country: { type: "string", pattern: "^[A-Z]{2}$" },
  population: { type: "integer", minimum: 0 },
  residents_total: { type: "integer", minimum: 0 },
  points_count: { type: "integer", minimum: 0 },
  population_count: { type: "integer", minimum: 0 },
  data_source: { type: "string", minLength: 1 },
  source_quality: { enum: SOURCE_QUALITY_VALUES },
  level_of_detail: { enum: LEVEL_OF_DETAIL_VALUES },
  location: { enum: LOCATION_TAGS },
  special_demand: {
    type: "array",
    items: { enum: SPECIAL_DEMAND_TAGS },
    uniqueItems: true,
  },
} as const;

const MAP_REQUIRED_FIELDS = [
  ...BASE_REQUIRED_FIELDS,
  "city_code",
  "country",
  "population",
  "residents_total",
  "points_count",
  "population_count",
  "data_source",
  "source_quality",
  "level_of_detail",
  "location",
  "special_demand",
] as const;

const REGISTRY_MANIFEST_SCHEMA = {
  oneOf: [
    {
      type: "object",
      properties: BASE_MANIFEST_PROPERTIES,
      required: BASE_REQUIRED_FIELDS,
      additionalProperties: false,
    },
    {
      type: "object",
      properties: MAP_MANIFEST_PROPERTIES,
      required: MAP_REQUIRED_FIELDS,
      additionalProperties: false,
    },
  ],
} as const;

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const validateManifest = ajv.compile(REGISTRY_MANIFEST_SCHEMA);

function formatValidationErrors(errors: readonly ErrorObject[] | null | undefined): string {
  if (!errors || errors.length === 0) {
    return "unknown schema validation error";
  }
  return errors
    .map((error) => {
      const path = error.instancePath || "/";
      return `${path} ${error.message ?? "validation error"}`;
    })
    .join("; ");
}

export function assertValidRegistryManifest(
  manifest: unknown,
  label: string,
): void {
  const valid = validateManifest(manifest);
  if (valid) return;
  const details = formatValidationErrors(validateManifest.errors);
  throw new Error(`${label} failed schema validation: ${details}`);
}

