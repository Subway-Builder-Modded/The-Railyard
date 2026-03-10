import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import YAML from "yaml";
import {
  DEFAULT_MAP_DATA_SOURCE,
  LEVEL_OF_DETAIL_VALUES,
  LOCATION_TAGS,
  SOURCE_QUALITY_VALUES,
  SPECIAL_DEMAND_TAGS,
} from "../lib/map-constants.js";

type IssueTemplateField = {
  id?: string;
  type: string;
  attributes?: {
    options?: Array<{ label: string }> | string[];
    value?: string;
    placeholder?: string;
  };
  validations?: {
    required?: boolean;
  };
};

function getField(body: unknown[], id: string): IssueTemplateField {
  const field = body.find((item) => {
    if (typeof item !== "object" || item === null) return false;
    return (item as { id?: string }).id === id;
  });
  assert.ok(field, `Expected field '${id}' in template`);
  return field as IssueTemplateField;
}

test("update-map.yml enforces expected map metadata fields/options", () => {
  const scriptsRoot = resolve(import.meta.dirname, "..", "..");
  const updateTemplatePath = resolve(
    scriptsRoot,
    "..",
    ".github",
    "ISSUE_TEMPLATE",
    "update-map.yml",
  );
  const parsed = YAML.parse(readFileSync(updateTemplatePath, "utf-8")) as {
    body: unknown[];
  };

  assert.ok(Array.isArray(parsed.body), "Template body should be an array");

  const sourceQuality = getField(parsed.body, "source_quality");
  assert.equal(sourceQuality.type, "dropdown");
  assert.deepEqual(sourceQuality.attributes?.options, SOURCE_QUALITY_VALUES);
  assert.equal(sourceQuality.validations?.required, true);

  const levelOfDetail = getField(parsed.body, "level_of_detail");
  assert.equal(levelOfDetail.type, "dropdown");
  assert.deepEqual(levelOfDetail.attributes?.options, LEVEL_OF_DETAIL_VALUES);
  assert.equal(levelOfDetail.validations?.required, true);

  const location = getField(parsed.body, "location");
  assert.equal(location.type, "dropdown");
  assert.deepEqual(location.attributes?.options, LOCATION_TAGS);
  assert.equal(location.validations?.required, true);

  const specialDemand = getField(parsed.body, "special_demand");
  assert.equal(specialDemand.type, "checkboxes");
  const specialDemandLabels = specialDemand.attributes?.options?.map((entry) =>
    typeof entry === "string" ? entry : entry.label
  );
  assert.deepEqual(
    specialDemandLabels,
    SPECIAL_DEMAND_TAGS,
  );

  const dataSource = getField(parsed.body, "data_source");
  assert.equal(dataSource.type, "input");
  assert.equal(dataSource.attributes?.value, DEFAULT_MAP_DATA_SOURCE);

  const methodology = getField(parsed.body, "methodology");
  assert.equal(methodology.type, "input");
  assert.equal(methodology.validations?.required, true);
  assert.ok(
    typeof methodology.attributes?.placeholder === "string"
      && methodology.attributes.placeholder.length > 0,
    "Methodology field should provide a non-empty placeholder",
  );
});
