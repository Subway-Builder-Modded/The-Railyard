import test from "node:test";
import assert from "node:assert/strict";
import {
  DEFAULT_LEVEL_OF_DETAIL,
  DEFAULT_MAP_DATA_SOURCE,
  DEFAULT_SOURCE_QUALITY,
  LEVEL_OF_DETAIL_VALUES,
  MAX_OSM_SOURCE_QUALITY,
  SOURCE_QUALITY_VALUES,
} from "../lib/map-constants.js";
import {
  getMapDataSource,
  getMapLevelOfDetail,
  getMapSourceQuality,
  getOptionalIssueValue,
  normalizeSourceQualityForDataSource,
} from "../lib/map-field-utils.js";

test("getOptionalIssueValue ignores empty/sentinel values", () => {
  assert.equal(getOptionalIssueValue(undefined), undefined);
  assert.equal(getOptionalIssueValue(""), undefined);
  assert.equal(getOptionalIssueValue("   "), undefined);
  assert.equal(getOptionalIssueValue("_No response_"), undefined);
  assert.equal(getOptionalIssueValue("None"), undefined);
  assert.equal(getOptionalIssueValue("No change"), undefined);
});

test("getOptionalIssueValue trims valid values", () => {
  assert.equal(getOptionalIssueValue("  LODES "), "LODES");
});

test("default map field values are applied when empty", () => {
  assert.equal(getMapDataSource(undefined), DEFAULT_MAP_DATA_SOURCE);
  assert.equal(getMapDataSource("_No response_"), DEFAULT_MAP_DATA_SOURCE);
  assert.equal(getMapSourceQuality(""), DEFAULT_SOURCE_QUALITY);
  assert.equal(getMapLevelOfDetail("None"), DEFAULT_LEVEL_OF_DETAIL);
});

test("OSM source cannot remain high-quality", () => {
  assert.equal(
    normalizeSourceQualityForDataSource("OSM", "high-quality"),
    MAX_OSM_SOURCE_QUALITY,
  );
  assert.equal(
    normalizeSourceQualityForDataSource(
      "INSEE",
      "high-quality",
    ),
    "high-quality",
  );
});

test("defaults exist in allowed constant lists", () => {
  assert.equal(SOURCE_QUALITY_VALUES.includes(DEFAULT_SOURCE_QUALITY), true);
  assert.equal(LEVEL_OF_DETAIL_VALUES.includes(DEFAULT_LEVEL_OF_DETAIL), true);
});
