import test from "node:test";
import assert from "node:assert/strict";
import {
  aggregateZipDownloadCountsByTag,
  parseGitHubReleaseAssetDownloadUrl,
} from "../lib/downloads.js";

test("parseGitHubReleaseAssetDownloadUrl parses GitHub release asset URLs", () => {
  const parsed = parseGitHubReleaseAssetDownloadUrl(
    "https://github.com/OwnerName/RepoName/releases/download/v1.2.0/my-map.zip",
  );

  assert.deepEqual(parsed, {
    repo: "ownername/reponame",
    owner: "OwnerName",
    name: "RepoName",
    tag: "v1.2.0",
    assetName: "my-map.zip",
  });
});

test("parseGitHubReleaseAssetDownloadUrl rejects non-release URLs", () => {
  assert.equal(
    parseGitHubReleaseAssetDownloadUrl("https://github.com/owner/repo"),
    null,
  );
  assert.equal(
    parseGitHubReleaseAssetDownloadUrl("https://example.com/file.zip"),
    null,
  );
});

test("aggregateZipDownloadCountsByTag includes zip totals and all asset lookup counts", () => {
  const byTag = aggregateZipDownloadCountsByTag([
    {
      tagName: "v1.0.0",
      assets: [
        { name: "map.zip", downloadCount: 11 },
        { name: "manifest.json", downloadCount: 50 },
        { name: "extras.ZIP", downloadCount: 3 },
      ],
    },
  ]);

  const release = byTag.get("v1.0.0");
  assert.ok(release, "Expected v1.0.0 release data");
  assert.equal(release.zipTotal, 14);
  assert.equal(release.assets.get("map.zip"), 11);
  assert.equal(release.assets.get("manifest.json"), 50);
  assert.equal(release.assets.get("extras.ZIP"), 3);
});

