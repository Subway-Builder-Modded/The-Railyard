import type { MapManifest } from "./manifests.js";
import * as D from "./download-definitions.js";
import { createGraphqlUsageState, fetchRepoReleaseIndexes, isSupportedReleaseTag, graphqlUsageSnapshot } from "./release-resolution.js";
import {
  type ListingContext,
  emptyIntegrity,
  fetchCustomVersions,
  getDirectoryForType,
  getIndexIds,
  getManifest,
  loadIntegrityCache,
  loadIntegritySnapshot,
  sortObjectByKeys,
  warnListing,
} from "./downloads-support.js";

export async function generateDownloadsDataDownloadOnly(
  options: D.GenerateDownloadsOptions,
): Promise<D.GenerateDownloadsResult> {
  const repoRoot = options.repoRoot;
  const listingType = options.listingType;
  const fetchImpl = options.fetchImpl ?? fetch;
  const token = options.token;
  const warnings: string[] = [];
  const dir = getDirectoryForType(listingType);
  const ids = getIndexIds(repoRoot, dir);
  const nowIso = new Date().toISOString();

  const downloadsByListing: D.DownloadsByListing = {};
  const listingContexts = new Map<string, ListingContext>();
  const repoSet = new Set<string>();

  for (const id of ids) {
    downloadsByListing[id] = {};
    let manifest;
    try {
      manifest = getManifest(repoRoot, dir, id);
    } catch (error) {
      warnListing(warnings, id, `failed to read manifest (${(error as Error).message})`);
      continue;
    }

    if (manifest.update.type === "github") {
      const repo = manifest.update.repo.toLowerCase();
      repoSet.add(repo);
      listingContexts.set(id, {
        id,
        listingType,
        cityCode: listingType === "map" ? (manifest as MapManifest).city_code : undefined,
        update: { type: "github", repo },
      });
      continue;
    }

    const customVersions = await fetchCustomVersions(id, manifest.update.url, fetchImpl, warnings);
    for (const version of customVersions) {
      if (version.parsed) {
        repoSet.add(version.parsed.repo);
      }
    }
    listingContexts.set(id, {
      id,
      listingType,
      cityCode: listingType === "map" ? (manifest as MapManifest).city_code : undefined,
      update: {
        type: "custom",
        url: manifest.update.url,
        versions: customVersions,
      },
    });
  }

  const usageState = createGraphqlUsageState();
  const { repoIndexes } = await fetchRepoReleaseIndexes(repoSet, {
    fetchImpl,
    token,
    warnings,
    usageState,
  });

  let versionsChecked = 0;

  for (const id of [...ids].sort()) {
    console.log(`[downloads] heartbeat:listing mode=download-only listing=${id}`);
    const context = listingContexts.get(id);
    if (!context) continue;

    if (context.update.type === "github") {
      const repoIndex = repoIndexes.get(context.update.repo);
      if (!repoIndex) {
        warnListing(warnings, id, "skipped all github-release versions (repo unavailable)");
        continue;
      }

      for (const tag of [...repoIndex.byTag.keys()].sort()) {
        const releaseData = repoIndex.byTag.get(tag);
        if (!releaseData) continue;
        if (!isSupportedReleaseTag(tag)) continue;
        const hasZipAsset = Array.from(releaseData.assets.keys())
          .some((assetName) => assetName.toLowerCase().endsWith(".zip"));
        if (!hasZipAsset) continue;

        versionsChecked += 1;
        downloadsByListing[id][tag] = releaseData.zipTotal;
      }
      continue;
    }

    for (const candidate of context.update.versions) {
      if (!candidate.semver) continue;
      versionsChecked += 1;

      if (!candidate.parsed) {
        warnListing(
          warnings,
          id,
          "skipped non-GitHub release download URL",
          candidate.version,
        );
        continue;
      }

      const repoIndex = repoIndexes.get(candidate.parsed.repo);
      if (!repoIndex) {
        warnListing(warnings, id, "skipped (repo unavailable)", candidate.version);
        continue;
      }
      const release = repoIndex.byTag.get(candidate.parsed.tag);
      if (!release) {
        warnListing(
          warnings,
          id,
          `skipped (tag '${candidate.parsed.tag}' not found)`,
          candidate.version,
        );
        continue;
      }
      const asset = release.assets.get(candidate.parsed.assetName);
      if (!asset) {
        warnListing(
          warnings,
          id,
          `skipped (asset '${candidate.parsed.assetName}' not found)`,
          candidate.version,
        );
        continue;
      }

      downloadsByListing[id][candidate.version] = asset.downloadCount;
    }
  }

  const sortedDownloads: D.DownloadsByListing = {};
  for (const id of [...ids].sort()) {
    sortedDownloads[id] = sortObjectByKeys(downloadsByListing[id] ?? {});
  }

  const integrity = loadIntegritySnapshot(repoRoot, dir) ?? emptyIntegrity(nowIso);
  let completeVersions = 0;
  let incompleteVersions = 0;
  for (const listing of Object.values(integrity.listings)) {
    for (const version of Object.values(listing.versions)) {
      if (version.is_complete) {
        completeVersions += 1;
      } else {
        incompleteVersions += 1;
      }
    }
  }

  return {
    downloads: sortedDownloads,
    integrity,
    integrityCache: loadIntegrityCache(repoRoot, dir),
    stats: {
      listings: ids.length,
      versions_checked: versionsChecked,
      complete_versions: completeVersions,
      incomplete_versions: incompleteVersions,
      filtered_versions: 0,
      cache_hits: 0,
    },
    warnings,
    rateLimit: graphqlUsageSnapshot(usageState),
  };
}
