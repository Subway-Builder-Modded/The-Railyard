import type { MapManifest } from "./manifests.js";
import * as D from "./download-definitions.js";
import { createGraphqlUsageState, fetchRepoReleaseIndexes, isSupportedReleaseTag, graphqlUsageSnapshot } from "./release-resolution.js";
import type { IntegrityCache, IntegrityCacheEntry, IntegritySource, IntegrityVersionEntry, ListingIntegrityEntry } from "./integrity.js";
import { inspectZipCompleteness } from "./integrity.js";
import {
  type ListingContext,
  buildIncompleteVersionEntry,
  createListingIntegrityEntry,
  fetchCustomVersions,
  fetchZipBuffer,
  getDirectoryForType,
  getIndexIds,
  getManifest,
  loadIntegrityCache,
  shouldUseCachedIntegrity,
  sortObjectByKeys,
  warnListing,
  withCheckResult,
} from "./downloads-support.js";

export async function generateDownloadsDataFull(
  options: D.GenerateDownloadsOptions,
): Promise<D.GenerateDownloadsResult> {
  const repoRoot = options.repoRoot;
  const listingType = options.listingType;
  const fetchImpl = options.fetchImpl ?? fetch;
  const token = options.token;
  const warnings: string[] = [];
  const dir = getDirectoryForType(listingType);
  const ids = getIndexIds(repoRoot, dir);
  const now = new Date();
  const nowIso = now.toISOString();

  const cache = loadIntegrityCache(repoRoot, dir);
  const nextCache: IntegrityCache = {
    schema_version: 1,
    entries: {},
  };

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

  const integrityListings: Record<string, ListingIntegrityEntry> = {};
  let versionsChecked = 0;
  let completeVersions = 0;
  let incompleteVersions = 0;
  let filteredVersions = 0;
  let cacheHits = 0;

  for (const id of [...ids].sort()) {
    console.log(`[downloads] heartbeat:listing mode=full listing=${id}`);
    const context = listingContexts.get(id);
    if (!context) {
      integrityListings[id] = createListingIntegrityEntry({});
      continue;
    }

    const versionEntries: Record<string, IntegrityVersionEntry> = {};
    const listingCacheEntries = cache.entries[id] ?? {};
    const nextListingCacheEntries: Record<string, IntegrityCacheEntry> = {};

    if (context.update.type === "github") {
      const repo = context.update.repo;
      const repoIndex = repoIndexes.get(repo);
      if (!repoIndex) {
        warnListing(warnings, id, "skipped all github-release versions (repo unavailable)");
        integrityListings[id] = createListingIntegrityEntry(versionEntries);
        nextCache.entries[id] = nextListingCacheEntries;
        continue;
      }

      for (const tag of [...repoIndex.byTag.keys()].sort()) {
        const releaseData = repoIndex.byTag.get(tag);
        if (!releaseData) continue;
        versionsChecked += 1;

        const zipAssets = Array.from(releaseData.assets.entries())
          .filter(([assetName]) => assetName.toLowerCase().endsWith(".zip"));
        const zipAssetNames = zipAssets.map(([assetName]) => assetName).sort();
        const fingerprint = zipAssetNames.length > 0
          ? `github:${repo}:${tag}:${zipAssetNames.join("|")}`
          : `github:${repo}:${tag}:no-zip`;
        const cached = listingCacheEntries[tag];
        const sourceBase: IntegritySource = {
          update_type: "github",
          repo,
          tag,
        };

        if (shouldUseCachedIntegrity(cached, fingerprint, now)) {
          cacheHits += 1;
          versionEntries[tag] = cached.result;
          nextListingCacheEntries[tag] = cached;
        } else if (!isSupportedReleaseTag(tag)) {
          const result = buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            [`non-semver release tag '${tag}'`],
          );
          versionEntries[tag] = result;
          nextListingCacheEntries[tag] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        } else if (zipAssets.length === 0) {
          const result = buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            ["release has no .zip asset"],
          );
          versionEntries[tag] = result;
          nextListingCacheEntries[tag] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        } else {
          const hasReleaseManifestAsset = releaseData.assets.has("manifest.json");
          let selectedResult: IntegrityVersionEntry | null = null;
          const attemptedErrors: string[] = [];

          for (const [assetName, asset] of zipAssets.sort(([a], [b]) => a.localeCompare(b))) {
            if (!asset.downloadUrl) {
              attemptedErrors.push(`zip asset '${assetName}' is missing download URL`);
              continue;
            }
            const zipBuffer = await fetchZipBuffer(id, asset.downloadUrl, fetchImpl, warnings, tag, assetName);
            if (!zipBuffer) {
              attemptedErrors.push(`zip asset '${assetName}' could not be fetched`);
              continue;
            }
            const check = await inspectZipCompleteness(listingType, zipBuffer, {
              cityCode: context.cityCode,
              releaseHasManifestAsset: hasReleaseManifestAsset,
            });
            selectedResult = withCheckResult(
              check,
              { ...sourceBase, asset_name: assetName, download_url: asset.downloadUrl },
              fingerprint,
              nowIso,
            );
            if (check.isComplete) {
              break;
            }
            attemptedErrors.push(...check.errors.map((error) => `asset '${assetName}': ${error}`));
            selectedResult = null;
          }

          const result = selectedResult ?? buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            attemptedErrors.length > 0 ? attemptedErrors : ["all zip assets failed integrity checks"],
          );
          versionEntries[tag] = result;
          nextListingCacheEntries[tag] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        }

        if (isSupportedReleaseTag(tag)) {
          const result = versionEntries[tag];
          if (result.is_complete) {
            downloadsByListing[id][tag] = releaseData.zipTotal;
          } else {
            filteredVersions += 1;
            warnListing(
              warnings,
              id,
              `excluded by integrity validation (${result.errors.join("; ") || "unknown error"})`,
              tag,
            );
          }
        }
      }
    } else {
      for (const candidate of context.update.versions) {
        const versionKey = candidate.version;
        versionsChecked += 1;

        const fallbackFingerprint = candidate.sha256
          ? `sha256:${candidate.sha256}`
          : `custom:${versionKey}:${candidate.downloadUrl ?? "missing-download"}`;
        const fingerprint = candidate.sha256
          ? `sha256:${candidate.sha256}`
          : (
            candidate.parsed
              ? `custom:${candidate.parsed.repo}:${candidate.parsed.tag}:${candidate.parsed.assetName}:${candidate.downloadUrl ?? "missing-download"}`
              : fallbackFingerprint
          );
        const sourceBase: IntegritySource = {
          update_type: "custom",
          repo: candidate.parsed?.repo,
          tag: candidate.parsed?.tag,
          asset_name: candidate.parsed?.assetName,
          download_url: candidate.downloadUrl ?? undefined,
        };
        const cached = listingCacheEntries[versionKey];

        if (shouldUseCachedIntegrity(cached, fingerprint, now)) {
          cacheHits += 1;
          versionEntries[versionKey] = cached.result;
          nextListingCacheEntries[versionKey] = cached;
        } else if (candidate.errors.length > 0) {
          const result = buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            candidate.errors,
          );
          versionEntries[versionKey] = result;
          nextListingCacheEntries[versionKey] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        } else if (!candidate.parsed) {
          const result = buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            ["download URL could not be parsed as a GitHub release asset URL"],
          );
          versionEntries[versionKey] = result;
          nextListingCacheEntries[versionKey] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        } else if (!candidate.parsed.assetName.toLowerCase().endsWith(".zip")) {
          const result = buildIncompleteVersionEntry(
            sourceBase,
            fingerprint,
            nowIso,
            [`download asset '${candidate.parsed.assetName}' is not a .zip`],
          );
          versionEntries[versionKey] = result;
          nextListingCacheEntries[versionKey] = {
            fingerprint,
            last_checked_at: nowIso,
            result,
          };
        } else {
          const repoIndex = repoIndexes.get(candidate.parsed.repo);
          if (!repoIndex) {
            const result = buildIncompleteVersionEntry(
              sourceBase,
              fingerprint,
              nowIso,
              ["repository is unavailable via GitHub GraphQL"],
            );
            versionEntries[versionKey] = result;
            nextListingCacheEntries[versionKey] = {
              fingerprint,
              last_checked_at: nowIso,
              result,
            };
          } else {
            const release = repoIndex.byTag.get(candidate.parsed.tag);
            if (!release) {
              const result = buildIncompleteVersionEntry(
                sourceBase,
                fingerprint,
                nowIso,
                [`release tag '${candidate.parsed.tag}' not found`],
              );
              versionEntries[versionKey] = result;
              nextListingCacheEntries[versionKey] = {
                fingerprint,
                last_checked_at: nowIso,
                result,
              };
            } else {
              const asset = release.assets.get(candidate.parsed.assetName);
              if (!asset) {
                const result = buildIncompleteVersionEntry(
                  sourceBase,
                  fingerprint,
                  nowIso,
                  [`release asset '${candidate.parsed.assetName}' not found`],
                );
                versionEntries[versionKey] = result;
                nextListingCacheEntries[versionKey] = {
                  fingerprint,
                  last_checked_at: nowIso,
                  result,
                };
              } else if (!asset.downloadUrl) {
                const result = buildIncompleteVersionEntry(
                  sourceBase,
                  fingerprint,
                  nowIso,
                  [`release asset '${candidate.parsed.assetName}' has no download URL`],
                );
                versionEntries[versionKey] = result;
                nextListingCacheEntries[versionKey] = {
                  fingerprint,
                  last_checked_at: nowIso,
                  result,
                };
              } else {
                const zipBuffer = await fetchZipBuffer(
                  id,
                  asset.downloadUrl,
                  fetchImpl,
                  warnings,
                  versionKey,
                  candidate.parsed.assetName,
                );
                if (!zipBuffer) {
                  const result = buildIncompleteVersionEntry(
                    sourceBase,
                    fingerprint,
                    nowIso,
                    [`failed to fetch ZIP asset '${candidate.parsed.assetName}'`],
                  );
                  versionEntries[versionKey] = result;
                  nextListingCacheEntries[versionKey] = {
                    fingerprint,
                    last_checked_at: nowIso,
                    result,
                  };
                } else {
                  const check = await inspectZipCompleteness(listingType, zipBuffer, {
                    cityCode: context.cityCode,
                    releaseHasManifestAsset: release.assets.has("manifest.json"),
                  });
                  const result = withCheckResult(
                    check,
                    {
                      ...sourceBase,
                      asset_name: candidate.parsed.assetName,
                      download_url: asset.downloadUrl,
                    },
                    fingerprint,
                    nowIso,
                  );
                  versionEntries[versionKey] = result;
                  nextListingCacheEntries[versionKey] = {
                    fingerprint,
                    last_checked_at: nowIso,
                    result,
                  };
                }
              }
            }
          }
        }

        if (candidate.semver) {
          const result = versionEntries[versionKey];
          if (result?.is_complete === true && candidate.parsed) {
            const repoIndex = repoIndexes.get(candidate.parsed.repo);
            const release = repoIndex?.byTag.get(candidate.parsed.tag);
            const asset = release?.assets.get(candidate.parsed.assetName);
            if (asset) {
              downloadsByListing[id][versionKey] = asset.downloadCount;
            }
          } else {
            filteredVersions += 1;
            warnListing(
              warnings,
              id,
              `excluded by integrity validation (${result?.errors.join("; ") || "unknown error"})`,
              versionKey,
            );
          }
        }
      }
    }

    for (const result of Object.values(versionEntries)) {
      if (result.is_complete) {
        completeVersions += 1;
      } else {
        incompleteVersions += 1;
      }
    }

    integrityListings[id] = createListingIntegrityEntry(versionEntries);
    nextCache.entries[id] = sortObjectByKeys(nextListingCacheEntries);
  }

  const sortedDownloads: D.DownloadsByListing = {};
  for (const id of [...ids].sort()) {
    sortedDownloads[id] = sortObjectByKeys(downloadsByListing[id] ?? {});
  }

  return {
    downloads: sortedDownloads,
    integrity: {
      schema_version: 1,
      generated_at: nowIso,
      listings: sortObjectByKeys(integrityListings),
    },
    integrityCache: {
      schema_version: 1,
      entries: sortObjectByKeys(nextCache.entries),
    },
    stats: {
      listings: ids.length,
      versions_checked: versionsChecked,
      complete_versions: completeVersions,
      incomplete_versions: incompleteVersions,
      filtered_versions: filteredVersions,
      cache_hits: cacheHits,
    },
    warnings,
    rateLimit: graphqlUsageSnapshot(usageState),
  };
}
