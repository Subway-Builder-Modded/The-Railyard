import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import type { ManifestType } from "./manifests.js";
import type { DownloadsByListing } from "./download-definitions.js";
import { isObject, toFiniteNonNegativeNumber, sortObjectByKeys } from "./json-utils.js";

/**
 * Loads frozen download counts for versions that were clamped before being
 * marked incomplete by integrity checks (e.g. manifest-version-mismatch).
 *
 * These counts are preserved indefinitely so that historical download totals
 * remain monotonically non-decreasing across snapshots.
 */
export function loadGrandfatheredDownloads(
  repoRoot: string,
  listingType: ManifestType,
): Record<string, Record<string, number>> {
  const dir = listingType === "map" ? "maps" : "mods";
  const path = resolve(repoRoot, dir, "grandfathered-downloads.json");
  if (!existsSync(path)) return {};
  try {
    const raw = JSON.parse(readFileSync(path, "utf-8")) as unknown;
    if (!isObject(raw)) return {};
    const result: Record<string, Record<string, number>> = {};
    for (const [listingId, versions] of Object.entries(raw)) {
      if (!isObject(versions)) continue;
      const versionMap: Record<string, number> = {};
      for (const [version, count] of Object.entries(versions as Record<string, unknown>)) {
        const n = toFiniteNonNegativeNumber(count);
        if (n !== null && n > 0) versionMap[version] = n;
      }
      if (Object.keys(versionMap).length > 0) result[listingId] = versionMap;
    }
    return result;
  } catch {
    return {};
  }
}

/**
 * Freezes a listing's last-known download counts into
 * grandfathered-downloads.json. Used at deprecation time: the deprecation
 * overlay removes the listing from pipeline output every run, and the
 * grandfathered merge is what keeps its historical totals visible. Sources,
 * per version, the max of the currently committed downloads.json value and
 * any existing grandfathered value — the listing's PUBLISHED state only.
 * The version-bucket ledger is deliberately not consulted: it remembers
 * maxes for release assets the pipeline never surfaced (old tag-format
 * releases outside the enumeration window) and retains pre-correction highs,
 * and freezing those resurrects counts the registry never reported — they
 * appear as a phantom download spike on the deprecation day
 * (advanced-analytics +335 on 2026-08-22; busan-3 +343 on 2026-09-10).
 * Returns the frozen map, or null when the listing has no published counts
 * (nothing to freeze).
 */
export function freezeListingDownloads(
  repoRoot: string,
  listingType: ManifestType,
  listingId: string,
): Record<string, number> | null {
  const dir = listingType === "map" ? "maps" : "mods";
  const grandfathered = loadGrandfatheredDownloads(repoRoot, listingType);

  let currentDownloads: Record<string, number> = {};
  const downloadsPath = resolve(repoRoot, dir, "downloads.json");
  if (existsSync(downloadsPath)) {
    try {
      const raw = JSON.parse(readFileSync(downloadsPath, "utf-8")) as unknown;
      if (isObject(raw) && isObject((raw as Record<string, unknown>)[listingId])) {
        currentDownloads = (raw as Record<string, Record<string, number>>)[listingId]!;
      }
    } catch {
      // downloads.json unreadable — freeze from existing grandfathered only
    }
  }

  const frozen: Record<string, number> = { ...(grandfathered[listingId] ?? {}) };
  const consider = (version: string, count: unknown) => {
    const n = toFiniteNonNegativeNumber(count);
    if (n === null || n <= 0) return;
    if (frozen[version] === undefined || n > frozen[version]!) frozen[version] = n;
  };
  for (const [version, count] of Object.entries(currentDownloads)) {
    consider(version, count);
  }
  if (Object.keys(frozen).length === 0) return null;

  const next: Record<string, Record<string, number>> = { ...grandfathered, [listingId]: frozen };
  const sorted: Record<string, Record<string, number>> = {};
  for (const id of Object.keys(next).sort()) {
    sorted[id] = sortObjectByKeys(next[id]!);
  }
  writeFileSync(
    resolve(repoRoot, dir, "grandfathered-downloads.json"),
    JSON.stringify(sorted, null, 2) + "\n",
  );
  return frozen;
}

/**
 * Merges grandfathered (frozen) download counts into pipeline output.
 * Only fills in versions that the pipeline did not produce (i.e. versions
 * marked incomplete by integrity checks). If the pipeline already produced
 * a count for a version, that count takes precedence.
 */
export function mergeGrandfatheredDownloads(
  downloads: DownloadsByListing,
  grandfathered: Record<string, Record<string, number>>,
): DownloadsByListing {
  if (Object.keys(grandfathered).length === 0) return downloads;
  const merged: DownloadsByListing = {};
  for (const [listingId, versions] of Object.entries(downloads)) {
    merged[listingId] = { ...versions };
  }
  for (const [listingId, versions] of Object.entries(grandfathered)) {
    if (!merged[listingId]) merged[listingId] = {};
    for (const [version, frozenCount] of Object.entries(versions)) {
      if (merged[listingId]![version] === undefined) {
        merged[listingId]![version] = frozenCount;
      }
    }
  }

  // Sort listing IDs and version keys within each listing for stable, readable output
  const sorted: DownloadsByListing = {};
  for (const listingId of Object.keys(merged).sort()) {
    sorted[listingId] = sortObjectByKeys(merged[listingId]!);
  }
  return sorted;
}
