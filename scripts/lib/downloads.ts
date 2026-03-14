import type * as D from "./download-definitions.js";
import { generateDownloadsDataDownloadOnly } from "./downloads-download-only.js";
import { generateDownloadsDataFull } from "./downloads-full.js";
import { aggregateZipDownloadCountsByTag } from "./downloads-support.js";
import { isSupportedReleaseTag, parseGitHubReleaseAssetDownloadUrl } from "./release-resolution.js";

export type {
  ParsedReleaseAssetUrl,
  DownloadsByListing,
  GenerateDownloadsOptions,
  GenerateDownloadsResult,
} from "./download-definitions.js";

export { isSupportedReleaseTag, parseGitHubReleaseAssetDownloadUrl, aggregateZipDownloadCountsByTag };

/**
 * Generates deterministic per-listing download counts for maps or mods and
 * produces integrity metadata for each version.
 *
 * Modes:
 * - `full`: includes ZIP integrity inspection and integrity cache updates
 * - `download-only`: counts semver GitHub release assets without ZIP integrity inspection
 */
export async function generateDownloadsData(
  options: D.GenerateDownloadsOptions,
): Promise<D.GenerateDownloadsResult> {
  if ((options.mode ?? "full") === "download-only") {
    return generateDownloadsDataDownloadOnly(options);
  }
  return generateDownloadsDataFull(options);
}
