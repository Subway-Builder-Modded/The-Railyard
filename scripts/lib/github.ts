/**
 * Parse a GitHub releases/tag URL into its repo and tag components.
 * Returns null if the URL isn't a GitHub release tag URL.
 */
function parseGitHubReleaseTagUrl(url: string): { repo: string; tag: string } | null {
  const match = url.match(/github\.com\/([^/]+\/[^/]+)\/releases\/tag\/([^/?#]+)/i);
  if (!match) return null;
  return { repo: match[1], tag: decodeURIComponent(match[2]) };
}

export async function validateGitHubRepo(repo: string, sourceUrl?: string): Promise<string[]> {
  const errors: string[] = [];
  const token = process.env.GITHUB_TOKEN;
  const headers: Record<string, string> = { Accept: "application/vnd.github.v3+json" };
  if (token) headers["Authorization"] = `Bearer ${token}`;

  // 1. Check repo exists
  const repoRes = await fetch(`https://api.github.com/repos/${repo}`, { headers });
  if (!repoRes.ok) {
    errors.push(`**github-repo**: Repository \`${repo}\` does not exist or is not accessible.`);
    return errors;
  }

  // 2. Check releases exist
  const releasesRes = await fetch(`https://api.github.com/repos/${repo}/releases?per_page=1`, { headers });
  const releases: { tag_name: string; assets: { name: string }[] }[] = await releasesRes.json();
  if (!Array.isArray(releases) || releases.length === 0) {
    errors.push(`**github-repo**: Repository \`${repo}\` has no releases. Create at least one release with a .zip asset.`);
    return errors;
  }

  // 3. Check latest release has a .zip asset
  const assets = releases[0].assets || [];
  const hasZip = assets.some((a) => a.name.endsWith(".zip"));
  if (!hasZip) {
    errors.push(`**github-repo**: Latest release in \`${repo}\` has no .zip asset. Upload a .zip file to your release.`);
  }

  // 4. Monorepo detection — check if the source URL points to a specific
  //    release tag in the same repo, which indicates the repo hosts multiple
  //    mods/maps as separate releases. The mod manager always downloads the
  //    latest release, so this would serve the wrong file.
  if (sourceUrl) {
    const parsed = parseGitHubReleaseTagUrl(sourceUrl);
    if (parsed && parsed.repo.toLowerCase() === repo.toLowerCase()) {
      const latestTag = releases[0].tag_name;
      const sourceTag = parsed.tag;

      const updateJsonExample = `https://raw.githubusercontent.com/${repo}/main/${sourceTag.toLowerCase()}-update.json`;

      if (sourceTag !== latestTag) {
        // Source points to a non-latest release — mod manager WILL serve wrong file
        errors.push(
          `**github-repo**: Your source URL points to the \`${sourceTag}\` release, but the latest release in \`${repo}\` is tagged \`${latestTag}\`. ` +
          `The mod manager always downloads the **latest** release, so it would serve the wrong file.\n\n` +
          `This happens when a repository hosts multiple mods/maps as separate releases (a "monorepo"). ` +
          `**GitHub Releases** update type only supports repositories with a single mod or map.\n\n` +
          `To fix this, switch your update type to **Custom URL** and create an \`update.json\` file. ` +
          `You can host it in the same repo — for example:\n\`${updateJsonExample}\`\n\n` +
          `See [the update.json format](https://github.com/Subway-Builder-Modded/The-Railyard/blob/main/ARCHITECTURE.md#custom-updatejson-format) in the docs.`
        );
      } else if (!/^v?\d/.test(sourceTag)) {
        // Source tag matches latest but doesn't look like a version — proactive detection.
        // Tags like "SCK", "LAX" are project identifiers, not versions.
        errors.push(
          `**github-repo**: Your source URL points to the \`${sourceTag}\` release in \`${repo}\`. ` +
          `This tag doesn't look like a version number (e.g. \`v1.0\`, \`1.2.0\`), which suggests this repository ` +
          `may host multiple mods/maps as separate releases.\n\n` +
          `**GitHub Releases** update type always downloads the latest release — if another release is published ` +
          `to this repo, the mod manager will start serving the wrong file.\n\n` +
          `To fix this, switch your update type to **Custom URL** and create an \`update.json\` file. ` +
          `You can host it in the same repo — for example:\n\`${updateJsonExample}\`\n\n` +
          `See [the update.json format](https://github.com/Subway-Builder-Modded/The-Railyard/blob/main/ARCHITECTURE.md#custom-updatejson-format) in the docs.`
        );
      }
    }
  }

  return errors;
}
