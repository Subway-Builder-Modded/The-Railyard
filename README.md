# The Railyard

The central registry for **Subway Builder** community mods and custom maps.

> [!WARNING]
> **Work in Progress** - The Railyard is still under active development. Expect changes to the submission process, schema, and tooling.

## Submit Your Work

All submissions are handled through GitHub Issues. Pick a template to get started:

- [Publish a New Mod](https://github.com/Subway-Builder-Modded/The-Railyard/issues/new?template=publish-mod.yml)
- [Publish a New Map](https://github.com/Subway-Builder-Modded/The-Railyard/issues/new?template=publish-map.yml)
- [Update an Existing Mod](https://github.com/Subway-Builder-Modded/The-Railyard/issues/new?template=update-mod.yml)
- [Update an Existing Map](https://github.com/Subway-Builder-Modded/The-Railyard/issues/new?template=update-map.yml)
- [Report an Issue](https://github.com/Subway-Builder-Modded/The-Railyard/issues/new?template=report.yml)

## How It Works

The Railyard stores metadata only - manifests, gallery images, and pointers to where your mod or map is actually hosted (GitHub Releases, CDNs, etc.). When you submit through an issue template, CI validates your submission and opens a PR automatically. Once merged, your listing goes live.

## Map Issue Templates

`publish-map.yml` and `update-map.yml` are generated from a shared script:

- Generate both templates:
  - `pnpm --dir scripts run generate:map-templates`
- Verify templates are up to date:
  - `pnpm --dir scripts run check:map-templates`

## Registry Analytics

Download and release-integrity snapshots are generated into:

- `maps/downloads.json`
- `mods/downloads.json`
- `maps/integrity.json`
- `mods/integrity.json`

Local commands:

- Generate full registry analytics (downloads + integrity):
  - `pnpm --dir scripts run generate-registry-analytics`
- Generate maps only:
  - `pnpm --dir scripts run generate-registry-analytics:maps`
- Generate mods only:
  - `pnpm --dir scripts run generate-registry-analytics:mods`
- Generate maps only (download-only mode):
  - `pnpm --dir scripts run generate-downloads:maps:download-only`
- Generate mods only (download-only mode):
  - `pnpm --dir scripts run generate-downloads:mods:download-only`
- Generate map demand stats only:
  - `pnpm --dir scripts run generate-registry-demand-stats`

Integrity behavior:

- Only semver versions (`vX.Y.Z` or `X.Y.Z`) are eligible for download counting.
- Versions that fail integrity checks are excluded from `downloads.json`.
- Non-semver versions are still recorded in `integrity.json` as incomplete.

Automation:

- `regenerate-downloads-hourly.yml` runs hourly in download-only mode (updates downloads only; no ZIP integrity pass).
- `regenerate-registry-analytics.yml` runs every 8 hours in full mode (refreshes downloads + integrity + integrity cache, and map demand stats).
- Full mode posts two Discord summaries (downloads/integrity and map demand stats) to the same webhook secret: `DISCORD_WEBHOOK_URL`.

## Download History

Daily combined download snapshots are cached under:

- `history/snapshot_YYYY_MM_DD.json`

Each snapshot includes:

- `maps` and `mods` sections
- embedded current `downloads` and `index` payloads
- `total_downloads`
- `net_downloads` versus the previous snapshot (or total on first snapshot)
- `entries` count from the corresponding `index.json`

Local command:

- Generate/update today’s history snapshot:
  - `pnpm --dir scripts run generate-download-history`

Automation:

- `cache-download-history.yml` runs daily (and on manual dispatch), commits `history/snapshot_YYYY_MM_DD.json`, and posts summary stats to Discord.

## Map Demand Stats

Map manifests now support auto-derived demand metrics from map ZIPs:

- `population` (auto-derived from residents total, kept for backwards compatibility)
- `residents_total`
- `points_count`
- `population_count`

Local command:

- Generate/refresh map demand stats:
  - `pnpm --dir scripts run generate-registry-demand-stats`
  - (alias) `pnpm --dir scripts run generate-map-demand-stats`
  - Force refresh all maps (ignore SHA/cache skip):
    - `pnpm --dir scripts run generate-registry-demand-stats -- --force`
  - Refresh one map by id:
    - `pnpm --dir scripts run generate-registry-demand-stats -- --id <map-id>`

For technical details, see [ARCHITECTURE.md](ARCHITECTURE.md).
