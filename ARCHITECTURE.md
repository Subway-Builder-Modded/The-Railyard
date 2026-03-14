# The Railyard - Architecture

The Railyard is the central metadata registry for **Subway Builder** community mods and maps.
This repository stores manifests, gallery assets, and update pointers. It does not store mod/map binaries.

## Repository Layout

```text
The-Railyard/
|-- .github/
|   |-- ISSUE_TEMPLATE/
|   |   |-- config.yml
|   |   |-- publish-mod.yml
|   |   |-- publish-map.yml        # auto-generated
|   |   |-- update-mod.yml
|   |   |-- update-map.yml         # auto-generated
|   |   `-- report.yml
|   `-- workflows/
|       |-- publish.yml
|       |-- update-metadata.yml
|       |-- regenerate-index.yml
|       |-- regenerate-downloads-hourly.yml
|       |-- regenerate-registry-analytics.yml
|       |-- cache-download-history.yml
|       |-- close-invalid.yml
|       `-- report.yml
|-- scripts/
|   |-- lib/
|   |   |-- manifests.ts
|   |   |-- map-constants.ts
|   |   |-- map-field-utils.ts
|   |   |-- map-update-logic.ts
|   |   |-- downloads.ts
|   |   |-- download-history.ts
|   |   |-- map-demand-stats.ts
|   |   |-- release-resolution.ts
|   |   |-- discord-webhook.ts
|   |   |-- registry-manifest.ts
|   |   |-- mod-manifest.ts
|   |   |-- gallery.ts
|   |   |-- github.ts
|   |   `-- custom-url.ts
|   |-- tests/
|   |-- generate-map-templates.ts
|   |-- validate-publish.ts
|   |-- validate-update.ts
|   |-- create-listing.ts
|   |-- update-listing.ts
|   |-- generate-downloads.ts
|   |-- generate-download-history.ts
|   |-- generate-map-demand-stats.ts
|   |-- notify-discord.ts
|   `-- regenerate-indexes.ts
|-- mods/
|   |-- index.json
|   |-- downloads.json
|   |-- integrity.json
|   |-- integrity-cache.json
|   `-- <mod-id>/
|       |-- manifest.json
|       `-- gallery/
|-- maps/
|   |-- index.json
|   |-- downloads.json
|   |-- integrity.json
|   |-- integrity-cache.json
|   |-- demand-stats-cache.json
|   `-- <map-id>/
|       |-- manifest.json
|       `-- gallery/
|-- history/
|   `-- snapshot_YYYY_MM_DD.json
|-- README.md
`-- ARCHITECTURE.md
```

## Data Model

### Index files

- `mods/index.json`
- `maps/index.json`

Both are generated from existing listing directories after merges.

### Download count snapshots

- `mods/downloads.json`
- `maps/downloads.json`

Each file maps listing IDs to versioned cumulative download counts:

```json
{
  "some-listing-id": {
    "v1.0.0": 120,
    "v1.1.0": 237
  }
}
```

Count policy:

- zip assets only
- for unresolved custom versions (non-GitHub URL, missing tag/asset), version is skipped and warning is emitted in workflow logs
- semver versions that fail integrity checks are hard-filtered from `downloads.json`

### Release integrity snapshots

- `mods/integrity.json`
- `maps/integrity.json`

Each file includes:

- `schema_version`
- `generated_at`
- `listings.<id>` with:
- `has_complete_version`, `latest_semver_version`, `latest_semver_complete`
- `complete_versions`, `incomplete_versions`
- `versions.<version>` entries containing:
- `is_complete`, `errors`
- `required_checks`, `matched_files`
- `source`, `fingerprint`, `checked_at`

Integrity cache files:

- `mods/integrity-cache.json`
- `maps/integrity-cache.json`

Caches are fingerprint-based to avoid repeated ZIP extraction for unchanged versions.

### Download history snapshots

- `history/snapshot_YYYY_MM_DD.json`

Each daily snapshot includes:

- `maps.downloads` and `mods.downloads`
- `maps.index` and `mods.index`
- `total_downloads` (sum of all version counts in `downloads`)
- `net_downloads` (delta versus previous snapshot; falls back to total if first snapshot)
- `entries` (cardinality of `index.json` listing array)

### Mod manifest (`mods/<mod-id>/manifest.json`)

```json
{
  "schema_version": 1,
  "id": "better-trains",
  "name": "Better Trains",
  "author": "someuser",
  "github_id": 123456,
  "description": "Adds new train models.",
  "tags": ["trains", "cosmetic"],
  "gallery": ["gallery/screenshot1.png"],
  "source": "https://github.com/someuser/better-trains",
  "update": {
    "type": "github",
    "repo": "someuser/better-trains"
  }
}
```

### Map manifest (`maps/<map-id>/manifest.json`)

Maps include all mod fields plus map-specific metadata:

```json
{
  "schema_version": 1,
  "id": "raleigh",
  "name": "Raleigh",
  "author": "muffintime",
  "github_id": 87654321,
  "description": "Custom map of the Raleigh metro area.",
  "tags": ["north-america", "airports"],
  "gallery": ["gallery/screenshot1.png"],
  "source": "https://github.com/muffintime/sb-raleigh",
  "update": {
    "type": "github",
    "repo": "muffintime/sb-raleigh"
  },
  "city_code": "RDU",
  "country": "US",
  "population": 1500000,
  "residents_total": 1500000,
  "points_count": 4242,
  "population_count": 1500000,
  "data_source": "LODES",
  "source_quality": "high-quality",
  "level_of_detail": "medium-detail",
  "location": "north-america",
  "special_demand": ["airports"]
}
```

Map-specific fields:

- `city_code`: `^[A-Z0-9]{2,4}$`
- `country`: ISO-3166-1 alpha-2 code
- `population`: integer >= 0 (auto-derived from demand data)
- `residents_total`: integer >= 0 (sum of demand point residents)
- `points_count`: integer >= 0 (number of demand points)
- `population_count`: integer >= 0 (number of population entries)
- `data_source`: non-empty string
- `source_quality`: `low-quality | medium-quality | high-quality`
- `level_of_detail`: `low-detail | medium-detail | high-detail`
- `location`: exactly one location tag
- `special_demand`: array of feature tags

Backward compatibility rule:

- For map manifests, `tags` is still maintained as the union of `location` and `special_demand`.

OSM quality rule:

- OSM-based data sources cannot be `high-quality`.
- Create/update logic caps OSM-derived `high-quality` to `medium-quality`.
- Update validation rejects OSM + `high-quality` combinations.

## Tags and Taxonomy

### Mod tags

Category tags from issue templates:
`cosmetic`, `gameplay`, `library`, `misc`, `qol`, `stations`, `tracks`, `trains`, `ui`.

### Map location tags (exactly one)

`caribbean`, `central-america`, `central-asia`, `east-africa`, `east-asia`, `europe`, `middle-east`, `north-africa`, `north-america`, `oceania`, `south-america`, `south-asia`, `southeast-asia`, `southern-africa`, `west-africa`.

### Map special-demand tags (zero or more)

`airports`, `entertainment`, `ferries`, `hospitals`, `parks`, `schools`, `universities`.

## Update Sources

`update` supports two formats:

```json
{ "type": "github", "repo": "owner/repo" }
```

```json
{ "type": "custom", "url": "https://example.com/update.json" }
```

Validation checks reachable/updateable endpoints at publish/update time.

## Map ZIP Format

Map ZIPs follow the patcher/map-manager expectations and place files at archive root:

```text
map-name.zip
|-- config.json
|-- demand_data.json
|-- buildings_index.json
|-- roads.geojson
|-- runways_taxiways.geojson
`-- XXX.pmtiles
```

## Issue Templates and Generation

- `publish-map.yml` and `update-map.yml` are generated by `scripts/generate-map-templates.ts`.
- The generator uses a shared field definition after `map-id` and enforces identical template tails.
- Generation/check uses YAML + AJV schema validation.
- Templates should not be edited manually.

## CI Workflow

### Publish flow

1. User opens publish issue (`publish-mod` or `publish-map`).
2. Workflow parses issue form values.
3. `validate-publish.ts` validates payload and external references.
4. `create-listing.ts` writes listing files and downloads gallery images.
5. Workflow opens a PR for maintainers.

### Update flow

1. User opens update issue (`update-mod` or `update-map`).
2. `validate-update.ts` checks listing existence and ownership (`github_id`).
3. Map updates additionally validate map field constraints via shared map update logic.
4. `update-listing.ts` applies only requested field changes.
5. Workflow opens a PR.

### Post-merge flow

- `regenerate-indexes.ts` rebuilds `mods/index.json` and `maps/index.json`.

### Scheduled analytics flow

- `regenerate-downloads-hourly.yml` runs hourly and on manual dispatch.
- It runs map/mod generation in download-only mode (no ZIP integrity pass) and commits updated `downloads.json` files if changed.
- `regenerate-registry-analytics.yml` runs every 8 hours and on manual dispatch.
- It runs map/mod generation in full mode and map demand stats generation, then commits updated `downloads.json` + `integrity.json` (+ integrity cache files), map manifests, and `maps/demand-stats-cache.json` if changed.
- It emits two Discord summaries in a single run (downloads/integrity and map demand stats).
- Uses GitHub GraphQL `ReleaseAsset.downloadCount` with `GITHUB_TOKEN` by default (`GH_DOWNLOADS_TOKEN` optional override).
- `cache-download-history.yml` runs daily and on manual dispatch.
- It snapshots current `maps/downloads.json` + `mods/downloads.json` with indexes into `history/snapshot_YYYY_MM_DD.json`.
- It computes `net_downloads` against the previous snapshot for trend and popularity analysis.
- Map demand stats subflow (inside `regenerate-registry-analytics.yml`) refreshes demand-derived metadata in manifests and updates `maps/demand-stats-cache.json`.
- It skips ZIP extraction when source fingerprints are unchanged:
- For `sha256:*` fingerprints, skip regardless of age.
- For other fingerprints, skip when last checked within 9 hours.
- Reason for non-`sha256` fallback:
- Tag/asset-name or URL-based fingerprints can remain unchanged while upstream ZIP content is replaced, so periodic rechecks prevent stale derived stats.

## Script Responsibilities

- `validate-publish.ts`: publish-time validation for maps/mods.
- `validate-update.ts`: update-time validation and ownership checks.
- `create-listing.ts`: creates new manifests and gallery files.
- `update-listing.ts`: applies manifest metadata updates.
- `regenerate-indexes.ts`: reindexes listings.
- `generate-downloads.ts`: generates downloads in `full` or `download-only` mode.
- `generate-download-history.ts`: caches daily combined download snapshots in `history/`.
- `generate-map-demand-stats.ts`: updates map `population`/`residents_total`/`points_count`/`population_count`.
- `generate-map-templates.ts`: generates and verifies map issue templates.
- `notify-discord.ts`: shared Discord webhook notifier for workflow summaries.

## Testing

Script-level tests live under `scripts/tests` and currently cover:

- map field utility behavior/defaults
- map template generation invariants
- map update integration behavior (changed-fields-only + invalid existing-state failure)
