# The Railyard - Architecture

The Railyard is the central registry for **Subway Builder** community mods and custom maps. It stores metadata (manifests, gallery images) and points to external sources (GitHub releases, CDNs) for actual downloads and updates.

## Directory Structure

```
The-Railyard/
├── .github/
│   ├── ISSUE_TEMPLATE/         # Issue form templates
│   │   ├── config.yml          # Disables blank issues
│   │   ├── publish-mod.yml
│   │   ├── publish-map.yml
│   │   ├── update-mod.yml
│   │   ├── update-map.yml
│   │   └── report.yml
│   └── workflows/              # CI automation
│       ├── publish.yml         # Creates PRs from publish issues
│       ├── update-metadata.yml # Creates PRs from update issues
│       ├── regenerate-index.yml# Rebuilds index.json on merge
│       ├── close-invalid.yml   # Auto-closes non-template issues
│       └── report.yml          # Acknowledges reports
├── scripts/                    # TypeScript CI scripts
│   ├── lib/
│   │   ├── custom-url.ts      # Custom update URL validation helpers
│   │   └── github.ts          # GitHub API validation helpers
│   ├── package.json
│   ├── validate-publish.ts
│   ├── validate-update.ts
│   ├── create-listing.ts
│   ├── update-listing.ts
│   └── regenerate-indexes.ts
├── mods/
│   ├── index.json              # Registry of all mod IDs (auto-generated)
│   └── <mod-id>/
│       ├── manifest.json       # Mod metadata + update pointer
│       └── gallery/
│           ├── screenshot1.png
│           └── screenshot2.png
├── maps/
│   ├── index.json              # Registry of all map IDs (auto-generated)
│   └── <map-id>/
│       ├── manifest.json       # Map metadata + update pointer
│       └── gallery/
│           ├── thumbnail.png
│           └── screenshot1.png
├── ARCHITECTURE.md
└── README.md
```

---

## Mods

### `mods/index.json`

Top-level registry listing every mod by ID.

```json
{
  "schema_version": 1,
  "mods": ["mod-id", "another-mod"]
}
```

### `mods/<mod-id>/manifest.json`

```json
{
  "schema_version": 1,
  "id": "mod-id",
  "name": "Better Trains",
  "author": "SomeModder",
  "github_id": 12345678,
  "description": "Adds realistic train models and sounds.",
  "tags": ["vehicles", "cosmetic"],
  "gallery": ["gallery/screenshot1.png", "gallery/screenshot2.png"],
  "source": "https://github.com/somemodder/better-trains",
  "update": {
    "type": "github",
    "repo": "somemodder/better-trains"
  }
}
```

| Field            | Type       | Description                                                           |
| ---------------- | ---------- | --------------------------------------------------------------------- |
| `schema_version` | `number`   | Schema version for forward compatibility. Currently `1`.              |
| `id`             | `string`   | Unique mod identifier. Must match the directory name.                 |
| `name`           | `string`   | Human-readable display name.                                          |
| `author`         | `string`   | Mod author's name or handle.                                          |
| `github_id`      | `number`   | Immutable GitHub user ID of the publisher. Used for ownership checks. |
| `description`    | `string`   | Short description of what the mod does.                               |
| `tags`           | `string[]` | Categorization tags (e.g. `"vehicles"`, `"cosmetic"`, `"gameplay"`).  |
| `gallery`        | `string[]` | Relative paths to gallery images within the mod directory.            |
| `source`         | `string`   | URL to the mod's source code or homepage.                             |
| `update`         | `object`   | Update source configuration (see below).                              |

### Update Types

#### GitHub Releases

Railyard fetches directly from `https://api.github.com/repos/{repo}/releases` and picks the first `.zip` asset from the latest release. No filename convention is enforced -- any `.zip` asset will be used.

Publish validation verifies the repo exists and has at least one release with a `.zip` asset.

```json
"update": {
  "type": "github",
  "repo": "somemodder/better-trains"
}
```

#### Custom URL

Points to a self-hosted `update.json` file maintained by the mod author. Publish validation fetches the URL and verifies it returns valid JSON matching the `update.json` schema (has `schema_version`, a non-empty `versions` array, and required fields on the first entry).

```json
"update": {
  "type": "custom",
  "url": "https://example.com/better-trains/update.json"
}
```

### Custom `update.json` Format

```json
{
  "schema_version": 1,
  "versions": [
    {
      "version": "1.2.0",
      "game_version": ">=2.1.0",
      "date": "2026-02-20",
      "changelog": "Added new express train model.",
      "download": "https://example.com/better-trains/releases/v1.2.0.zip",
      "sha256": "a1b2c3d4..."
    },
    {
      "version": "1.1.0",
      "game_version": ">=2.0.0",
      "date": "2026-01-15",
      "changelog": "Initial public release.",
      "download": "https://example.com/better-trains/releases/v1.1.0.zip",
      "sha256": "e5f6a7b8..."
    }
  ]
}
```

| Field          | Type     | Description                                         |
| -------------- | -------- | --------------------------------------------------- |
| `version`      | `string` | Semver version string.                              |
| `game_version` | `string` | Semver range for game compatibility filtering.      |
| `date`         | `string` | Release date (ISO 8601).                            |
| `changelog`    | `string` | Human-readable changelog entry.                     |
| `download`     | `string` | Direct download URL for the release ZIP.            |
| `sha256`       | `string` | SHA-256 hash of the ZIP for integrity verification. |

---

## Maps

### `maps/index.json`

Top-level registry listing every map by ID.

```json
{
  "schema_version": 1,
  "maps": ["raleigh", "dublin", "toronto"]
}
```

### `maps/<map-id>/manifest.json`

```json
{
  "schema_version": 1,
  "id": "raleigh",
  "name": "Raleigh",
  "author": "muffintime",
  "github_id": 87654321,
  "city_code": "RDU",
  "country": "US",
  "population": 1500000,
  "description": "Custom map of the Raleigh metropolitan area.",
  "tags": ["north-america", "medium-city"],
  "gallery": ["gallery/thumbnail.png", "gallery/screenshot1.png"],
  "source": "https://github.com/muffintime/sb-raleigh",
  "update": {
    "type": "github",
    "repo": "muffintime/sb-raleigh"
  }
}
```

Maps share all fields from the mod manifest, plus three map-specific fields:

| Field        | Type     | Description                                                                                         |
| ------------ | -------- | --------------------------------------------------------------------------------------------------- |
| `city_code`  | `string` | 2-4 letter IATA/ICAO city code used by the game internally. Must not clash with vanilla city codes. |
| `country`    | `string` | ISO 3166-1 alpha-2 country code. Used to sort maps into country tabs in Railyard's UI.              |
| `population` | `number` | Metropolitan area population. Used for display and sorting without needing to download the map.     |

### Update Types

Identical to mods. Both `github` and `custom` types work the same way.

### Map Download ZIP Format

The download ZIP must follow [Kronifer's Map Manager](https://github.com/Subway-Builder-Modded/subwaybuilder-patcher/releases) format. Files must be at the root of the ZIP, not nested in a subfolder:

```
map-name.zip
├── config.json
├── demand_data.json
├── buildings_index.json
├── roads.geojson
├── runways_taxiways.geojson
└── XXX.pmtiles              (XXX = city code)
```

### Tags

#### Shared Tags (Mods & Maps)

**Region:**
`caribbean`, `central-america`, `central-asia`, `east-africa`, `east-asia`, `europe`, `middle-east`, `north-africa`, `north-america`, `oceania`, `south-america`, `south-asia`, `southeast-asia`, `southern-africa`, `west-africa`

#### Map-Only Tags

**Size:**
`small-city` (<500K), `medium-city` (500K-2M), `large-city` (2M+), `mega-city` (10M+)

**Detail:**
`full-detail`, `high-detail`, `medium-detail`, `low-detail`

**Features:**
`airports`, `entertainment`, `ferries`, `highways`, `universities`

#### Mod-Only Tags

**Category:**
`cosmetic`, `gameplay`, `library`, `misc`, `qol`, `stations`, `tracks`, `trains`, `ui`

## Issue-Driven CI Workflow

All submissions and updates are managed through GitHub Issues. Blank issues are disabled -- users must pick a template.

### Submission Flow

1. Author opens a **Publish New Mod/Map** issue using the structured form
2. CI parses the issue body, validates the data, and creates the listing files
3. A PR is automatically opened (e.g. `feat(mod): add \`better-trains\``)
4. Human reviewers merge the PR
5. Merging the PR auto-closes the issue (via `Fixes #N` in the PR body)
6. A post-merge workflow regenerates `index.json` from the filesystem

### Update Flow

1. Author opens an **Update Existing Mod/Map Metadata** issue
2. CI verifies the issue author's `github_id` matches the manifest's `github_id`
3. If ownership check fails, the issue is auto-closed with an explanation
4. Otherwise, a PR is created with the updated manifest fields

### Ownership Verification

Each manifest stores `github_id` -- the immutable numeric GitHub user ID of the original publisher. This is checked on update requests via `github.event.issue.user.id`. Usernames can change; IDs cannot.

### Index Regeneration

`mods/index.json` and `maps/index.json` are **never edited directly by PRs**. They are regenerated from the filesystem (scanning `*/manifest.json`) after merges to `main`. This eliminates merge conflicts when multiple PRs are open.

### Scripts (`scripts/`)

TypeScript scripts handle the complex logic, keeping workflow YAML thin:

| Script                  | Purpose                                                                       |
| ----------------------- | ----------------------------------------------------------------------------- |
| `validate-publish.ts`   | Validates new submissions (ID format, uniqueness, URLs, vanilla code clashes) |
| `validate-update.ts`    | Validates updates (existence check, ownership verification)                   |
| `create-listing.ts`     | Creates `manifest.json` and downloads gallery images                          |
| `update-listing.ts`     | Patches existing `manifest.json` with changed fields                          |
| `regenerate-indexes.ts` | Scans filesystem and rebuilds `index.json` files                              |

---

## Dependencies

Mods and maps can declare dependencies on other mods. Dependencies are specified **inside the mod's own `manifest.json` (or `config.json` for maps)** (the one shipped in the mod's download ZIP), not in this repository. The Railyard registry does not track dependencies -- they are resolved at install time by Railyard.

The `dependencies` field is a simple array of `mod-id@version` strings:

```json
{
  "dependencies": ["some-library@1.0.0", "another-mod@2.3.1"]
}
```

Each entry references a mod ID from this registry and the minimum required version. Railyard will ensure dependencies are installed before the dependent mod is loaded.

---

## Design Principles

- **Metadata only in this repo.** Actual mod/map binaries live on GitHub Releases, CDNs, or other file hosts. This keeps the repo lightweight.
- **Unified schema.** Mods and maps share the same update mechanism (`github` or `custom`), so Railyard uses one code path for fetching and updating both.
- **Manifest = storefront, ZIP = runtime.** The manifest contains browsing/discovery metadata. The ZIP's internal `config.json` is the source of truth for game-facing configuration. Some fields (like `population`, `country`) are intentionally duplicated so Railyard can display information before download.
- **Integrity verification.** `sha256` hashes in custom update files allow Railyard to verify downloads. GitHub releases rely on GitHub's own integrity guarantees.
- **Compatibility filtering.** `game_version` semver ranges let Railyard hide incompatible versions from users.
