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

For technical details, see [ARCHITECTURE.md](ARCHITECTURE.md).
