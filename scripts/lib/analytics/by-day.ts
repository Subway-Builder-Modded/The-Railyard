import { compareStableSemverAsc } from "../semver.js";
import { toListingLabel } from "./listing-meta.js";
import type { VersionGrainSnapshotTotals } from "./snapshots.js";
import type {
  AssetByDayRow,
  AuthorCreditEntry,
  DailySeriesRow,
  ListingKey,
  ListingMeta,
  ListingProjectRow,
  ListingTotals,
  SnapshotEntry,
} from "./types.js";

export function toSnapshotDateLabel(fileName: string): string {
  const match = fileName.match(/^snapshot_(\d{4}_\d{2}_\d{2})\.json$/);
  return match?.[1] ?? fileName;
}

export function buildDailyDeltaSnapshotTotals(
  snapshots: SnapshotEntry[],
  monotonicTotalsBySnapshot: Map<string, ListingTotals>,
): Map<string, ListingTotals> {
  const bySnapshot = new Map<string, ListingTotals>();
  let previousTotals = new Map<ListingKey, number>();

  for (const snapshot of snapshots) {
    const currentTotals = monotonicTotalsBySnapshot.get(snapshot.file) ?? new Map<ListingKey, number>();
    const keys = new Set<ListingKey>([
      ...previousTotals.keys(),
      ...currentTotals.keys(),
    ]);
    const deltaTotals = new Map<ListingKey, number>();
    for (const key of keys) {
      const delta = Math.max(0, (currentTotals.get(key) ?? 0) - (previousTotals.get(key) ?? 0));
      if (delta > 0 || currentTotals.has(key) || previousTotals.has(key)) {
        deltaTotals.set(key, delta);
      }
    }
    bySnapshot.set(snapshot.file, deltaTotals);
    previousTotals = currentTotals;
  }

  return bySnapshot;
}

export function buildSignedDailyDeltaSnapshotTotals(
  snapshots: SnapshotEntry[],
  adjustedTotalsBySnapshot: Map<string, ListingTotals>,
): Map<string, ListingTotals> {
  const bySnapshot = new Map<string, ListingTotals>();
  let previousTotals = new Map<ListingKey, number>();

  for (const snapshot of snapshots) {
    const currentTotals = adjustedTotalsBySnapshot.get(snapshot.file) ?? new Map<ListingKey, number>();
    const keys = new Set<ListingKey>([
      ...previousTotals.keys(),
      ...currentTotals.keys(),
    ]);
    const deltaTotals = new Map<ListingKey, number>();
    for (const key of keys) {
      const delta = (currentTotals.get(key) ?? 0) - (previousTotals.get(key) ?? 0);
      if (delta !== 0 || currentTotals.has(key) || previousTotals.has(key)) {
        deltaTotals.set(key, delta);
      }
    }
    bySnapshot.set(snapshot.file, deltaTotals);
    previousTotals = currentTotals;
  }

  return bySnapshot;
}

export function buildListingByDayRows(
  snapshotDates: string[],
  latestTotals: ListingTotals,
  dailyDeltasBySnapshot: Map<string, ListingTotals>,
  listingMeta: Map<ListingKey, ListingMeta>,
  listingProjectByKey: Map<ListingKey, ListingProjectRow>,
): DailySeriesRow[] {
  const rows: DailySeriesRow[] = [];
  for (const [key, totalDownloads] of latestTotals.entries()) {
    const [listingType, id] = key.split(":") as ["maps" | "mods", string];
    const meta = listingMeta.get(key) ?? {
      name: id,
      author: "UNKNOWN",
      author_alias: "UNKNOWN",
      attribution_link: "https://github.com/UNKNOWN",
      github_id: null,
    };
    const project = listingProjectByKey.get(key);
    const row: DailySeriesRow = {
      listing_type: toListingLabel(listingType),
      id,
      name: meta.name,
      author: meta.author,
      author_alias: meta.author_alias,
      attribution_link: meta.attribution_link,
      project_key: project?.project_key ?? `${toListingLabel(listingType)}:${id}`,
      project_name: project?.project_name ?? meta.name,
      total_downloads: totalDownloads,
    };
    for (const snapshotDate of snapshotDates) {
      row[snapshotDate] = dailyDeltasBySnapshot.get(`snapshot_${snapshotDate}.json`)?.get(key) ?? 0;
    }
    rows.push(row);
  }

  rows.sort((a, b) =>
    Number(b.total_downloads) - Number(a.total_downloads)
    || String(a.id).localeCompare(String(b.id)));
  return rows;
}

// First snapshot date (YYYY_MM_DD) each version key appeared in downloads.json
// — i.e. the day the version became downloadable (integrity-complete).
// Keys are `${listingKey}@@${version}`, matching the version-grain series.
export function buildVersionFirstSeenDates(
  snapshotDates: string[],
  listingVersionsBySnapshot: Map<string, { maps: Set<string>; mods: Set<string> }>,
): Map<string, string> {
  const firstSeen = new Map<string, string>();
  for (const snapshotDate of snapshotDates) {
    const versions = listingVersionsBySnapshot.get(`snapshot_${snapshotDate}.json`);
    if (!versions) continue;
    for (const listingType of ["maps", "mods"] as const) {
      for (const idVersion of versions[listingType]) {
        const key = `${listingType}:${idVersion}`;
        if (!firstSeen.has(key)) {
          firstSeen.set(key, snapshotDate);
        }
      }
    }
  }
  return firstSeen;
}

// Per-(listing, version) daily deltas from the version-grain snapshot series.
// Version sets come from the full snapshot history, so versions that later
// disappeared from downloads.json keep their historical rows. Rows are grouped
// by listing with versions in stable-semver order, so consumers can slice a
// listing's block without re-sorting.
export function buildListingVersionByDayRows(
  snapshotDates: string[],
  latestTotals: ListingTotals,
  historyVersionsByListing: Map<ListingKey, Set<string>>,
  versionGrain: VersionGrainSnapshotTotals,
  latestSnapshotFile: string,
  firstSeenByVersionKey: Map<string, string>,
): DailySeriesRow[] {
  const latestMonotonic = versionGrain.monotonicBySnapshot.get(latestSnapshotFile);
  const rows: DailySeriesRow[] = [];
  for (const key of latestTotals.keys()) {
    const [listingType, id] = key.split(":") as ["maps" | "mods", string];
    const versions = [...(historyVersionsByListing.get(key) ?? new Set<string>())]
      .sort(compareStableSemverAsc);
    for (const version of versions) {
      const versionKey = `${key}@@${version}`;
      const row: DailySeriesRow = {
        listing_type: toListingLabel(listingType),
        id,
        version,
        total_downloads: latestMonotonic?.get(versionKey) ?? 0,
      };
      for (const snapshotDate of snapshotDates) {
        row[snapshotDate] =
          versionGrain.dailyDeltasBySnapshot.get(`snapshot_${snapshotDate}.json`)?.get(versionKey) ?? 0;
      }
      // Appended last so existing positional consumers keep working. Empty for
      // versions never present in a snapshot (retired before history began).
      row.first_seen = firstSeenByVersionKey.get(versionKey) ?? "";
      rows.push(row);
    }
  }

  rows.sort((a, b) =>
    String(a.listing_type).localeCompare(String(b.listing_type))
    || String(a.id).localeCompare(String(b.id))
    || compareStableSemverAsc(String(a.version), String(b.version)));
  return rows;
}

export function buildProjectByDayRows(
  snapshotDates: string[],
  latestTotals: ListingTotals,
  dailyDeltasBySnapshot: Map<string, ListingTotals>,
  listingMeta: Map<ListingKey, ListingMeta>,
  listingProjectByKey: Map<ListingKey, ListingProjectRow>,
): DailySeriesRow[] {
  const projectRows = new Map<string, DailySeriesRow>();

  for (const [key, totalDownloads] of latestTotals.entries()) {
    const project = listingProjectByKey.get(key);
    if (!project) continue;
    const existing = projectRows.get(project.project_key) ?? {
      project_key: project.project_key,
      project_name: project.project_name,
      author: "",
      author_alias: "",
      attribution_link: "",
      listing_count: 0,
      total_downloads: 0,
    };
    const meta = listingMeta.get(key) ?? {
      name: "",
      author: "UNKNOWN",
      author_alias: "UNKNOWN",
      attribution_link: "https://github.com/UNKNOWN",
      github_id: null,
    };
    const authors = new Set(String(existing.author).split("; ").filter(Boolean));
    const authorAliases = new Set(String(existing.author_alias).split("; ").filter(Boolean));
    const attributionLinks = new Set(String(existing.attribution_link).split("; ").filter(Boolean));
    authors.add(meta.author);
    authorAliases.add(meta.author_alias);
    attributionLinks.add(meta.attribution_link);
    existing.author = [...authors].sort().join("; ");
    existing.author_alias = [...authorAliases].sort().join("; ");
    existing.attribution_link = [...attributionLinks].sort().join("; ");
    existing.listing_count = Number(existing.listing_count) + 1;
    existing.total_downloads = Number(existing.total_downloads) + totalDownloads;
    for (const snapshotDate of snapshotDates) {
      existing[snapshotDate] = Number(existing[snapshotDate] ?? 0)
        + (dailyDeltasBySnapshot.get(`snapshot_${snapshotDate}.json`)?.get(key) ?? 0);
    }
    projectRows.set(project.project_key, existing);
  }

  return [...projectRows.values()].sort((a, b) =>
    Number(b.total_downloads) - Number(a.total_downloads)
    || String(a.project_key).localeCompare(String(b.project_key)));
}

// Author-by-day rolls up per CREDITED person (caretaker crediting): each credit
// entry contributes its totals/deltas to the person it is credited to. For
// listings without caretakers this is exactly the old listing-author rollup.
export function buildAuthorByDayRows(
  snapshotDates: string[],
  creditEntries: readonly AuthorCreditEntry[],
): DailySeriesRow[] {
  const authorRows = new Map<string, DailySeriesRow>();

  for (const entry of creditEntries) {
    const existing = authorRows.get(entry.person.author) ?? {
      author: entry.person.author,
      author_alias: entry.person.author_alias,
      attribution_link: entry.person.attribution_link,
      asset_count: 0,
      map_count: 0,
      mod_count: 0,
      total_downloads: 0,
    };
    existing.asset_count = Number(existing.asset_count) + 1;
    if (entry.listingType === "maps") existing.map_count = Number(existing.map_count) + 1;
    if (entry.listingType === "mods") existing.mod_count = Number(existing.mod_count) + 1;
    existing.total_downloads = Number(existing.total_downloads) + entry.currentTotal;
    for (const snapshotDate of snapshotDates) {
      existing[snapshotDate] = Number(existing[snapshotDate] ?? 0)
        + entry.deltaAt(`snapshot_${snapshotDate}.json`);
    }
    authorRows.set(entry.person.author, existing);
  }

  return [...authorRows.values()].sort((a, b) =>
    Number(b.total_downloads) - Number(a.total_downloads)
    || String(a.author).localeCompare(String(b.author)));
}

export function buildAssetsByDayRows(
  snapshotDates: string[],
  dailyDeltasBySnapshot: Map<string, ListingTotals>,
  signedDailyDeltasBySnapshot: Map<string, ListingTotals>,
  listingIdsBySnapshot: Map<string, { maps: Set<string>; mods: Set<string> }>,
  listingVersionsBySnapshot: Map<string, { maps: Set<string>; mods: Set<string> }>,
): AssetByDayRow[] {
  let cumulativeMaps = 0;
  let cumulativeMods = 0;
  const seenMaps = new Set<string>();
  const seenMods = new Set<string>();
  const seenMapVersions = new Set<string>();
  const seenModVersions = new Set<string>();

  return snapshotDates.map((snapshotDate) => {
    let maps = 0;
    let mods = 0;
    let mapsSigned = 0;
    let modsSigned = 0;
    let newMaps = 0;
    let newMods = 0;
    let newMapVersions = 0;
    let newModVersions = 0;
    const snapshotTotals = dailyDeltasBySnapshot.get(`snapshot_${snapshotDate}.json`)
      ?? new Map<ListingKey, number>();
    const snapshotSignedTotals = signedDailyDeltasBySnapshot.get(`snapshot_${snapshotDate}.json`)
      ?? new Map<ListingKey, number>();
    const listingIds = listingIdsBySnapshot.get(`snapshot_${snapshotDate}.json`) ?? {
      maps: new Set<string>(),
      mods: new Set<string>(),
    };
    const listingVersions = listingVersionsBySnapshot.get(`snapshot_${snapshotDate}.json`) ?? {
      maps: new Set<string>(),
      mods: new Set<string>(),
    };

    for (const [key, totalDownloads] of snapshotTotals.entries()) {
      const [listingType] = key.split(":") as ["maps" | "mods", string];
      if (listingType === "maps") maps += totalDownloads;
      if (listingType === "mods") mods += totalDownloads;
    }
    for (const [key, totalDownloads] of snapshotSignedTotals.entries()) {
      const [listingType] = key.split(":") as ["maps" | "mods", string];
      if (listingType === "maps") mapsSigned += totalDownloads;
      if (listingType === "mods") modsSigned += totalDownloads;
    }

    for (const id of listingIds.maps) {
      if (!seenMaps.has(id)) {
        newMaps += 1;
      }
      seenMaps.add(id);
    }
    for (const id of listingIds.mods) {
      if (!seenMods.has(id)) {
        newMods += 1;
      }
      seenMods.add(id);
    }
    for (const versionKey of listingVersions.maps) {
      if (!seenMapVersions.has(versionKey)) {
        newMapVersions += 1;
      }
      seenMapVersions.add(versionKey);
    }
    for (const versionKey of listingVersions.mods) {
      if (!seenModVersions.has(versionKey)) {
        newModVersions += 1;
      }
      seenModVersions.add(versionKey);
    }

    cumulativeMaps += maps;
    cumulativeMods += mods;

    return {
      snapshot_date: snapshotDate,
      total_downloads: maps + mods,
      maps,
      mods,
      total_downloads_signed: mapsSigned + modsSigned,
      maps_signed: mapsSigned,
      mods_signed: modsSigned,
      total_downloads_clamped: maps + mods,
      maps_clamped: maps,
      mods_clamped: mods,
      cumulative_total: cumulativeMaps + cumulativeMods,
      cumulative_maps: cumulativeMaps,
      cumulative_mods: cumulativeMods,
      total_new_assets: newMaps + newMods,
      new_maps: newMaps,
      new_mods: newMods,
      cumulative_assets: seenMaps.size + seenMods.size,
      cumulative_maps_assets: seenMaps.size,
      cumulative_mods_assets: seenMods.size,
      total_new_assets_versions: newMapVersions + newModVersions,
      new_maps_versions: newMapVersions,
      new_mods_versions: newModVersions,
      cumulative_asset_versions: seenMapVersions.size + seenModVersions.size,
      cumulative_maps_versions: seenMapVersions.size,
      cumulative_mods_versions: seenModVersions.size,
    };
  });
}
