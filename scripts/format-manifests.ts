import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const REPO_ROOT = resolve(import.meta.dirname, "..");

function getManifestPaths(dirName: "maps" | "mods"): string[] {
  const dirPath = resolve(REPO_ROOT, dirName);
  const paths: string[] = [];

  for (const entry of readdirSync(dirPath, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    paths.push(resolve(dirPath, entry.name, "manifest.json"));
  }

  return paths.sort();
}

function formatJsonFile(path: string): boolean {
  const raw = readFileSync(path, "utf-8");
  const parsed = JSON.parse(raw) as unknown;
  const formatted = `${JSON.stringify(parsed, null, 2)}\n`;
  if (formatted === raw) return false;
  writeFileSync(path, formatted, "utf-8");
  return true;
}

function run(): void {
  const manifestPaths = [
    ...getManifestPaths("maps"),
    ...getManifestPaths("mods"),
  ];

  let changed = 0;
  for (const manifestPath of manifestPaths) {
    if (formatJsonFile(manifestPath)) {
      changed += 1;
    }
  }

  console.log(
    `Formatted ${changed} manifest.json file(s) out of ${manifestPaths.length}.`,
  );
}

run();

