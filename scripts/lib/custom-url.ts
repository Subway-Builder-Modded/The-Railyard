import { validateModManifest } from "./mod-manifest.js";

export async function validateCustomUpdateUrl(url: string, listingType?: string, modId?: string): Promise<string[]> {
  const errors: string[] = [];

  // 1. Fetch the URL
  let res: Response;
  try {
    res = await fetch(url, { headers: { Accept: "application/json" } });
  } catch (err) {
    errors.push(`**custom-update-url**: Could not reach \`${url}\` (${(err as Error).message}).`);
    return errors;
  }

  if (!res.ok) {
    errors.push(`**custom-update-url**: \`${url}\` returned HTTP ${res.status}.`);
    return errors;
  }

  // 2. Parse as JSON
  let body: unknown;
  try {
    body = await res.json();
  } catch {
    errors.push(`**custom-update-url**: \`${url}\` did not return valid JSON.`);
    return errors;
  }

  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    errors.push(`**custom-update-url**: \`${url}\` must return a JSON object.`);
    return errors;
  }

  const data = body as Record<string, unknown>;

  // 3. Check schema_version
  if (data.schema_version !== 1) {
    errors.push(`**custom-update-url**: \`schema_version\` must be \`1\`.`);
  }

  // 4. Check versions array
  if (!Array.isArray(data.versions)) {
    errors.push(`**custom-update-url**: \`versions\` must be an array.`);
    return errors;
  }

  if (data.versions.length === 0) {
    errors.push(`**custom-update-url**: \`versions\` array is empty. Add at least one version entry.`);
    return errors;
  }

  // 5. Validate first version entry has required fields
  const required = ["version", "game_version", "date", "download", "sha256"] as const;
  const first = data.versions[0] as Record<string, unknown>;
  for (const field of required) {
    if (typeof first[field] !== "string" || first[field] === "") {
      errors.push(`**custom-update-url**: First version entry is missing required field \`${field}\`.`);
    }
  }

  // 6. (Mods only) Check first version entry has a manifest URL, fetch and validate it
  if (listingType === "mod") {
    if (typeof first.manifest !== "string" || first.manifest === "") {
      errors.push(`**custom-update-url**: First version entry is missing required field \`manifest\`. Mods must provide a URL to their manifest.json.`);
    } else {
      try {
        const manifestRes = await fetch(first.manifest as string, { headers: { Accept: "application/json" } });
        if (!manifestRes.ok) {
          errors.push(`**custom-update-url**: Could not fetch manifest from \`${first.manifest}\` (HTTP ${manifestRes.status}).`);
        } else {
          try {
            const manifestData = await manifestRes.json();
            const manifestErrors = validateModManifest(manifestData, modId);
            errors.push(...manifestErrors);
          } catch {
            errors.push(`**custom-update-url**: Manifest at \`${first.manifest}\` is not valid JSON.`);
          }
        }
      } catch (err) {
        errors.push(`**custom-update-url**: Could not reach manifest URL \`${first.manifest}\` (${(err as Error).message}).`);
      }
    }
  }

  return errors;
}
