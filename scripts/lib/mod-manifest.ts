export function validateModManifest(data: unknown, expectedModId?: string): string[] {
  const errors: string[] = [];

  if (typeof data !== "object" || data === null || Array.isArray(data)) {
    errors.push("**manifest.json**: Must be a JSON object.");
    return errors;
  }

  const obj = data as Record<string, unknown>;

  for (const field of ["id", "name", "version", "main"] as const) {
    if (typeof obj[field] !== "string" || obj[field] === "") {
      errors.push(`**manifest.json**: Missing required field \`${field}\`.`);
    }
  }

  if (expectedModId && typeof obj.id === "string" && obj.id !== expectedModId) {
    errors.push(`**manifest.json**: \`id\` is \`${obj.id}\` but expected \`${expectedModId}\` to match the Railyard mod ID.`);
  }

  if (typeof obj.author !== "object" || obj.author === null || Array.isArray(obj.author)) {
    errors.push("**manifest.json**: `author` must be an object with a `name` field.");
  } else {
    const author = obj.author as Record<string, unknown>;
    if (typeof author.name !== "string" || author.name === "") {
      errors.push("**manifest.json**: `author.name` is required.");
    }
  }

  if (typeof obj.dependencies !== "object" || obj.dependencies === null || Array.isArray(obj.dependencies)) {
    errors.push("**manifest.json**: `dependencies` must be an object.");
  } else {
    const deps = obj.dependencies as Record<string, unknown>;
    if (!("subway-builder" in deps) || typeof deps["subway-builder"] !== "string") {
      errors.push('**manifest.json**: `dependencies` must include `"subway-builder"` with a semver range.');
    }
  }

  return errors;
}
