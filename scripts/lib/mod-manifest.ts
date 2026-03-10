import { z } from "zod";

const ModReleaseManifestSchema = z
  .object({
    id: z.string().min(1, "`id` is required."),
    name: z.string().min(1, "`name` is required."),
    version: z.string().min(1, "`version` is required."),
    main: z.string().min(1, "`main` is required."),
    author: z.object({
      name: z.string().min(1, "`author.name` is required."),
    }),
    dependencies: z.record(z.string()),
  })
  .superRefine((value, ctx) => {
    if (typeof value.dependencies["subway-builder"] !== "string") {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["dependencies", "subway-builder"],
        message:
          '`dependencies` must include "subway-builder" with a semver range.',
      });
    }
  });

export function validateModManifest(data: unknown, expectedModId?: string): string[] {
  const parsed = ModReleaseManifestSchema.safeParse(data);
  const errors: string[] = [];

  if (!parsed.success) {
    for (const issue of parsed.error.issues) {
      const path = issue.path.length > 0 ? issue.path.join(".") : "manifest";
      errors.push(`**manifest.json**: ${path}: ${issue.message}`);
    }
    return errors;
  }

  if (expectedModId && parsed.data.id !== expectedModId) {
    errors.push(
      `**manifest.json**: \`id\` is \`${parsed.data.id}\` but expected \`${expectedModId}\` to match the Railyard mod ID.`,
    );
  }

  return errors;
}
