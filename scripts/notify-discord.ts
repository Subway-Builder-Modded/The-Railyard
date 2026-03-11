import { pathToFileURL } from "node:url";
import { sendDiscordMarkdown } from "./lib/discord-webhook.js";

interface ParsedNotificationPayload {
  title: string;
  status: string;
  lines: string[];
  runUrl?: string;
}

function parseLines(raw: string | undefined): string[] {
  if (!raw || raw.trim() === "") return [];
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter((entry): entry is string => typeof entry === "string" && entry.trim() !== "")
      .map((entry) => entry.trim());
  } catch {
    return [];
  }
}

function readPayloadFromEnv(): ParsedNotificationPayload {
  return {
    title: process.env.DISCORD_TITLE?.trim() || "Workflow Notification",
    status: process.env.DISCORD_STATUS?.trim() || "unknown",
    runUrl: process.env.DISCORD_RUN_URL?.trim() || undefined,
    lines: parseLines(process.env.DISCORD_LINES_JSON),
  };
}

async function run(): Promise<void> {
  const webhookUrl = process.env.DISCORD_WEBHOOK_URL?.trim();
  if (!webhookUrl) {
    console.log("DISCORD_WEBHOOK_URL not set; skipping Discord notification.");
    return;
  }

  const payload = readPayloadFromEnv();
  await sendDiscordMarkdown({
    webhookUrl,
    title: payload.title,
    status: payload.status,
    lines: payload.lines,
    runUrl: payload.runUrl,
  });
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  run().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}

