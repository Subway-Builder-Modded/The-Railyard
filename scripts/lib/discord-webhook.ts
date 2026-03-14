import { fetchWithTimeout, resolveTimeoutMsFromEnv } from "./http.js";

interface SendDiscordMarkdownOptions {
  webhookUrl: string;
  title: string;
  status: string;
  lines: string[];
  runUrl?: string;
  warnings?: string[];
  errors?: string[];
}

export function buildDiscordMarkdownMessage(options: SendDiscordMarkdownOptions): string {
  const messageLines = [
    `**${options.title}** (\`${options.status}\`)`,
    ...options.lines,
  ];
  if (options.runUrl) {
    messageLines.push(`[View workflow run](${options.runUrl})`);
  }
  return messageLines.join("\n");
}

interface DiscordEmbed {
  title?: string;
  description: string;
  color: number;
}

interface DiscordWebhookPayload {
  embeds: DiscordEmbed[];
}

function resolveStatusColor(statusRaw: string): number {
  const status = statusRaw.trim().toLowerCase();
  if (status === "success" || status === "completed") return 0x2ECC71; // green
  if (status === "failure" || status === "failed" || status === "error") return 0xE74C3C; // red
  if (status === "cancelled" || status === "canceled") return 0x95A5A6; // gray
  return 0x3498DB; // blue fallback
}

function toBulletedDescription(items: string[], maxItems = 20): string {
  const normalized = items
    .map((item) => item.trim())
    .filter((item) => item !== "");
  if (normalized.length === 0) {
    return "- none";
  }

  const displayed = normalized.slice(0, maxItems).map((item) => `- ${item}`);
  const hiddenCount = normalized.length - displayed.length;
  if (hiddenCount > 0) {
    displayed.push(`- ...and ${hiddenCount} more`);
  }

  const description = displayed.join("\n");
  return description.length > 4000 ? `${description.slice(0, 3997)}...` : description;
}

export function buildDiscordWebhookPayload(options: SendDiscordMarkdownOptions): DiscordWebhookPayload {
  const summaryLines = [
    ...options.lines,
    ...(options.runUrl ? [`[View workflow run](${options.runUrl})`] : []),
  ];
  const summaryDescription = [
    `**${options.title}** (\`${options.status}\`)`,
    ...summaryLines,
  ].join("\n");

  const embeds: DiscordEmbed[] = [
    {
      description: summaryDescription.length > 4000
        ? `${summaryDescription.slice(0, 3997)}...`
        : summaryDescription,
      color: resolveStatusColor(options.status),
    },
  ];

  const warnings = options.warnings ?? [];
  if (warnings.length > 0) {
    embeds.push({
      title: `Warnings (${warnings.length})`,
      description: toBulletedDescription(warnings),
      color: 0xF1C40F, // yellow
    });
  }

  const errors = options.errors ?? [];
  if (errors.length > 0) {
    embeds.push({
      title: `Errors (${errors.length})`,
      description: toBulletedDescription(errors),
      color: 0xE74C3C, // red
    });
  }

  return { embeds };
}

export async function sendDiscordMarkdown(options: SendDiscordMarkdownOptions): Promise<void> {
  const timeoutMs = resolveTimeoutMsFromEnv("DISCORD_WEBHOOK_TIMEOUT_MS", 15_000);
  const payload = buildDiscordWebhookPayload(options);
  const response = await fetchWithTimeout(
    fetch,
    options.webhookUrl,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    },
    {
      timeoutMs,
      heartbeatPrefix: "[discord]",
      heartbeatLabel: "send-webhook",
    },
  );
  if (!response.ok) {
    throw new Error(`Discord webhook returned HTTP ${response.status}`);
  }
}

