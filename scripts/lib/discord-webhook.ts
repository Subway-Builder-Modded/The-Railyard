interface SendDiscordMarkdownOptions {
  webhookUrl: string;
  title: string;
  status: string;
  lines: string[];
  runUrl?: string;
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

export async function sendDiscordMarkdown(options: SendDiscordMarkdownOptions): Promise<void> {
  const content = buildDiscordMarkdownMessage(options);
  const response = await fetch(options.webhookUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ content }),
  });
  if (!response.ok) {
    throw new Error(`Discord webhook returned HTTP ${response.status}`);
  }
}

