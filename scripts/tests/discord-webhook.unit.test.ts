import test from "node:test";
import assert from "node:assert/strict";
import { buildDiscordMarkdownMessage } from "../lib/discord-webhook.js";

test("buildDiscordMarkdownMessage renders markdown lines with run link", () => {
  const content = buildDiscordMarkdownMessage({
    webhookUrl: "https://discord.example/webhook",
    title: "Regenerate Download Counts",
    status: "success",
    lines: [
      "- **Updated records:** 10",
      "- **New downloads:** +25",
    ],
    runUrl: "https://github.com/example/repo/actions/runs/1",
  });

  assert.equal(
    content,
    [
      "**Regenerate Download Counts** (`success`)",
      "- **Updated records:** 10",
      "- **New downloads:** +25",
      "[View workflow run](https://github.com/example/repo/actions/runs/1)",
    ].join("\n"),
  );
});

