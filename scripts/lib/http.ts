export interface FetchWithTimeoutOptions {
  timeoutMs: number;
  heartbeatPrefix?: string;
  heartbeatLabel?: string;
}

export function parsePositiveInteger(value: string | undefined): number | null {
  if (typeof value !== "string") return null;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return parsed;
}

export function resolveTimeoutMsFromEnv(envName: string, fallbackMs: number): number {
  const fromEnv = parsePositiveInteger(process.env[envName]);
  return fromEnv ?? fallbackMs;
}

export async function fetchWithTimeout(
  fetchImpl: typeof fetch,
  url: string,
  init: RequestInit | undefined,
  options: FetchWithTimeoutOptions,
): Promise<Response> {
  const startedAt = Date.now();
  const shouldLog = Boolean(options.heartbeatPrefix && options.heartbeatLabel);
  if (shouldLog) {
    console.log(`${options.heartbeatPrefix} heartbeat:start ${options.heartbeatLabel}`);
  }

  const controller = new AbortController();
  const timeoutHandle = setTimeout(() => {
    controller.abort();
  }, options.timeoutMs);

  try {
    const response = await fetchImpl(url, {
      ...init,
      signal: controller.signal,
    });
    if (shouldLog) {
      const durationMs = Date.now() - startedAt;
      console.log(
        `${options.heartbeatPrefix} heartbeat:end ${options.heartbeatLabel} status=${response.status} durationMs=${durationMs}`,
      );
    }
    return response;
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    const message = error instanceof Error ? error.message : String(error);
    const timeoutHint = error instanceof Error && error.name === "AbortError"
      ? `timed out after ${options.timeoutMs}ms`
      : message;
    if (shouldLog) {
      console.warn(
        `${options.heartbeatPrefix} heartbeat:error ${options.heartbeatLabel} durationMs=${durationMs} error=${timeoutHint}`,
      );
    }
    throw new Error(timeoutHint);
  } finally {
    clearTimeout(timeoutHandle);
  }
}
