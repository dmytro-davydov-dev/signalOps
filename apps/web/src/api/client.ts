export type ApiErrorKind = "network" | "http" | "invalid_response";

export class ApiError extends Error {
  readonly kind: ApiErrorKind;
  readonly status: number | undefined;

  constructor(kind: ApiErrorKind, message: string, status?: number) {
    super(message);
    this.name = "ApiError";
    this.kind = kind;
    this.status = status;
  }
}

// Empty means same origin: the Vite dev server and nginx proxy API paths.
const baseUrl = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");

const REQUEST_TIMEOUT_MS = 10_000;

export interface RequestOptions {
  /** Non-2xx statuses whose JSON body is a valid result (for example readiness 503). */
  acceptStatuses?: readonly number[];
  signal?: AbortSignal;
}

export async function getJson<T>(
  path: string,
  parse: (value: unknown) => T | undefined,
  { acceptStatuses = [], signal }: RequestOptions = {},
): Promise<T> {
  let response: Response;
  try {
    const timeout = AbortSignal.timeout(REQUEST_TIMEOUT_MS);
    response = await fetch(`${baseUrl}${path}`, {
      headers: { Accept: "application/json" },
      signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
    });
  } catch (cause) {
    // Caller cancellation (for example an unmounted query) is not an API failure.
    if (signal?.aborted) {
      throw cause;
    }
    if (cause instanceof DOMException && cause.name === "TimeoutError") {
      throw new ApiError("network", "The SignalOps API did not respond in time.");
    }
    throw new ApiError("network", "The SignalOps API could not be reached.");
  }

  if (!response.ok && !acceptStatuses.includes(response.status)) {
    throw new ApiError("http", `The API responded with HTTP ${String(response.status)}.`, response.status);
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new ApiError("invalid_response", "The API returned a response that is not JSON.", response.status);
  }
  const parsed = parse(body);
  if (parsed === undefined) {
    throw new ApiError(
      "invalid_response",
      "The API returned a response this UI does not understand.",
      response.status,
    );
  }
  return parsed;
}
