import { clearOperatorToken, getOperatorToken } from "./operatorToken";

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
export const baseUrl = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");

const REQUEST_TIMEOUT_MS = 10_000;

export interface RequestOptions {
  /** Non-2xx statuses whose JSON body is a valid result (for example readiness 503). */
  acceptStatuses?: readonly number[];
  signal?: AbortSignal;
  /** Send the operator token; a 401 then discards it, so the UI asks again. */
  operator?: boolean;
  /** Extra request headers, for example `Idempotency-Key`. */
  headers?: Readonly<Record<string, string>>;
}

interface RequestBody {
  method: "PATCH" | "POST";
  json: unknown;
}

export function getJson<T>(
  path: string,
  parse: (value: unknown) => T | undefined,
  options: RequestOptions = {},
): Promise<T> {
  return request(path, parse, undefined, options);
}

export function patchJson<T>(
  path: string,
  json: unknown,
  parse: (value: unknown) => T | undefined,
  options: RequestOptions = {},
): Promise<T> {
  return request(path, parse, { method: "PATCH", json }, options);
}

export function postJson<T>(
  path: string,
  json: unknown,
  parse: (value: unknown) => T | undefined,
  options: RequestOptions = {},
): Promise<T> {
  return request(path, parse, { method: "POST", json }, options);
}

async function request<T>(
  path: string,
  parse: (value: unknown) => T | undefined,
  body: RequestBody | undefined,
  { acceptStatuses = [], signal, operator = false, headers: extra = {} }: RequestOptions,
): Promise<T> {
  const headers: Record<string, string> = { ...extra, Accept: "application/json" };
  const token = operator ? getOperatorToken() : null;
  if (token !== null) {
    headers.Authorization = `Bearer ${token}`;
  }
  if (body) {
    headers["Content-Type"] = "application/json";
  }

  let response: Response;
  try {
    const timeout = AbortSignal.timeout(REQUEST_TIMEOUT_MS);
    response = await fetch(`${baseUrl}${path}`, {
      method: body?.method ?? "GET",
      headers,
      body: body ? JSON.stringify(body.json) : undefined,
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

  if (operator && response.status === 401) {
    clearOperatorToken({ rejected: true });
  }
  if (!response.ok && !acceptStatuses.includes(response.status)) {
    throw new ApiError("http", `The API responded with HTTP ${String(response.status)}.`, response.status);
  }

  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    throw new ApiError("invalid_response", "The API returned a response that is not JSON.", response.status);
  }
  const parsed = parse(payload);
  if (parsed === undefined) {
    throw new ApiError(
      "invalid_response",
      "The API returned a response this UI does not understand.",
      response.status,
    );
  }
  return parsed;
}
