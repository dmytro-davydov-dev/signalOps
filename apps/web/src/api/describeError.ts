import { ApiError } from "./client";

const hints: Record<ApiError["kind"], string> = {
  network: "Check that the api service is running and reachable from this browser.",
  http: "The API is running but rejected or failed the request.",
  invalid_response: "The UI and API versions may be out of sync.",
};

export function describeError(error: unknown): { message: string; hint: string | undefined } {
  if (error instanceof ApiError) {
    return { message: error.message, hint: hints[error.kind] };
  }
  if (error instanceof Error) {
    return { message: error.message, hint: undefined };
  }
  return { message: "An unexpected error occurred.", hint: undefined };
}
