import { QueryClient } from "@tanstack/react-query";
import { ApiError } from "./api/client";

const RETRIES = 1;

/** A 4xx answer will not change on retry; an expired token must reach the form at once. */
function shouldRetry(failures: number, error: unknown): boolean {
  if (error instanceof ApiError && error.status !== undefined && error.status < 500) {
    return false;
  }
  return failures < RETRIES;
}

export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: shouldRetry, refetchOnWindowFocus: true },
    },
  });
}
