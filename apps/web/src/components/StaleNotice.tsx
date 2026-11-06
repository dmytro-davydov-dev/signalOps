import { ApiError } from "../api/client";

interface StaleNoticeProps {
  error: unknown;
  updatedAt: number;
}

/** A failed refetch keeps the previous data; say so rather than hiding it. */
export function StaleNotice({ error, updatedAt }: StaleNoticeProps) {
  const disconnected = error instanceof ApiError && error.kind === "network";
  const time = new Date(updatedAt).toLocaleTimeString();
  return (
    <p className="state state-stale" role="status">
      {disconnected ? "Disconnected from the API." : "Last refresh failed."} Showing results from{" "}
      {time}.
    </p>
  );
}
