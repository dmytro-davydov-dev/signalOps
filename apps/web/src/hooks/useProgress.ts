import { useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchProgress, type ProgressEvent } from "../api/progress";

const POLL_MS = 3_000;
// A bound on pages read per refresh, so a burst of events cannot loop forever.
const MAX_PAGES_PER_READ = 20;

export const progressKey = (investigationId: string, replayScope: string) =>
  ["progress", investigationId, replayScope] as const;

/** Append events to `known`, skipping any already held, keeping cursor order. */
export function mergeEvents(
  known: readonly ProgressEvent[],
  incoming: readonly ProgressEvent[],
): ProgressEvent[] {
  const last = known.at(-1)?.id ?? 0;
  const fresh = incoming.filter((event) => event.id > last);
  return fresh.length === 0 ? [...known] : [...known, ...fresh];
}

/**
 * The investigation's durable progress history, read after the last event held,
 * so each refresh fetches only what is new.
 */
export function useProgress(investigationId: string, replayScope: string, active: boolean) {
  const queryClient = useQueryClient();
  const key = progressKey(investigationId, replayScope);
  return useQuery({
    queryKey: key,
    queryFn: async ({ signal }) => {
      let events = queryClient.getQueryData<ProgressEvent[]>(key) ?? [];
      for (let page = 0; page < MAX_PAGES_PER_READ; page += 1) {
        const after = events.at(-1)?.id ?? 0;
        const result = await fetchProgress(investigationId, replayScope, after, signal);
        events = mergeEvents(events, result.items);
        if (result.next_after === null) {
          break;
        }
      }
      return events;
    },
    refetchInterval: active ? POLL_MS : false,
  });
}
