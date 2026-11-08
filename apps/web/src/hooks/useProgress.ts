import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { fetchProgress, type ProgressEvent } from "../api/progress";
import { openProgressStream, type StreamState } from "../api/progressStream";

// A bound on pages read per history load, so a burst of events cannot loop forever.
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

/** The durable progress history, read after the last event held. */
export function useProgressHistory(investigationId: string, replayScope: string) {
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
      // The stream may have added events while this read ran; keep them.
      return mergeEvents(events, queryClient.getQueryData<ProgressEvent[]>(key) ?? []);
    },
  });
}

/**
 * Keeps the history current through the SSE stream while `active`. The stream
 * starts after the last event already held, so it only adds what is new, and a
 * step's end or the investigation's end refreshes the investigation itself.
 */
export function useProgressStream(
  investigationId: string,
  replayScope: string,
  active: boolean,
): StreamState | undefined {
  const queryClient = useQueryClient();
  const [state, setState] = useState<StreamState>();

  useEffect(() => {
    if (!active) {
      return undefined;
    }
    const key = progressKey(investigationId, replayScope);
    const held = queryClient.getQueryData<ProgressEvent[]>(key) ?? [];
    const refreshInvestigation = () =>
      void queryClient.invalidateQueries({
        queryKey: ["investigation", investigationId, replayScope],
      });
    return openProgressStream(
      investigationId,
      replayScope,
      {
        onEvent: (event) => {
          queryClient.setQueryData<ProgressEvent[]>(key, (known = []) =>
            mergeEvents(known, [event]),
          );
          if (event.event_type !== "step.started") {
            refreshInvestigation();
          }
        },
        onReset: () => {
          queryClient.setQueryData<ProgressEvent[]>(key, []);
          refreshInvestigation();
        },
        onState: setState,
      },
      { after: held.at(-1)?.id ?? 0 },
    );
  }, [active, investigationId, replayScope, queryClient]);

  return active ? state : undefined;
}
