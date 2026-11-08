// One idempotency key per pending start request of an incident. The key outlives
// a lost response or a reload, so a retry reaches the same investigation; it is
// dropped once the server answers, so the next deliberate start is a new request.

const PREFIX = "signalops.investigationRequestKey.";

function newKey(): string {
  return crypto.randomUUID();
}

export function pendingRequestKey(incidentId: string): string {
  try {
    const stored = sessionStorage.getItem(PREFIX + incidentId);
    if (stored !== null) {
      return stored;
    }
    const key = newKey();
    sessionStorage.setItem(PREFIX + incidentId, key);
    return key;
  } catch {
    // Storage blocked: the caller keeps the key in memory for this page instead.
    return newKey();
  }
}

export function clearRequestKey(incidentId: string): void {
  try {
    sessionStorage.removeItem(PREFIX + incidentId);
  } catch {
    // Nothing stored.
  }
}
