// The operator's bearer token, entered in the UI (ADR-019). It is never built into
// the bundle. sessionStorage keeps it across reloads and drops it with the tab;
// storage can throw or be blocked, so memory is the source of truth.
const STORAGE_KEY = "signalops.operatorToken";

export interface OperatorSession {
  token: string | null;
  /** The API refused the last token, so the form should say why it is back. */
  rejected: boolean;
}

function readStored(): string | null {
  try {
    return sessionStorage.getItem(STORAGE_KEY);
  } catch {
    return null;
  }
}

let session: OperatorSession = { token: readStored(), rejected: false };
const listeners = new Set<() => void>();

function update(next: OperatorSession): void {
  session = next;
  try {
    if (next.token === null) {
      sessionStorage.removeItem(STORAGE_KEY);
    } else {
      sessionStorage.setItem(STORAGE_KEY, next.token);
    }
  } catch {
    // Without storage the token lasts until reload.
  }
  listeners.forEach((listener) => {
    listener();
  });
}

export function getOperatorToken(): string | null {
  return session.token;
}

export function getOperatorSession(): OperatorSession {
  return session;
}

export function setOperatorToken(token: string): void {
  update({ token, rejected: false });
}

export function clearOperatorToken({ rejected }: { rejected: boolean }): void {
  if (session.token !== null || rejected !== session.rejected) {
    update({ token: null, rejected });
  }
}

export function subscribeOperatorSession(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}
