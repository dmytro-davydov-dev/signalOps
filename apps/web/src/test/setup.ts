import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

// React Router builds a Request per navigation with jsdom's AbortSignal, which
// Node's Request refuses. Test routes have no loaders to abort, so drop the signal.
class NodeRequest extends Request {
  constructor(input: RequestInfo | URL, init: RequestInit = {}) {
    super(input, { ...init, signal: undefined });
  }
}
globalThis.Request = NodeRequest;

afterEach(() => {
  cleanup();
});
