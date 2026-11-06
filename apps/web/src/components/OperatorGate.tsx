import { useQueryClient } from "@tanstack/react-query";
import { useState, useSyncExternalStore, type ReactNode, type SyntheticEvent } from "react";
import {
  clearOperatorToken,
  getOperatorSession,
  setOperatorToken,
  subscribeOperatorSession,
} from "../api/operatorToken";

/** Renders its children once an operator token is present, and asks for one otherwise. */
export function OperatorGate({ children }: { children: ReactNode }) {
  const session = useSyncExternalStore(subscribeOperatorSession, getOperatorSession);
  const queryClient = useQueryClient();
  const [value, setValue] = useState("");

  // Another token may belong to another workspace: its cached rows must not stay.
  const signIn = (event: SyntheticEvent) => {
    event.preventDefault();
    queryClient.clear();
    setOperatorToken(value.trim());
    setValue("");
  };

  if (session.token === null) {
    return (
      <section>
        <h1>Operator sign-in</h1>
        {session.rejected && (
          <p className="state state-error" role="alert">
            The API rejected that token. It may be wrong, or it may have been rotated.
          </p>
        )}
        <form className="filters" onSubmit={signIn}>
          <label>
            Operator token
            <input
              type="password"
              autoComplete="off"
              value={value}
              onChange={(event) => {
                setValue(event.target.value);
              }}
            />
          </label>
          <button type="submit" disabled={value.trim() === ""}>
            Continue
          </button>
        </form>
        <p className="hint">
          Tokens come from <code>SIGNALOPS_OPERATOR_TOKENS</code>. This tab keeps yours until it
          closes.
        </p>
      </section>
    );
  }

  return (
    <>
      {children}
      <p className="hint">
        <button
          type="button"
          className="link"
          onClick={() => {
            queryClient.clear();
            clearOperatorToken({ rejected: false });
          }}
        >
          Forget operator token
        </button>
      </p>
    </>
  );
}
