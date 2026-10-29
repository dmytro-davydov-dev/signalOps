import { describeError } from "../api/describeError";

interface ErrorStateProps {
  title: string;
  error: unknown;
  onRetry?: () => void;
}

export function ErrorState({ title, error, onRetry }: ErrorStateProps) {
  const { message, hint } = describeError(error);
  return (
    <section className="state state-error" role="alert">
      <h2>{title}</h2>
      <p>{message}</p>
      {hint && <p className="hint">{hint}</p>}
      {onRetry && (
        <button type="button" onClick={onRetry}>
          Retry
        </button>
      )}
    </section>
  );
}
