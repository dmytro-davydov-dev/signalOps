import { useRouteError } from "react-router";
import { ErrorState } from "../components/ErrorState";

export function RouteErrorPage() {
  const error = useRouteError();
  return (
    <main className="content">
      <ErrorState
        title="Something went wrong"
        error={error}
        onRetry={() => {
          window.location.reload();
        }}
      />
    </main>
  );
}
