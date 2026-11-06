import { Link } from "react-router";

export function HomePage() {
  return (
    <section>
      <h1>SignalOps</h1>
      <p>Evidence-backed incident diagnoses from noisy telemetry.</p>
      <p>
        Review <Link to="/incidents">incidents</Link>, or check{" "}
        <Link to="/status">system health</Link> to confirm the API and its dependencies are up.
      </p>
    </section>
  );
}
