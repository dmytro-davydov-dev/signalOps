import { NavLink, Outlet } from "react-router";

export function Layout() {
  return (
    <div className="layout">
      <header className="topbar">
        <span className="brand">SignalOps</span>
        <nav aria-label="Main">
          <NavLink to="/" end>
            Overview
          </NavLink>
          <NavLink to="/incidents">Incidents</NavLink>
          <NavLink to="/evaluations">Evaluation</NavLink>
          <NavLink to="/status">System health</NavLink>
        </nav>
      </header>
      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}
