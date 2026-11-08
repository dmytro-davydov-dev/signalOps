import type { RouteObject } from "react-router";
import { Layout } from "./components/Layout";
import { EvaluationCasePage } from "./pages/EvaluationCasePage";
import { EvaluationRunPage } from "./pages/EvaluationRunPage";
import { EvaluationsPage } from "./pages/EvaluationsPage";
import { HealthPage } from "./pages/HealthPage";
import { HomePage } from "./pages/HomePage";
import { IncidentDetailPage } from "./pages/IncidentDetailPage";
import { IncidentsPage } from "./pages/IncidentsPage";
import { InvestigationPage } from "./pages/InvestigationPage";
import { NotFoundPage } from "./pages/NotFoundPage";
import { RouteErrorPage } from "./pages/RouteErrorPage";

export const routes: RouteObject[] = [
  {
    path: "/",
    element: <Layout />,
    errorElement: <RouteErrorPage />,
    children: [
      { index: true, element: <HomePage /> },
      { path: "incidents", element: <IncidentsPage /> },
      { path: "incidents/:incidentId", element: <IncidentDetailPage /> },
      { path: "investigations/:investigationId", element: <InvestigationPage /> },
      { path: "evaluations", element: <EvaluationsPage /> },
      { path: "evaluations/:runId", element: <EvaluationRunPage /> },
      { path: "evaluations/:runId/cases/:caseId", element: <EvaluationCasePage /> },
      { path: "status", element: <HealthPage /> },
      { path: "*", element: <NotFoundPage /> },
    ],
  },
];
