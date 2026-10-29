import type { RouteObject } from "react-router";
import { Layout } from "./components/Layout";
import { HealthPage } from "./pages/HealthPage";
import { HomePage } from "./pages/HomePage";
import { NotFoundPage } from "./pages/NotFoundPage";
import { RouteErrorPage } from "./pages/RouteErrorPage";

export const routes: RouteObject[] = [
  {
    path: "/",
    element: <Layout />,
    errorElement: <RouteErrorPage />,
    children: [
      { index: true, element: <HomePage /> },
      { path: "status", element: <HealthPage /> },
      { path: "*", element: <NotFoundPage /> },
    ],
  },
];
