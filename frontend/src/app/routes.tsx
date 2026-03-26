import { createBrowserRouter } from "react-router";
import { LandingPage } from "./pages/Landing";
import { Dashboard } from "./pages/Dashboard";
import { BlogGenerator } from "./pages/BlogGenerator";
import { KeywordAnalyzer } from "./pages/KeywordAnalyzer";
import { History } from "./pages/History";
import { NotFound } from "./pages/NotFound";
import { MainLayout } from "./components/layout/MainLayout";
import { ProtectedRoute } from "./components/layout/ProtectedRoute";

export const router = createBrowserRouter([
  {
    path: "/",
    element: <LandingPage />,
  },
  {
    path: "/app",
    element: <ProtectedRoute />,
    children: [
      {
        path: "",
        element: <MainLayout />,
        children: [
          {
            index: true,
            element: <Dashboard />,
          },
          {
            path: "generate",
            element: <BlogGenerator />,
          },
          {
            path: "keywords",
            element: <KeywordAnalyzer />,
          },
          {
            path: "history",
            element: <History />,
          },
        ],
      },
    ],
  },
  {
    path: "*",
    element: <NotFound />,
  },
]);
