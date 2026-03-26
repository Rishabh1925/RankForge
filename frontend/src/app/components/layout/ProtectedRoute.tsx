import { Navigate, Outlet, useLocation } from "react-router";
import { isAuthenticated } from "../../services/auth";

export function ProtectedRoute() {
    const location = useLocation();

    if (!isAuthenticated()) {
        // Redirect unauthenticated users to landing/login
        return <Navigate to="/" state={{ from: location }} replace />;
    }

    // User is authenticated, proceed to child routes
    return <Outlet />;
}
