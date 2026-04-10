// src/router.jsx
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { useAuthStore }     from './stores/auth.store.js';
import AppLayout            from './components/layout/AppLayout.jsx';
import LoginPage            from './pages/Login/index.jsx';
import OverviewPage         from './pages/Overview/index.jsx';
import WorkspacePage        from './pages/Workspace/index.jsx';
import SubmitPostingPage    from './pages/SubmitPosting/index.jsx';
import DataIngestionPage    from './pages/DataIngestion/index.jsx';
import ReportsPage          from './pages/Reports/index.jsx';

function PrivateRoute({ children }) {
  const isAuthenticated = useAuthStore(state => state.isAuthenticated);
  if (!isAuthenticated) return <Navigate to="/login" replace />;
  return <AppLayout>{children}</AppLayout>;
}

export function AppRouter() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<LoginPage />} />

        <Route path="/" element={
          <PrivateRoute><OverviewPage /></PrivateRoute>
        } />

        <Route path="/workspace" element={
          <PrivateRoute><WorkspacePage /></PrivateRoute>
        } />

        <Route path="/submit-posting" element={
          <PrivateRoute><SubmitPostingPage /></PrivateRoute>
        } />

        <Route path="/ingestion" element={
          <PrivateRoute><DataIngestionPage /></PrivateRoute>
        } />

        <Route path="/reports" element={
          <PrivateRoute><ReportsPage /></PrivateRoute>
        } />

        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}