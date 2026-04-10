import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import { Providers }  from './providers.jsx'
import { AppRouter }  from './router.jsx'
import { useAuthStore } from './stores/auth.store.js'

function AppWithAuth() {
  const initAuth = useAuthStore(state => state.initAuth);
  initAuth();
  return <AppRouter />;
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <Providers>
      <AppWithAuth />
    </Providers>
  </StrictMode>,
)