// src/providers.jsx
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Crear el QueryClient con configuración global
// Estas opciones aplican a TODAS las queries de la app
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Cuánto tiempo considera los datos "frescos" — 2 minutos
      // Dentro de este tiempo NO hace refetch aunque el componente
      // se monte de nuevo
      staleTime: 2 * 60 * 1000,

      // Cuántas veces reintenta si falla — 1 vez
      retry: 1,

      // No refetch cuando el usuario vuelve a la pestaña
      // (importante para datos financieros — no queremos sorpresas)
      refetchOnWindowFocus: false,
    },
    mutations: {
      // Las mutaciones (POST/PUT/DELETE) no reintentan por defecto
      retry: 0,
    },
  },
});

export function Providers({ children }) {
  return (
    <QueryClientProvider client={queryClient}>
      {children}
    </QueryClientProvider>
  );
}