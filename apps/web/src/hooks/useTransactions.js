// src/hooks/useTransactions.js
import { useQuery } from '@tanstack/react-query';
import {
  getTransactionsRequest,
  getTransactionsSummaryRequest,
} from '../api/endpoints/transactions.api.js';

// Hook para la lista paginada de transacciones
export function useTransactions(filters = {}) {
  return useQuery({
    queryKey: ['transactions', filters],
    queryFn:  () => getTransactionsRequest(filters),
    // El backend retorna { status, data: [...], pagination: {...} }
    // React Query envuelve eso en response.data via axios
    // entonces la estructura real es: response.data = { status, data, pagination }
    select: (response) => ({
      data:       response.data,
      pagination: response.pagination,
    }),
  });
}

// Hook para el resumen de estados (KPI cards)
export function useTransactionsSummary() {
  return useQuery({
    queryKey: ['transactions', 'summary'],
    queryFn:  getTransactionsSummaryRequest,
    select:   (data) => data.data,
  });
}