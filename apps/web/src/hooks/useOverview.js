import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useState, useCallback }                 from 'react';
import {
  getOverviewRequest,
  getAnalystsRequest,
  reassignTransactionRequest,
} from '../api/endpoints/overview.api.js';

// Hook principal — trae todos los datos del Overview
export function useOverviewData(filters) {
  return useQuery({
    queryKey: ['overview', filters],
    queryFn:  () => getOverviewRequest(filters),
    select:   (res) => res.data,
    staleTime: 60 * 1000, // 1 minuto
  });
}

// Hook para la lista de analistas (dropdowns)
export function useAnalysts() {
  return useQuery({
    queryKey: ['analysts'],
    queryFn:  getAnalystsRequest,
    select:   (res) => res.data,
    staleTime: 5 * 60 * 1000, // 5 minutos — cambia poco
  });
}

// Hook para reasignación con optimistic update
export function useReassignTransaction() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ bankRef1, toUserId }) =>
      reassignTransactionRequest(bankRef1, toUserId),

    // Optimistic update — actualiza la UI ANTES de que responda el servidor
    onMutate: async ({ bankRef1, toUserId, toUserName }) => {
      // Cancelar queries en vuelo para evitar conflictos
      await queryClient.cancelQueries({ queryKey: ['overview'] });

      // Guardar estado anterior para poder revertir
      const previousData = queryClient.getQueriesData({ queryKey: ['overview'] });

      // Actualizar el cache inmediatamente
      queryClient.setQueriesData({ queryKey: ['overview'] }, (old) => {
        if (!old?.data?.groups) return old;
        return {
          ...old,
          data: {
            ...old.data,
            groups: old.data.groups.map(group => ({
              ...group,
              rows: group.rows.map(row =>
                row.bankRef1 === bankRef1
                  ? { ...row, assignedUserId: toUserId, assignedUserName: toUserName }
                  : row
              ),
            })),
          },
        };
      });

      return { previousData };
    },

    // Si falla → revertir al estado anterior
    onError: (err, variables, context) => {
      if (context?.previousData) {
        context.previousData.forEach(([queryKey, data]) => {
          queryClient.setQueryData(queryKey, data);
        });
      }
    },
  });
}

// Hook para los filtros con debounce
export function useOverviewFilters() {
  const [filters, setFilters] = useState({
    status:   'ALL',
    customer: '',
    dateFrom: '',
    dateTo:   '',
  });

  const [debouncedFilters, setDebouncedFilters] = useState(filters);

  const updateFilter = useCallback((key, value) => {
    setFilters(prev => {
      const next = { ...prev, [key]: value };
      // Solo customer hace debounce — los demás son instantáneos
      if (key === 'customer') {
        setTimeout(() => setDebouncedFilters(next), 300);
      } else {
        setDebouncedFilters(next);
      }
      return next;
    });
  }, []);

  return { filters, debouncedFilters, updateFilter };
}