// src/hooks/useWorkspace.js
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getMyQueueRequest,
  getPanelRequest,
  calculateRequest,
  approveRequest,
  acquireLockRequest,
  releaseLockRequest,
} from '../api/endpoints/workspace.api.js';

export function useMyQueue() {
  return useQuery({
    queryKey:  ['workspace', 'my-queue'],
    queryFn:   getMyQueueRequest,
    select:    (res) => res.data,
    staleTime: 0,
    refetchInterval: 60 * 1000,
  });
}

export function usePanel(stgId) {
  return useQuery({
    queryKey: ['workspace', 'panel', stgId],
    queryFn:  () => getPanelRequest(stgId),
    select:   (res) => res,
    enabled:  !!stgId,
    staleTime: 0,
  });
}

export function useCalculate() {
  return useMutation({
    mutationFn: ({ stgId, payload }) => calculateRequest(stgId, payload),
  });
}

export function useApprove() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ stgId, payload }) => approveRequest(stgId, payload),
    onSuccess: () => {
      // Invalidate all relevant caches immediately after approval
      queryClient.invalidateQueries({ queryKey: ['workspace', 'my-queue'] });
      queryClient.invalidateQueries({ queryKey: ['workspace', 'stats'] });
      queryClient.invalidateQueries({ queryKey: ['overview'] });
    },
  });
}

export function useAcquireLock() {
  return useMutation({
    mutationFn: (bankRef1) => acquireLockRequest(bankRef1),
  });
}

export function useReleaseLock() {
  return useMutation({
    mutationFn: (bankRef1) => releaseLockRequest(bankRef1),
  });
}