import { queryOptions, useQuery } from "@tanstack/react-query"
import type { Session } from "@auth/core/types";

export const useCurrentSession = () => {
    return useQuery(currentSessionOptions)
}

export const currentSessionOptions = queryOptions({
    queryKey: ['current-session'],
    queryFn: async () => {
        const response = await fetch('/api/auth/session', { credentials: 'include' })
        if (!response.ok) throw new Error('Failed to fetch session')
        return await response.json() as Session
    },
    refetchOnWindowFocus: false,
    retry: 1,
    staleTime: 3 * (60 * 1000)
})